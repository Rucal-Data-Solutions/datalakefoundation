package datalake.log

import org.apache.logging.log4j.core.{Appender, Filter, Layout}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.config.plugins.{Plugin, PluginAttribute, PluginElement, PluginFactory}
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.config.Property

import java.io.{PrintWriter, Serializable, StringWriter}
import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

@Plugin(name = "TableAppender", category = "Core", elementType = "appender", printObject = true)
class TableAppender(
    name: String,
    layout: org.apache.logging.log4j.core.Layout[_ <: Serializable],
    filter: Filter,
    ignoreExceptions: Boolean,
    val spark: SparkSession,
    val tableName: String,
    val logBufferThreshold: Int,
    val createTableIfNotExists: Boolean = true
) extends AbstractAppender(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY) {

  private case class LogEntry(timestamp: Timestamp, level: String, message: String, data: Option[String], dataType: Option[String], runId: Option[String])

  private val logBuffer = ArrayBuffer[LogEntry]()
  private val bufferLock = new Object()

  if (createTableIfNotExists) {
    ensureTableExists()
  }

  override def append(event: LogEvent): Unit = {
    val context = event.getContextData

    val timestamp = new Timestamp(event.getTimeMillis)
    val level = event.getLevel.name()
    val message = event.getMessage.getFormattedMessage
    val runId = Log4jConfigurator.getRunId

    // Check for thrown exception first, then fall back to context data
    val (data, dataType) = Option(event.getThrown) match {
      case Some(thrown) =>
        val sw = new StringWriter()
        thrown.printStackTrace(new PrintWriter(sw))
        (Some(sw.toString), Some("stacktrace"))
      case None =>
        (Option(context.getValue[String]("data")), Option(context.getValue[String]("data_type")))
    }

    val shouldFlush = bufferLock.synchronized {
      logBuffer += LogEntry(timestamp, level, message, data, dataType, runId)
      logBuffer.size >= logBufferThreshold
    }

    if (shouldFlush) {
      flush()
    }
  }

  def flush(): Unit = {
    val batch = bufferLock.synchronized {
      if (logBuffer.isEmpty) return
      val entries = logBuffer.toSeq
      logBuffer.clear()
      entries
    }

    writeToTable(batch)
  }

  private val logSchema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("level", StringType, nullable = true),
    StructField("message", StringType, nullable = true),
    StructField("data", StringType, nullable = true),
    StructField("data_type", StringType, nullable = true),
    StructField("run_id", StringType, nullable = true)
  ))

  private def writeToTable(batch: Seq[LogEntry]): Unit = {
    try {
      val isStopped = try {
        // Use SQL check for Spark Connect compatibility instead of sparkContext.isStopped
        spark.sql("SELECT 1")
        false
      } catch {
        case _: Exception => true
      }

      if (isStopped) {
        System.err.println(s"[TableAppender] WARNING: SparkSession stopped, cannot flush ${batch.size} log entries")
        return
      }

      val rows = batch.map { entry =>
        Row(entry.timestamp, entry.level, entry.message, entry.data.orNull, entry.dataType.orNull, entry.runId.orNull)
      }
      val df: DataFrame = spark.createDataFrame(rows.asJava, logSchema)

      val tableColumns = spark.table(tableName).columns
      val orderedDf = df.select(tableColumns.map(c => df.col(c)): _*)
      orderedDf.write.insertInto(tableName)
    } catch {
      case e: Exception =>
        error("Failed to flush logs to table", e)
        e.printStackTrace()
    }
  }

  private def ensureTableExists(): Unit = {
    try {
      val parts = tableName.split("\\.")
      val databaseName = parts.length match {
        case 3 => s"${parts(0)}.${parts(1)}"
        case 2 => parts(0)
        case _ => null
      }

      if (databaseName != null) {
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
      }

      spark.sql(s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          timestamp TIMESTAMP,
          level STRING,
          message STRING,
          data STRING,
          data_type STRING,
          run_id STRING
        ) USING DELTA
      """)
    } catch {
      case e: Exception =>
        error(s"Could not ensure table exists: $tableName", e)
    }
  }

  override def stop(): Unit = {
    flush()
    super.stop()
  }
}

object TableAppender {
  def createAppender(
      spark: SparkSession,
      tableName: String,
      threshold: Int = 10,
      createTableIfNotExists: Boolean = true
  ): TableAppender = {
    new TableAppender(
      "TableAppender",
      PatternLayout.createDefaultLayout(),
      null,
      true,
      spark,
      tableName,
      threshold,
      createTableIfNotExists
    )
  }
}
