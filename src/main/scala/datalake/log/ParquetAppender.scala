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
import org.apache.spark.sql.functions.{col, parse_json}

@Plugin(name = "ParquetAppender", category = "Core", elementType = "appender", printObject = true)
class ParquetAppender(
    name: String,
    layout: org.apache.logging.log4j.core.Layout[_ <: Serializable],
    filter: Filter,
    ignoreExceptions: Boolean,
    val spark: SparkSession,
    val parquetFilePath: String,
    val logBufferThreshold: Int
) extends AbstractAppender(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY) {

  private case class LogEntry(timestamp: Timestamp, level: String, message: String, data: Option[String], dataType: Option[String], runId: Option[String])

  private val logBuffer = ArrayBuffer[LogEntry]()
  private val bufferLock = new Object()

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

    writeToParquet(batch)
  }

  private val logSchema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("level", StringType, nullable = true),
    StructField("message", StringType, nullable = true),
    StructField("data_str", StringType, nullable = true),
    StructField("data_type", StringType, nullable = true),
    StructField("run_id", StringType, nullable = true)
  ))

  private def writeToParquet(batch: Seq[LogEntry]): Unit = {
    try {
      val rows = batch.map { entry =>
        Row(entry.timestamp, entry.level, entry.message, entry.data.orNull, entry.dataType.orNull, entry.runId.orNull)
      }
      val df: DataFrame = spark.createDataFrame(rows.asJava, logSchema)
        .withColumn("data", parse_json(col("data_str")))
        .drop("data_str")
      df.write.mode("append").parquet(parquetFilePath)
    } catch {
      case e: Exception =>
        error("Failed to flush logs to parquet", e)
    }
  }

  override def stop(): Unit = {
    flush()
    super.stop()
  }
}

object ParquetAppender {
  def createAppender(
      spark: SparkSession,
      parquetPath: String = "./spark-logs.parquet",
      threshold: Int = 10
  ): ParquetAppender = {
    new ParquetAppender(
      "ParquetAppender",
      PatternLayout.createDefaultLayout(),
      null,
      true,
      spark,
      parquetPath,
      threshold
    )
  }
}