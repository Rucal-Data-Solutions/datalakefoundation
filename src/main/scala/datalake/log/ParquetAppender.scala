package datalake.log

import org.apache.logging.log4j.core.{Appender, Filter, Layout}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.config.plugins.{Plugin, PluginAttribute, PluginElement, PluginFactory}
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.config.Property

import java.io.Serializable
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

  private val logBuffer = mutable.ListBuffer[(String, String, String, String)]() // (jobId, user, env, message)
  private val lock = new ReentrantLock()

  override def append(event: LogEvent): Unit = {
    val context = event.getContextData

    val jobId = Option(context.getValue("jobId")).getOrElse("unknown")
    val user = Option(context.getValue("user")).getOrElse("unknown")
    val env = Option(context.getValue("env")).getOrElse("unknown")

    val message = new String(getLayout.toByteArray(event))

    addToLogBuffer((jobId, user, env, message))
    if (logBuffer.size >= logBufferThreshold) {
      writeLogsToParquet()
    }
  }

  private def addToLogBuffer(entry: (String, String, String, String)): Unit = {
    lock.lock()
    try {
      logBuffer += entry
    } finally {
      lock.unlock()
    }
  }

  private def writeLogsToParquet(): Unit = {
    lock.lock()
    try {
      if (logBuffer.nonEmpty) {
        import spark.implicits._
        val df: DataFrame = logBuffer.toSeq.toDF("job_id", "user", "env", "log_message")
        df.write.mode("append").parquet(parquetFilePath)
        logBuffer.clear()
      }
    } finally {
      lock.unlock()
    }
  }

  override def stop(): Unit = {
    super.stop()
    writeLogsToParquet()
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