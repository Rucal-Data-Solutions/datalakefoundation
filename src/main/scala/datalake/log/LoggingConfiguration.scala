package datalake.log

import org.apache.log4j.{AppenderSkeleton, Level, Logger, PatternLayout, LogManager}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock

class ParquetAppender(spark: SparkSession, parquetFilePath: String, logBufferThreshold: Int) extends AppenderSkeleton {
  private val logBuffer: mutable.ListBuffer[String] = mutable.ListBuffer[String]()
  private val lock = new ReentrantLock()

  override def append(event: LoggingEvent): Unit = {
    val message = this.layout.format(event)
    addToLogBuffer(message)
    if (logBuffer.size >= logBufferThreshold) {
      writeLogsToParquet()
    }
  }

  override def close(): Unit = {
    writeLogsToParquet()
  }

  override def requiresLayout(): Boolean = true

  private def addToLogBuffer(message: String): Unit = {
    lock.lock()
    try {
      logBuffer += message
    } finally {
      lock.unlock()
    }
  }

  private def writeLogsToParquet(): Unit = {
    lock.lock()
    try {
      if (logBuffer.nonEmpty) {
        import spark.implicits._
        val df: DataFrame = logBuffer.toArray.toSeq.toDF("log_message")
        df.write.mode("append").parquet(parquetFilePath)
        logBuffer.clear()
      }
    } finally {
      lock.unlock()
    }
  }
}

object LoggingConfiguration {
  def configureLogging(spark: SparkSession, parquetFilePath: String, logBufferThreshold: Int): Unit = {
    val parquetAppender = new ParquetAppender(spark, parquetFilePath, logBufferThreshold)
    parquetAppender.setThreshold(Level.INFO)
    parquetAppender.setLayout(new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n"))

    val rootLogger = Logger.getRootLogger
    rootLogger.addAppender(parquetAppender)
  }
}