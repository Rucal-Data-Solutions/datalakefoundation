package datalake.log

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.{Configurator, LoggerConfig}
import org.apache.spark.sql.SparkSession

object Log4jConfigurator {
  @volatile private var initialized = false

  def init(spark: SparkSession): Unit = synchronized {
    if (!initialized) {
      val ctx = LoggerContext.getContext(false)
      val config = ctx.getConfiguration

      val parquetAppender = ParquetAppender.createAppender(spark, "dlf_log.parquet")
      parquetAppender.start()

      val loggerName = "datalake"
      val loggerConfig = Option(config.getLoggerConfig(loggerName))
        .filter(_.getName == loggerName)
        .getOrElse {
          val newConfig = new LoggerConfig(loggerName, Level.INFO, true)
          config.addLogger(loggerName, newConfig)
          newConfig
        }

      loggerConfig.addAppender(parquetAppender, Level.INFO, null)


      ctx.updateLoggers()

      initialized = true
    }
  }
}