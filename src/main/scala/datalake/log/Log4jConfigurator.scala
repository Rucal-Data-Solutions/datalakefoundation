package datalake.log

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.{Configurator, LoggerConfig}
import org.apache.spark.sql.SparkSession
import datalake.metadata.Environment

object Log4jConfigurator {
  @volatile private var initialized = false

  def init(spark: SparkSession, environment: Option[Environment] = None): Unit = synchronized {
    if (!initialized) {
      val ctx = LoggerContext.getContext(false)
      val config = ctx.getConfiguration

      // val parquetAppender = ParquetAppender.createAppender(spark, "dlf_log.parquet")
      // parquetAppender.start()

      val loggerName = "datalake"
      
      // Determine log level from environment or default to WARN
      val logLevel = environment match {
        case Some(env) => parseLogLevel(env.LogLevel)
        case None => Level.WARN
      }
      
      val loggerConfig = Option(config.getLoggerConfig(loggerName))
        .filter(_.getName == loggerName)
        .getOrElse {
          val newConfig = new LoggerConfig(loggerName, logLevel, true)
          config.addLogger(loggerName, newConfig)
          newConfig
        }

      // Set the logger level from environment configuration
      loggerConfig.setLevel(logLevel)

      // loggerConfig.addAppender(parquetAppender, Level.INFO, null)


      ctx.updateLoggers()

      initialized = true
    }
  }
  
  private def parseLogLevel(levelString: String): Level = {
    levelString.toUpperCase match {
      case "TRACE" => Level.TRACE
      case "DEBUG" => Level.DEBUG
      case "INFO" => Level.INFO
      case "WARN" => Level.WARN
      case "ERROR" => Level.ERROR
      case "FATAL" => Level.FATAL
      case "OFF" => Level.OFF
      case _ => Level.WARN // Default fallback
    }
  }
}