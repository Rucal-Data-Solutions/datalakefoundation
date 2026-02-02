package datalake.log

import org.apache.logging.log4j.{Level, LogManager, ThreadContext}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.Appender
import org.apache.logging.log4j.core.appender.AsyncAppender
import org.apache.logging.log4j.core.config.{AppenderRef, Configurator, LoggerConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import datalake.metadata.Environment
import java.util.UUID

object Log4jConfigurator {
  @volatile private var initialized = false
  @volatile private var currentAsyncAppender: Option[AsyncAppender] = None
  @volatile private var currentBaseAppender: Option[Appender] = None
  @volatile private var currentRunId: Option[String] = None

  def init(spark: SparkSession, environment: Option[Environment] = None): Unit = synchronized {
    if (!initialized) {
      // Generate a unique run ID for this session
      val runId = UUID.randomUUID().toString
      currentRunId = Some(runId)
      ThreadContext.put("run_id", runId)

      val ctx = LoggerContext.getContext(false)
      val config = ctx.getConfiguration

      // Create the base appender
      val baseAppender = createLogAppender(spark, environment)
      baseAppender.start()
      config.addAppender(baseAppender)
      currentBaseAppender = Some(baseAppender)

      // Wrap with AsyncAppender for non-blocking logging
      val asyncAppender = AsyncAppender.newBuilder()
        .setName("AsyncDatalakeAppender")
        .setAppenderRefs(Array(AppenderRef.createAppenderRef(baseAppender.getName, null, null)))
        .setConfiguration(config)
        .setBlocking(true)           // Block when queue is full to prevent event loss
        .setBufferSize(1024)         // Ring buffer size
        .build()
      asyncAppender.start()
      config.addAppender(asyncAppender)
      currentAsyncAppender = Some(asyncAppender)

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

      loggerConfig.addAppender(asyncAppender, logLevel, null)

      // Suppress noisy Spark parser cache INFO messages
      Configurator.setLevel("org.apache.spark.sql.catalyst.parser.AbstractParser$ParserCaches", Level.WARN)

      // Suppress SparkSession Connect mode warnings (expected when using Spark Connect)
      Configurator.setLevel("org.apache.spark.sql.SparkSession", Level.ERROR)

      // Suppress Databricks edge config INFO messages (expected during initialization)
      Configurator.setLevel("com.databricks.DatabricksEdgeConfigs", Level.WARN)
      Configurator.setLevel("com.databricks.logging.Log4jUsageLogger", Level.WARN)

      ctx.updateLoggers()

      // Register SparkListener to flush logs before SparkContext stops
      // Wrapped in try-catch to handle Spark version incompatibilities and Spark Connect gracefully
      try {
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            shutdown()
          }
        })
      } catch {
        case _: NoSuchMethodError | _: AbstractMethodError | _: UnsupportedOperationException =>
          // Spark version mismatch or Spark Connect - rely on JVM shutdown hook instead
        case e: Exception if e.getMessage != null && e.getMessage.contains("UNSUPPORTED_CONNECT_FEATURE") =>
          // Spark Connect does not support SparkContext access - rely on JVM shutdown hook instead
      }

      // Also register JVM shutdown hook as a fallback
      Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown()))

      initialized = true
    }
  }

  def shutdown(): Unit = synchronized {
    if (initialized) {
      // Stop appenders in reverse order - async first, then base
      currentAsyncAppender.foreach(_.stop())
      currentBaseAppender.foreach(_.stop())
      currentAsyncAppender = None
      currentBaseAppender = None
      currentRunId = None
      ThreadContext.remove("run_id")
      initialized = false
    }
  }

  def flush(): Unit = synchronized {
    if (initialized) {
      // Stop and restart async appender to flush its queue
      currentAsyncAppender.foreach(_.stop())

      // Flush the base appender (TableAppender or ParquetAppender)
      currentBaseAppender.foreach {
        case ta: TableAppender => ta.flush()
        case pa: ParquetAppender => pa.flush()
        case _ => // Other appenders don't need explicit flushing
      }

      // Restart async appender if it was stopped
      currentAsyncAppender.foreach(_.start())
    }
  }

  def getRunId: Option[String] = currentRunId
  
  private def createLogAppender(spark: SparkSession, environment: Option[Environment]): Appender = {
    environment match {
      case Some(env) =>
        env.LogAppenderType match {
          case "table" =>
            TableAppender.createAppender(
              spark = spark,
              tableName = env.LogOutput,
              threshold = 10,
              createTableIfNotExists = true
            )
          case _ =>
            ParquetAppender.createAppender(
              spark = spark,
              parquetPath = env.LogOutput,
              threshold = 10
            )
        }
      case None =>
        ParquetAppender.createAppender(spark, "dlf_log.parquet")
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