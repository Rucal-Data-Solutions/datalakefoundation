package datalake.log

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.core.filter.{CompositeFilter, MarkerFilter, ThresholdFilter}
import org.apache.logging.log4j.core.Filter
import org.apache.logging.log4j.message.SimpleMessage

class LogLevelConfigTest extends AnyFunSuite with SparkSessionTest {

  private def resetConfigurator(): Unit = {
    DatalakeLogManager.shutdown()
    val initializedField = Log4jConfigurator.getClass.getDeclaredField("initialized")
    initializedField.setAccessible(true)
    initializedField.set(Log4jConfigurator, false)
  }

  test("Log level should be configurable from environment") {
    val infoEnv = new Environment(
      "TEST",
      "/tmp/test",
      "UTC",
      "raw",
      "bronze",
      "silver",
      output_method = "paths",
      log_level = "INFO"
    )

    resetConfigurator()
    Log4jConfigurator.init(spark, Some(infoEnv))

    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig("datalake")

    assert(loggerConfig.getLevel == Level.INFO, "Log level should be set to INFO")
  }

  test("Logger level should be opened to INFO when configured level is WARN") {
    val defaultEnv = new Environment(
      "TEST",
      "/tmp/test",
      "UTC",
      "raw",
      "bronze",
      "silver",
      output_method = "paths"
      // log_level not specified, defaults to WARN
    )

    resetConfigurator()
    Log4jConfigurator.init(spark, Some(defaultEnv))

    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig("datalake")

    // Logger gate is opened to INFO so audit events can pass through
    assert(loggerConfig.getLevel == Level.INFO,
      "Logger level should be opened to INFO to allow audit events")
  }

  test("Summary should be written when log level is WARN") {
    val logPath = s"$testBasePath/summary-test-logs.parquet"

    val appender = ParquetAppender.createAppender(
      spark = spark,
      parquetPath = logPath,
      threshold = 1
    )
    appender.start()

    val summary = ProcessingSummary(recordsInSlice = 42, inserted = 42)
    val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
    contextData.putValue("data", summary.toJson)
    contextData.putValue("data_type", "ProcessingSummary")

    val summaryEvent = Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage("Summary test message"))
      .setLevel(Level.INFO)
      .setMarker(DatalakeLogManager.AuditMarker)
      .setContextData(contextData)
      .setTimeMillis(System.currentTimeMillis())
      .build()

    // Verify the composite filter accepts AUDIT-marked INFO events
    val auditFilter = MarkerFilter.createFilter(
      "AUDIT", Filter.Result.ACCEPT, Filter.Result.NEUTRAL
    )
    val thresholdFilter =
      ThresholdFilter.createFilter(Level.WARN, Filter.Result.ACCEPT, Filter.Result.DENY)
    val compositeFilter =
      CompositeFilter.createFilters(Array(auditFilter, thresholdFilter))

    assert(compositeFilter.filter(summaryEvent) == Filter.Result.ACCEPT,
      "Composite filter should ACCEPT AUDIT-marked events")

    appender.append(summaryEvent)
    appender.stop()

    val logs = spark.read.parquet(logPath)
    val summaryRows = logs.collect()
      .filter(_.getAs[String]("message") == "Summary test message")

    assert(summaryRows.length == 1, "Summary should be written even when log level is WARN")
    assert(summaryRows.head.getAs[String]("level") == "INFO")
    assert(summaryRows.head.getAs[String]("data_type") == "ProcessingSummary")
  }

  test("Regular INFO messages should be filtered when log level is WARN") {
    val regularEvent = Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage("regular info message"))
      .setLevel(Level.INFO)
      .setTimeMillis(System.currentTimeMillis())
      .build()

    val auditFilter = MarkerFilter.createFilter(
      "AUDIT", Filter.Result.ACCEPT, Filter.Result.NEUTRAL
    )
    val thresholdFilter =
      ThresholdFilter.createFilter(Level.WARN, Filter.Result.ACCEPT, Filter.Result.DENY)
    val compositeFilter =
      CompositeFilter.createFilters(Array(auditFilter, thresholdFilter))

    assert(compositeFilter.filter(regularEvent) == Filter.Result.DENY,
      "Composite filter should DENY regular INFO messages when level is WARN")
  }
}
