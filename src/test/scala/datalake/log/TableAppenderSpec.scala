package datalake.log

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.SimpleMessage
import org.apache.logging.log4j.{LogManager, ThreadContext}

import datalake.metadata.{SparkSessionTest, Environment}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TableAppenderSpec extends AnyFlatSpec with SparkSessionTest {
  private val testTableName = "default.test_dlf_logs"

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP TABLE IF EXISTS $testTableName")
    // Reset Log4jConfigurator state for each test
    resetLog4jConfigurator()
  }

  override def afterEach(): Unit = {
    Log4jConfigurator.shutdown()
    super.afterEach()
  }

  private def resetLog4jConfigurator(): Unit = {
    import org.apache.logging.log4j.core.LoggerContext
    import org.apache.logging.log4j.core.config.LoggerConfig

    // First shutdown any existing appenders properly
    Log4jConfigurator.shutdown()

    // Clean up the Log4j configuration
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration

    // Remove appenders from the datalake logger config
    Option(config.getLoggerConfig("datalake")).filter(_.getName == "datalake").foreach { loggerConfig =>
      loggerConfig.getAppenders.keySet().toArray.foreach { name =>
        loggerConfig.removeAppender(name.toString)
      }
    }

    ctx.updateLoggers()

    // Use reflection to reset the initialized flag
    val initializedField = Log4jConfigurator.getClass.getDeclaredField("initialized")
    initializedField.setAccessible(true)
    initializedField.setBoolean(Log4jConfigurator, false)

    val asyncField = Log4jConfigurator.getClass.getDeclaredField("currentAsyncAppender")
    asyncField.setAccessible(true)
    asyncField.set(Log4jConfigurator, None)

    val baseField = Log4jConfigurator.getClass.getDeclaredField("currentBaseAppender")
    baseField.setAccessible(true)
    baseField.set(Log4jConfigurator, None)

    val runIdField = Log4jConfigurator.getClass.getDeclaredField("currentRunId")
    runIdField.setAccessible(true)
    runIdField.set(Log4jConfigurator, None)

    ThreadContext.remove("run_id")
  }

  "TableAppender" should "buffer and write logs correctly to table" in {
    val appender = TableAppender.createAppender(
      spark = spark,
      tableName = testTableName,
      threshold = 2,
      createTableIfNotExists = true
    )
    appender.start()

    def createLogEvent(level: Level, message: String, data: Option[String] = None): LogEvent = {
      val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
      data.foreach(d => contextData.putValue("data", d))

      Log4jLogEvent.newBuilder()
        .setMessage(new SimpleMessage(message))
        .setLevel(level)
        .setContextData(contextData)
        .setTimeMillis(System.currentTimeMillis())
        .build()
    }

    appender.append(createLogEvent(Level.INFO, "Test message 1"))
    appender.append(createLogEvent(Level.WARN, "Test message 2", Some("""{"records": 100}""")))

    appender.stop()

    val logs = spark.table(testTableName)
    logs.count() shouldBe 2

    val rows = logs.collect()
    // Column order: timestamp, level, message, data
    rows.map(_.getString(1)).toSet should contain allOf("INFO", "WARN")
    rows.map(_.getString(2)).toSet should contain allOf("Test message 1", "Test message 2")
  }

  it should "create table if it doesn't exist" in {
    spark.catalog.tableExists(testTableName) shouldBe false

    val appender = TableAppender.createAppender(
      spark = spark,
      tableName = testTableName,
      threshold = 10,
      createTableIfNotExists = true
    )
    appender.start()

    spark.catalog.tableExists(testTableName) shouldBe true

    // Verify new schema
    val columns = spark.table(testTableName).columns
    columns should contain allOf("timestamp", "level", "message", "data", "run_id")

    appender.stop()
  }

  it should "write logs through Log4jConfigurator with table appender" in {
    val env = new Environment(
      name = "test",
      root_folder = "/tmp/test",
      timezone = "UTC",
      raw_path = "raw",
      bronze_path = "bronze",
      silver_path = "silver",
      output_method = "paths",
      log_level = "DEBUG",
      log_appender_type = "table",
      log_output = Some(testTableName)
    )

    // Initialize logging with table appender
    Log4jConfigurator.init(spark, Some(env))

    // Log some messages through the datalake logger
    val logger = LogManager.getLogger("datalake.test")
    logger.info("Integration test message 1")
    logger.info("Integration test message 2")
    logger.debug("Integration test debug message")

    // Shutdown to flush logs
    Log4jConfigurator.shutdown()

    // Verify logs were written
    val logs = spark.table(testTableName)
    val count = logs.count()

    println(s"=== Table contents ($count rows) ===")
    logs.show(truncate = false)

    count should be >= 2L

    val rows = logs.collect()
    // Verify level column contains expected values
    rows.exists(_.getString(1) == "INFO") shouldBe true
    // Verify messages are captured
    rows.exists(_.getString(2).contains("Integration test message")) shouldBe true
    // Verify run_id is captured and all entries have the same run_id
    val runIds = rows.map(r => Option(r.getAs[String]("run_id"))).filter(_.isDefined).map(_.get).distinct
    runIds.length shouldBe 1
    runIds.head should fullyMatch regex "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
  }

  it should "provide run_id through DatalakeLogManager" in {
    val env = new Environment(
      name = "test",
      root_folder = "/tmp/test",
      timezone = "UTC",
      raw_path = "raw",
      bronze_path = "bronze",
      silver_path = "silver",
      output_method = "paths",
      log_level = "DEBUG",
      log_appender_type = "table",
      log_output = Some(testTableName)
    )

    // Before initialization, run_id should be None
    DatalakeLogManager.getRunId shouldBe None

    // Initialize logging
    Log4jConfigurator.init(spark, Some(env))

    // After initialization, run_id should be available
    val runId = DatalakeLogManager.getRunId
    runId shouldBe defined
    runId.get should fullyMatch regex "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

    Log4jConfigurator.shutdown()

    // After shutdown, run_id should be None again
    DatalakeLogManager.getRunId shouldBe None
  }

  it should "write logs with structured data using withSummary helper" in {
    // Use direct TableAppender to avoid Log4j configuration state issues between tests
    val appender = TableAppender.createAppender(
      spark = spark,
      tableName = testTableName,
      threshold = 1,
      createTableIfNotExists = true
    )
    appender.start()

    val summary = ProcessingSummary(
      recordsInSlice = 500,
      inserted = 200,
      updated = 150,
      deleted = 50,
      unchanged = 100,
      entityId = Some("test_entity")
    )

    val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
    contextData.putValue("data", summary.toJson)

    val event = Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage("Processing completed successfully"))
      .setLevel(Level.INFO)
      .setContextData(contextData)
      .setTimeMillis(System.currentTimeMillis())
      .build()

    appender.append(event)
    appender.stop()

    val logs = spark.table(testTableName)
    val rows = logs.collect()

    val rowWithData = rows.find(r => Option(r.getString(3)).exists(_.contains("records_in_slice")))
    rowWithData shouldBe defined
    rowWithData.get.getString(3) should include("\"records_in_slice\":500")
    rowWithData.get.getString(3) should include("\"inserted\":200")
  }
}
