package datalake.metadata

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.impl.Log4jLogEvent
import org.apache.logging.log4j.message.SimpleMessage

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import org.apache.commons.io.FileUtils

import datalake.log._


class ParquetAppenderSpec extends AnyFlatSpec with SparkSessionTest {
  private val testParquetPath = s"${testBasePath}/test-logs.parquet"

  "ParquetAppender" should "buffer and write logs correctly" in {
    val appender = ParquetAppender.createAppender(
      spark = spark,
      parquetPath = testParquetPath,
      threshold = 2
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

    // Test single log entry
    appender.append(createLogEvent(Level.INFO, "Test message 1"))

    // Should trigger async write due to threshold
    appender.append(createLogEvent(Level.WARN, "Test message 2", Some("""{"records": 100}""")))

    // Stop flushes remaining logs and waits for completion
    appender.stop()

    // Verify parquet file contents
    val logs = spark.read.parquet(testParquetPath)
    logs.count() shouldBe 2

    val rows = logs.collect()
    // Column order: timestamp, level, message, data
    rows.map(_.getString(1)).toSet should contain allOf("INFO", "WARN")
    rows.map(_.getString(2)).toSet should contain allOf("Test message 1", "Test message 2")
    rows.exists(r => r.getString(3) == """{"records": 100}""") shouldBe true
  }

  it should "store processing summary data as JSON" in {
    val appender = ParquetAppender.createAppender(
      spark = spark,
      parquetPath = s"${testBasePath}/test-logs-summary.parquet",
      threshold = 1
    )
    appender.start()

    val summary = ProcessingSummary(
      recordsInSlice = 1000,
      inserted = 500,
      updated = 300,
      deleted = 50,
      unchanged = 150,
      entityId = Some("entity_123"),
      sliceFile = Some("slice_001.parquet")
    )

    val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
    contextData.putValue("data", summary.toJson)

    val event = Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage("Processing completed"))
      .setLevel(Level.INFO)
      .setContextData(contextData)
      .setTimeMillis(System.currentTimeMillis())
      .build()

    appender.append(event)
    appender.stop()

    val logs = spark.read.parquet(s"${testBasePath}/test-logs-summary.parquet")
    val row = logs.collect().head
    val dataJson = row.getString(3)

    dataJson should include("\"records_in_slice\":1000")
    dataJson should include("\"inserted\":500")
    dataJson should include("\"updated\":300")
    dataJson should include("\"deleted\":50")
    dataJson should include("\"entity_id\":\"entity_123\"")
  }
}


