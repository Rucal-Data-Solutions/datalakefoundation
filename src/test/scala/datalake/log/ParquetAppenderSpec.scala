package datalake.metadata

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

    def createLogEvent(jobId: String, user: String, env: String, message: String): LogEvent = {
      val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
      contextData.putValue("jobId", jobId)
      contextData.putValue("user", user)
      contextData.putValue("env", env)

      Log4jLogEvent.newBuilder()
        .setMessage(new SimpleMessage(message))
        .setContextData(contextData)
        .build()
    }

    // Test single log entry
    appender.append(createLogEvent("job1", "user1", "dev", "Test message 1"))
    
    // Should trigger write due to threshold
    appender.append(createLogEvent("job2", "user2", "prod", "Test message 2"))

    // Verify parquet file contents
    val logs = spark.read.parquet(testParquetPath)
    logs.count() shouldBe 2
    
    val rows = logs.collect()
    rows.map(_.getString(0)).toSet should contain allOf("job1", "job2")
    rows.map(_.getString(1)).toSet should contain allOf("user1", "user2")
    rows.map(_.getString(2)).toSet should contain allOf("dev", "prod")
    
    appender.stop()
  }
}


