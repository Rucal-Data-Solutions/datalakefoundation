package datalake.log

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils

import datalake.metadata._
import datalake.processing._

class LoggingImpactSpec extends AnyFunSuite with SparkSessionTest {

  case class TimingResult(
    logLevel: String,
    rowCount: Int,
    durationMs: Long,
    throughputRowsPerSec: Double
  )

  private def measureProcessingTime(
    testEntity: Entity,
    slicePath: String,
    sliceName: String,
    strategy: ProcessStrategy
  ): Long = {
    val startTime = System.nanoTime()

    val proc = new Processing(testEntity, sliceName)
    proc.Process(strategy)

    val endTime = System.nanoTime()
    (endTime - startTime) / 1_000_000 // Convert to milliseconds
  }

  private def setLogLevel(level: Level): Unit = {
    Configurator.setLevel("datalake", level)
  }

  test("Measure logging impact on processing with different log levels") {
    import spark.implicits._

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(2)
    val paths = testEntity.getPaths

    val testId = s"logging_impact_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val rowCount = 1000

    // Generate test data
    val testData = (1 to rowCount).map { i =>
      (i, i.toLong * 100, s"User$i", s"Data$i", testId)
    }.toDF("ID", "SeqNr", "name", "data", "test_id")

    val logLevels = Seq(
      ("OFF", Level.OFF),
      ("ERROR", Level.ERROR),
      ("WARN", Level.WARN),
      ("INFO", Level.INFO),
      ("DEBUG", Level.DEBUG)
    )

    val results = scala.collection.mutable.ArrayBuffer[TimingResult]()

    for ((levelName, level) <- logLevels) {
      // Clean up before each test
      FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

      val slice = s"logging_test_${levelName}_${testId}.parquet"
      testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

      // Set log level
      setLogLevel(level)

      // Measure processing time
      val durationMs = measureProcessingTime(testEntity, paths.bronzepath, slice, Full)
      val throughput = if (durationMs > 0) rowCount.toDouble / (durationMs.toDouble / 1000.0) else 0.0

      results += TimingResult(levelName, rowCount, durationMs, throughput)
    }

    // Print results
    info("")
    info("=" * 70)
    info(f"Logging Impact Test Results ($rowCount rows)")
    info("=" * 70)
    info(f"${"Log Level"}%-12s ${"Duration (ms)"}%-15s ${"Throughput (rows/sec)"}%-20s")
    info("-" * 70)

    results.foreach { r =>
      info(f"${r.logLevel}%-12s ${r.durationMs}%-15d ${r.throughputRowsPerSec}%-20.2f")
    }

    info("-" * 70)

    // Calculate overhead compared to OFF level
    val offDuration = results.find(_.logLevel == "OFF").map(_.durationMs).getOrElse(1L)
    info("")
    info("Overhead compared to LOG_OFF:")
    results.filter(_.logLevel != "OFF").foreach { r =>
      val overhead = ((r.durationMs - offDuration).toDouble / offDuration.toDouble) * 100
      info(f"  ${r.logLevel}%-10s: ${overhead}%+.2f%%")
    }
    info("=" * 70)

    // Assert that processing completes successfully at all levels
    results.foreach { r =>
      assert(r.durationMs > 0, s"Processing should complete for log level ${r.logLevel}")
    }
  }

  test("Measure logging impact with varying data sizes") {
    import spark.implicits._

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(2)
    val paths = testEntity.getPaths

    val testId = s"scaling_test_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val rowCounts = Seq(100, 500, 1000, 2000)
    val logLevels = Seq(("OFF", Level.OFF), ("DEBUG", Level.DEBUG))

    val results = scala.collection.mutable.ArrayBuffer[(String, Int, Long)]()

    for (rowCount <- rowCounts) {
      val testData = (1 to rowCount).map { i =>
        (i, i.toLong * 100, s"User$i", s"Data$i", testId)
      }.toDF("ID", "SeqNr", "name", "data", "test_id")

      for ((levelName, level) <- logLevels) {
        // Clean up before each test
        FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

        val slice = s"scaling_${levelName}_${rowCount}_${testId}.parquet"
        testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

        setLogLevel(level)

        val durationMs = measureProcessingTime(testEntity, paths.bronzepath, slice, Full)
        results += ((levelName, rowCount, durationMs))
      }
    }

    // Print scaling results
    info("")
    info("=" * 70)
    info("Logging Impact Scaling Test Results")
    info("=" * 70)
    info(f"${"Rows"}%-10s ${"OFF (ms)"}%-12s ${"DEBUG (ms)"}%-12s ${"Overhead (%)"}%-12s")
    info("-" * 70)

    for (rowCount <- rowCounts) {
      val offDuration = results.find(r => r._1 == "OFF" && r._2 == rowCount).map(_._3).getOrElse(1L)
      val debugDuration = results.find(r => r._1 == "DEBUG" && r._2 == rowCount).map(_._3).getOrElse(1L)
      val overhead = ((debugDuration - offDuration).toDouble / offDuration.toDouble) * 100
      info(f"${rowCount}%-10d ${offDuration}%-12d ${debugDuration}%-12d ${overhead}%+.2f")
    }
    info("=" * 70)

    // Assert all tests completed
    assert(results.size == rowCounts.size * logLevels.size, "All test combinations should complete")
  }

  test("Verify ParquetAppender buffer threshold impact") {
    import spark.implicits._

    val testParquetPath = s"${testBasePath}/buffer-test-logs.parquet"
    val messageCount = 50

    val thresholds = Seq(5, 10, 25, 50)
    val results = scala.collection.mutable.ArrayBuffer[(Int, Long)]()

    for (threshold <- thresholds) {
      // Clean up before each test
      FileUtils.deleteDirectory(new java.io.File(testParquetPath))

      val appender = ParquetAppender.createAppender(
        spark = spark,
        parquetPath = testParquetPath,
        threshold = threshold
      )
      appender.start()

      val startTime = System.nanoTime()

      // Append messages
      for (i <- 1 to messageCount) {
        val event = createLogEvent(s"Message $i")
        appender.append(event)
      }

      appender.stop()

      val durationMs = (System.nanoTime() - startTime) / 1_000_000

      results += ((threshold, durationMs))
    }

    // Print buffer threshold results
    info("")
    info("=" * 70)
    info(s"ParquetAppender Buffer Threshold Test ($messageCount messages)")
    info("=" * 70)
    info(f"${"Threshold"}%-12s ${"Duration (ms)"}%-15s")
    info("-" * 70)

    results.foreach { case (threshold, duration) =>
      info(f"${threshold}%-12d ${duration}%-15d")
    }
    info("=" * 70)

    // Verify all thresholds completed
    assert(results.size == thresholds.size, "All threshold tests should complete")
  }

  private def createLogEvent(message: String, data: Option[String] = None): org.apache.logging.log4j.core.LogEvent = {
    import org.apache.logging.log4j.core.impl.Log4jLogEvent
    import org.apache.logging.log4j.message.SimpleMessage

    val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
    data.foreach(d => contextData.putValue("data", d))

    Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage(message))
      .setLevel(Level.INFO)
      .setContextData(contextData)
      .setTimeMillis(System.currentTimeMillis())
      .build()
  }
}
