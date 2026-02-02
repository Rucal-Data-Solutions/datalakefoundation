package datalake.log

import org.apache.logging.log4j.{Level, ThreadContext}
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AsyncAppender
import org.apache.logging.log4j.core.config.AppenderRef
import org.apache.logging.log4j.core.LoggerContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.sql.Timestamp
import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.functions.{col, to_json}
import datalake.metadata.SparkSessionTest

/**
 * Test suite to verify that ThreadContext/MDC data is correctly captured
 * when using AsyncAppender. This tests for potential context loss issues
 * that can occur with mutable LogEvents and async processing.
 */
class AsyncContextSpec extends AnyFunSuite with SparkSessionTest {

  /**
   * A test appender that captures log entries for verification.
   * Records both the context data at append time and the actual LogEvent.
   */
  class CapturingAppender extends org.apache.logging.log4j.core.appender.AbstractAppender(
    "CapturingAppender",
    null,
    org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
    true,
    org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY
  ) {
    case class CapturedEntry(
      timestamp: Long,
      threadName: String,
      message: String,
      contextData: Map[String, String],
      captureThreadName: String  // Thread that called append() - may differ from log thread with async
    )

    private val entries = ArrayBuffer[CapturedEntry]()
    private val lock = new Object()

    override def append(event: LogEvent): Unit = {
      val context = event.getContextData
      val contextMap = scala.collection.mutable.Map[String, String]()

      // Extract all context data
      context.forEach(new org.apache.logging.log4j.util.BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit = {
          if (value != null) {
            contextMap(key) = value.toString
          }
        }
      })

      lock.synchronized {
        entries += CapturedEntry(
          timestamp = event.getTimeMillis,
          threadName = event.getThreadName,
          message = event.getMessage.getFormattedMessage,
          contextData = contextMap.toMap,
          captureThreadName = Thread.currentThread().getName
        )
      }
    }

    def getCapturedEntries: Seq[CapturedEntry] = lock.synchronized {
      entries.toSeq
    }

    def clear(): Unit = lock.synchronized {
      entries.clear()
    }
  }

  private def createLogEvent(message: String): LogEvent = {
    import org.apache.logging.log4j.core.impl.Log4jLogEvent
    import org.apache.logging.log4j.message.SimpleMessage

    // Capture current ThreadContext into a new StringMap
    val contextData = new org.apache.logging.log4j.util.SortedArrayStringMap()
    val currentContext = ThreadContext.getContext
    if (currentContext != null) {
      currentContext.forEach(new java.util.function.BiConsumer[String, String] {
        override def accept(key: String, value: String): Unit = {
          contextData.putValue(key, value)
        }
      })
    }

    Log4jLogEvent.newBuilder()
      .setMessage(new SimpleMessage(message))
      .setLevel(Level.INFO)
      .setContextData(contextData)
      .setTimeMillis(System.currentTimeMillis())
      .setThreadName(Thread.currentThread().getName)
      .build()
  }

  test("Direct appender captures ThreadContext correctly") {
    val appender = new CapturingAppender()
    appender.start()

    ThreadContext.put("request_id", "req-123")
    ThreadContext.put("user_id", "user-456")

    val event = createLogEvent("Test message with context")
    appender.append(event)

    ThreadContext.clearAll()

    val entries = appender.getCapturedEntries
    entries.size shouldBe 1
    entries.head.contextData.get("request_id") shouldBe Some("req-123")
    entries.head.contextData.get("user_id") shouldBe Some("user-456")

    appender.stop()
  }

  test("AsyncAppender preserves ThreadContext when context is cleared immediately after logging") {
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration

    val capturingAppender = new CapturingAppender()
    capturingAppender.start()
    config.addAppender(capturingAppender)

    val asyncAppender = AsyncAppender.newBuilder()
      .setName("TestAsyncAppender")
      .setAppenderRefs(Array(AppenderRef.createAppenderRef(capturingAppender.getName, null, null)))
      .setConfiguration(config)
      .setBlocking(true)
      .setBufferSize(128)
      .build()
    asyncAppender.start()

    try {
      // Simulate the withData pattern: set context, log, clear context immediately
      ThreadContext.put("data", """{"test": "value"}""")
      ThreadContext.put("data_type", "TestData")

      val event = createLogEvent("Message with context that gets cleared")
      asyncAppender.append(event)

      // Immediately clear context (simulating withData finally block)
      ThreadContext.remove("data")
      ThreadContext.remove("data_type")

      // Give async appender time to process
      Thread.sleep(100)
      asyncAppender.stop() // This flushes the queue

      val entries = capturingAppender.getCapturedEntries
      entries.size shouldBe 1

      // This is the critical assertion: context should be preserved
      val entry = entries.head
      withClue(s"Context data was: ${entry.contextData}") {
        entry.contextData.get("data") shouldBe Some("""{"test": "value"}""")
        entry.contextData.get("data_type") shouldBe Some("TestData")
      }
    } finally {
      capturingAppender.stop()
      config.getAppenders.remove("TestAsyncAppender")
      config.getAppenders.remove("CapturingAppender")
    }
  }

  test("AsyncAppender handles concurrent logging with different contexts") {
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration

    val capturingAppender = new CapturingAppender()
    capturingAppender.start()
    config.addAppender(capturingAppender)

    val asyncAppender = AsyncAppender.newBuilder()
      .setName("TestAsyncAppenderConcurrent")
      .setAppenderRefs(Array(AppenderRef.createAppenderRef(capturingAppender.getName, null, null)))
      .setConfiguration(config)
      .setBlocking(true)
      .setBufferSize(256)
      .build()
    asyncAppender.start()

    val threadCount = 10
    val messagesPerThread = 20
    val barrier = new CyclicBarrier(threadCount)
    val completionLatch = new CountDownLatch(threadCount)
    val executor = Executors.newFixedThreadPool(threadCount)

    try {
      for (threadId <- 0 until threadCount) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              barrier.await() // Synchronize thread start for maximum contention

              for (msgId <- 0 until messagesPerThread) {
                val uniqueData = s"""{"thread":$threadId,"msg":$msgId}"""
                val uniqueType = s"Type_${threadId}_$msgId"

                // Simulate withData pattern
                ThreadContext.put("data", uniqueData)
                ThreadContext.put("data_type", uniqueType)
                ThreadContext.put("thread_id", threadId.toString)

                val event = createLogEvent(s"Thread $threadId Message $msgId")
                asyncAppender.append(event)

                // Immediately clear (like withData's finally block)
                ThreadContext.remove("data")
                ThreadContext.remove("data_type")
                ThreadContext.remove("thread_id")
              }
            } finally {
              completionLatch.countDown()
            }
          }
        })
      }

      // Wait for all threads to complete
      completionLatch.await(30, TimeUnit.SECONDS)

      // Stop async appender to flush all pending events
      asyncAppender.stop()

      val entries = capturingAppender.getCapturedEntries

      // Verify we got all messages
      entries.size shouldBe (threadCount * messagesPerThread)

      // Verify each message has its correct context data (no cross-thread contamination)
      var contextMismatches = 0
      var missingContext = 0

      entries.foreach { entry =>
        val msgParts = entry.message.split(" ")
        val expectedThreadId = msgParts(1).toInt
        val expectedMsgId = msgParts(3).toInt

        val expectedData = s"""{"thread":$expectedThreadId,"msg":$expectedMsgId}"""
        val expectedType = s"Type_${expectedThreadId}_$expectedMsgId"

        entry.contextData.get("data") match {
          case Some(actualData) =>
            if (actualData != expectedData) {
              contextMismatches += 1
              info(s"MISMATCH: Message '${entry.message}' expected data '$expectedData' but got '$actualData'")
            }
          case None =>
            missingContext += 1
            info(s"MISSING: Message '${entry.message}' has no 'data' context")
        }

        entry.contextData.get("data_type") match {
          case Some(actualType) =>
            if (actualType != expectedType) {
              contextMismatches += 1
              info(s"MISMATCH: Message '${entry.message}' expected type '$expectedType' but got '$actualType'")
            }
          case None =>
            missingContext += 1
            info(s"MISSING: Message '${entry.message}' has no 'data_type' context")
        }
      }

      info(s"Total messages: ${entries.size}")
      info(s"Context mismatches: $contextMismatches")
      info(s"Missing context: $missingContext")

      // These assertions will fail if there's context loss/contamination
      withClue("Context data mismatches detected - AsyncAppender may have context issues") {
        contextMismatches shouldBe 0
      }
      withClue("Missing context data detected - ThreadContext not properly captured") {
        missingContext shouldBe 0
      }

    } finally {
      executor.shutdown()
      capturingAppender.stop()
      config.getAppenders.remove("TestAsyncAppenderConcurrent")
      config.getAppenders.remove("CapturingAppender")
    }
  }

  test("ParquetAppender with AsyncAppender preserves context under load") {
    val testPath = s"$testBasePath/async-context-test.parquet"

    val parquetAppender = ParquetAppender.createAppender(
      spark = spark,
      parquetPath = testPath,
      threshold = 5
    )
    parquetAppender.start()

    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration
    config.addAppender(parquetAppender)

    val asyncAppender = AsyncAppender.newBuilder()
      .setName("TestAsyncParquet")
      .setAppenderRefs(Array(AppenderRef.createAppenderRef(parquetAppender.getName, null, null)))
      .setConfiguration(config)
      .setBlocking(true)
      .setBufferSize(128)
      .build()
    asyncAppender.start()

    val threadCount = 5
    val messagesPerThread = 10
    val barrier = new CyclicBarrier(threadCount)
    val completionLatch = new CountDownLatch(threadCount)
    val executor = Executors.newFixedThreadPool(threadCount)

    try {
      for (threadId <- 0 until threadCount) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              barrier.await()

              for (msgId <- 0 until messagesPerThread) {
                val uniqueData = s"""{"thread":$threadId,"msg":$msgId}"""

                ThreadContext.put("data", uniqueData)
                ThreadContext.put("data_type", "AsyncTest")

                val event = createLogEvent(s"Thread $threadId Message $msgId")
                asyncAppender.append(event)

                ThreadContext.remove("data")
                ThreadContext.remove("data_type")
              }
            } finally {
              completionLatch.countDown()
            }
          }
        })
      }

      completionLatch.await(30, TimeUnit.SECONDS)
      asyncAppender.stop()
      parquetAppender.stop()

      // Read back the parquet file and verify context data
      val logs = spark.read.parquet(testPath)
        .withColumn("data_json", to_json(col("data")))
      val rows = logs.collect()

      rows.size shouldBe (threadCount * messagesPerThread)

      var contextIssues = 0
      rows.foreach { row =>
        val message = row.getAs[String]("message")
        val data = Option(row.getAs[String]("data_json"))

        val msgParts = message.split(" ")
        val expectedThreadId = msgParts(1).toInt
        val expectedMsgId = msgParts(3).toInt
        val expectedData = s"""{"msg":$expectedMsgId,"thread":$expectedThreadId}"""

        data match {
          case Some(actualData) if actualData != expectedData =>
            contextIssues += 1
            info(s"PARQUET MISMATCH: '$message' expected '$expectedData' got '$actualData'")
          case None =>
            contextIssues += 1
            info(s"PARQUET MISSING: '$message' has null data")
          case _ => // OK
        }
      }

      info(s"Parquet context issues: $contextIssues out of ${rows.size} messages")

      withClue("Context data issues in Parquet output") {
        contextIssues shouldBe 0
      }

    } finally {
      executor.shutdown()
      config.getAppenders.remove("TestAsyncParquet")
      config.getAppenders.remove(parquetAppender.getName)
    }
  }

  test("Rapid context switching stress test") {
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration

    val capturingAppender = new CapturingAppender()
    capturingAppender.start()
    config.addAppender(capturingAppender)

    val asyncAppender = AsyncAppender.newBuilder()
      .setName("TestAsyncStress")
      .setAppenderRefs(Array(AppenderRef.createAppenderRef(capturingAppender.getName, null, null)))
      .setConfiguration(config)
      .setBlocking(true)
      .setBufferSize(512)
      .build()
    asyncAppender.start()

    val iterations = 100

    try {
      // Rapid fire logging with immediate context clearing
      for (i <- 0 until iterations) {
        ThreadContext.put("iteration", i.toString)
        ThreadContext.put("data", s"""{"i":$i}""")

        val event = createLogEvent(s"Iteration $i")
        asyncAppender.append(event)

        // Immediately clear and set different context
        ThreadContext.clearAll()

        // Set different context that should NOT appear in previous log
        ThreadContext.put("iteration", "WRONG")
        ThreadContext.put("data", """{"wrong":"context"}""")
        ThreadContext.clearAll()
      }

      asyncAppender.stop()

      val entries = capturingAppender.getCapturedEntries
      entries.size shouldBe iterations

      var wrongContext = 0
      entries.foreach { entry =>
        val expectedIteration = entry.message.split(" ")(1)

        entry.contextData.get("iteration") match {
          case Some(actual) if actual != expectedIteration =>
            wrongContext += 1
            info(s"WRONG CONTEXT: '${entry.message}' has iteration=$actual, expected $expectedIteration")
          case Some("WRONG") =>
            wrongContext += 1
            info(s"CONTAMINATION: '${entry.message}' has 'WRONG' iteration value")
          case _ => // OK or missing
        }

        entry.contextData.get("data") match {
          case Some("""{"wrong":"context"}""") =>
            wrongContext += 1
            info(s"CONTAMINATION: '${entry.message}' has wrong context data")
          case _ => // OK
        }
      }

      info(s"Stress test: {wrongContext} context issues out of $iterations iterations")

      withClue("Context contamination detected in stress test") {
        wrongContext shouldBe 0
      }

    } finally {
      capturingAppender.stop()
      config.getAppenders.remove("TestAsyncStress")
      config.getAppenders.remove("CapturingAppender")
    }
  }
}
