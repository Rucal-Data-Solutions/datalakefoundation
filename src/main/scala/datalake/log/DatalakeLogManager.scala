package datalake.log

import org.apache.logging.log4j.{Level, LogManager, Logger, ThreadContext}
import org.apache.spark.sql.SparkSession
import datalake.metadata.Environment
import java.io.{StringWriter, PrintWriter}

trait LogData {
  def toJson: String
  def dataType: String = this.getClass.getSimpleName.stripSuffix("$")
}

case class ProcessingSummary(
  recordsInSlice: Long,
  inserted: Long = 0,
  updated: Long = 0,
  deleted: Long = 0,
  unchanged: Long = 0,
  touched: Long = 0,
  entityId: Option[String] = None,
  sliceFile: Option[String] = None
) extends LogData {
  def toJson: String = {
    val fields = Seq(
      s""""records_in_slice":$recordsInSlice""",
      s""""inserted":$inserted""",
      s""""updated":$updated""",
      s""""deleted":$deleted""",
      s""""unchanged":$unchanged""",
      s""""touched":$touched"""
    ) ++ entityId.map(id => s""""entity_id":"$id"""") ++
      sliceFile.map(f => s""""slice_file":"$f"""")

    "{" + fields.mkString(",") + "}"
  }
}

object DatalakeLogManager {
  def getLogger(cls: Class[_])(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark)
    LogManager.getLogger(cls)
  }

  def getLogger(cls: Class[_], environment: Environment)(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark, Some(environment))
    LogManager.getLogger(cls)
  }

  def withData[T](data: String, dataType: Option[String] = None)(block: => T): T = {
    ThreadContext.put("data", data)
    dataType.foreach(dt => ThreadContext.put("data_type", dt))
    try {
      block
    } finally {
      ThreadContext.remove("data")
      ThreadContext.remove("data_type")
    }
  }

  def withLogData[T](logData: LogData)(block: => T): T = {
    withData(logData.toJson, Some(logData.dataType))(block)
  }

  def logSummary(logger: Logger, summary: ProcessingSummary, message: String = "Processing complete"): Unit = {
    withLogData(summary) {
      logger.info(message)
    }
  }

  def logException(logger: Logger, level: Level, message: String, throwable: Throwable): Unit = {
    val sw = new StringWriter()
    throwable.printStackTrace(new PrintWriter(sw))
    withData(sw.toString, Some("stacktrace")) {
      logger.log(level, message)
    }
  }

  def getRunId: Option[String] = Log4jConfigurator.getRunId

  def flush(): Unit = Log4jConfigurator.flush()

  def shutdown(): Unit = Log4jConfigurator.shutdown()
}