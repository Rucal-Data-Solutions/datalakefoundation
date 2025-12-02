package datalake.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession, Dataset }
// import org.apache.logging.log4j.LogManager

import scala.util.{ Try, Success, Failure }
import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._

import org.apache.logging.log4j.LogManager

abstract class ProcessStrategy {
  implicit val spark: SparkSession =
    SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._
  
  final val logger = LogManager.getLogger(this.getClass())

  final val Name: String = {
    val cls = this.getClass()
    cls.getSimpleName().dropRight(1).toLowerCase()
  }

  /**
    * Builds a filter that represents the watermark window (previous -> new)
    * so we can scope delete detection to records touched by the current slice.
    */
  protected def watermarkWindowCondition(
      processing: Processing,
      currentWatermarks: Option[Array[(Watermark, Any)]],
      futureWatermarks: Option[Array[(Watermark, Any)]],
      targetDf: DataFrame,
      targetAlias: String = "target"
  )(implicit env: Environment): Option[Column] = {
    val previousValues = currentWatermarks
      .map(_.map { case (wm, value) => wm.Column_Name -> value }.toMap)
      .getOrElse(processing.watermarkColumns.flatMap(wm => wm.Value.map(v => wm.Column_Name -> v)).toMap)
    val futureValues =
      futureWatermarks.map(_.map { case (wm, value) => wm.Column_Name -> value }.toMap).getOrElse(Map.empty[String, Any])

    val schemaByName = targetDf.schema.fields.map(f => f.name -> f.dataType).toMap
    val candidateColumns = previousValues.keySet.intersect(futureValues.keySet)

    val condition = candidateColumns.foldLeft(Option.empty[Column]) { (acc, colName) =>
      schemaByName.get(colName) match {
        case Some(dataType) =>
          val lowerBound = lit(previousValues(colName)).cast(dataType)
          val upperBound = lit(futureValues(colName).toString).cast(dataType)
          val columnCondition = col(s"$targetAlias.$colName").geq(lowerBound) && col(s"$targetAlias.$colName").leq(upperBound)
          acc.map(_ && columnCondition).orElse(Some(columnCondition))
        case None =>
          logger.warn(s"Watermark column ${colName} not found in target schema; skipping it for delete detection.")
          acc
      }
    }

    if (condition.isEmpty && candidateColumns.nonEmpty) {
      logger.warn("Watermark values available but no matching columns found in target schema for delete detection.")
    }

    condition
  }

  def Process(processing: Processing)(implicit spark: SparkSession): Unit
}
