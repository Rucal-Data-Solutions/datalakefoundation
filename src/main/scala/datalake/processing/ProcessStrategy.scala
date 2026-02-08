package datalake.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession, Dataset }

import scala.util.{ Try, Success, Failure }
import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._

import org.apache.spark.sql.AnalysisException

import datalake.log.DatalakeLogManager

abstract class ProcessStrategy {
  implicit val spark: SparkSession =
    SparkSession.builder().getOrCreate()
  import spark.implicits._

  @transient protected lazy val logger = DatalakeLogManager.getLogger(this.getClass)

  final val Name: String = {
    val cls = this.getClass()
    cls.getSimpleName().dropRight(1).toLowerCase()
  }

  /**
    * Checks whether this is the first run for the given destination.
    *
    * For PathLocation: checks if a Delta table exists at the path.
    * For TableLocation: checks if the table exists in the catalog and is a Delta table.
    *
    * Only AnalysisException (table/path not found) is treated as a legitimate first run.
    * All other exceptions (permission errors, network issues, catalog misconfiguration)
    * are re-thrown to prevent accidentally triggering a full overwrite on an existing table.
    */
  protected def isFirstRun(destination: OutputLocation): Boolean = {
    destination match {
      case PathLocation(path) =>
        !DeltaTable.isDeltaTable(spark, path)
      case TableLocation(table) =>
        try {
          if (!spark.catalog.tableExists(table)) {
            true
          } else {
            DeltaTable.forName(table)
            false
          }
        } catch {
          case _: AnalysisException =>
            true
          case e: Exception =>
            logger.error(s"Unexpected error checking Delta table existence for '$table'. " +
              "Refusing to default to full overwrite to prevent data loss.", e)
            throw e
        }
    }
  }

  /**
    * Builds a filter that represents the watermark window (previous -> new)
    * so we can scope delete detection to records touched by the current slice.
    *
    * The window uses INCLUSIVE boundaries: target.col >= previousValue AND target.col <= currentValue
    *
    * Edge cases handled:
    * - Returns None if no watermark columns can be matched between previous/current values
    * - Returns None if watermark columns not found in target schema
    * - Logs warnings for missing watermark values or schema mismatches
    * - Records with NULL watermark values in target will NOT match the condition
    *
    * @param processing The processing context
    * @param currentWatermarks Current (previous run) watermark values
    * @param futureWatermarks Future (current slice) watermark values
    * @param targetDf Target DataFrame to check schema against
    * @param targetAlias Alias used for target table in merge
    * @return Optional Column representing the watermark window condition
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

    // Check if we have both previous and future watermarks
    if (previousValues.isEmpty) {
      if (processing.inferDeletesFromMissing) {
        logger.warn("inferDeletesFromMissing is enabled but no previous watermark values available. No deletes will be inferred. This is expected on first run.")
      }
      return None
    }

    if (futureValues.isEmpty) {
      if (processing.inferDeletesFromMissing) {
        logger.warn("inferDeletesFromMissing is enabled but no current watermark values found in slice. No deletes will be inferred.")
      }
      return None
    }

    val schemaByName = targetDf.schema.fields.map(f => f.name -> f.dataType).toMap
    val candidateColumns = previousValues.keySet.intersect(futureValues.keySet)

    if (candidateColumns.isEmpty && processing.inferDeletesFromMissing) {
      logger.warn(s"inferDeletesFromMissing is enabled but no matching watermark columns found between previous (${previousValues.keySet.mkString(", ")}) and current (${futureValues.keySet.mkString(", ")}) watermarks. No deletes will be inferred.")
      return None
    }

    val condition = candidateColumns.foldLeft(Option.empty[Column]) { (acc, colName) =>
      schemaByName.get(colName) match {
        case Some(dataType) =>
          // Strip quotes from watermark values if present (watermark expressions may include SQL quotes like '${last_value}')
          val cleanPreviousValue = previousValues(colName).toString.stripPrefix("'").stripSuffix("'")
          val cleanFutureValue = futureValues(colName).toString.stripPrefix("'").stripSuffix("'")

          val lowerBound = lit(cleanPreviousValue).cast(dataType)
          val upperBound = lit(cleanFutureValue).cast(dataType)
          val columnCondition = col(s"$targetAlias.$colName").geq(lowerBound) && col(s"$targetAlias.$colName").leq(upperBound)

          // Log the watermark window for visibility
          logger.info(s"Delete detection window for column '$colName': [$cleanPreviousValue <= $colName <= $cleanFutureValue]")

          acc.map(_ && columnCondition).orElse(Some(columnCondition))
        case None =>
          logger.warn(s"Watermark column '$colName' not found in target schema; skipping it for delete detection.")
          acc
      }
    }

    if (condition.isEmpty && candidateColumns.nonEmpty) {
      logger.warn("Watermark values available but no matching columns found in target schema for delete detection.")
    }

    if (condition.isDefined && processing.inferDeletesFromMissing) {
      logger.info(s"inferDeletesFromMissing is active. Records in the watermark window that are missing from the source will be marked as deleted.")
      logger.warn("Note: Records with NULL values in watermark columns will NOT be considered for deletion.")
    }

    condition
  }

  def Process(processing: Processing)(implicit spark: SparkSession): Unit
}
