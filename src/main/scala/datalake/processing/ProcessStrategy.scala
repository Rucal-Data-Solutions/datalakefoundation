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

import org.apache.spark.sql.streaming.StreamingQuery

import datalake.log.DatalakeLogManager

abstract class ProcessStrategy {
  implicit val spark: SparkSession =
    SparkSession.builder().getOrCreate()
  import spark.implicits._

  @transient protected lazy val logger = DatalakeLogManager.getLogger(this.getClass)

  val Name: String = {
    val simpleName = this.getClass.getSimpleName
    val cleaned =
      if (simpleName.endsWith("$")) simpleName.dropRight(1) else simpleName
    cleaned.toLowerCase()
  }

  /**
    * Checks whether this is the first run for the given destination.
    *
    * For PathLocation: checks if a Delta table exists at the path.
    * For TableLocation: checks if the table exists in the catalog and is a Delta table.
    *
    * A missing table (or missing schema/catalog) is a legitimate first run. A table that
    * exists but is not a Delta table throws NotADeltaTableException.
    * All other exceptions (permission errors, network issues, catalog misconfiguration)
    * are re-thrown to prevent accidentally triggering a full overwrite on an existing table.
    */
  protected def isFirstRun(destination: OutputLocation): Boolean = {
    destination match {
      case PathLocation(path) =>
        !DeltaTable.isDeltaTable(spark, path)
      case TableLocation(table) =>
        try {
          val exists =
            try spark.catalog.tableExists(table)
            catch {
              // Missing schema or catalog: the table is not there yet.
              case _: AnalysisException => false
            }
          if (!exists) {
            true
          } else {
            try {
              DeltaTable.forName(table)
              false
            } catch {
              case e: AnalysisException =>
                throw NotADeltaTableException(
                  s"Destination table '$table' exists but is not a Delta table. " +
                    "Nothing was written. Drop or migrate the table, or point the entity " +
                    s"at a different destination. Catalog error: ${e.getMessage}"
                )
            }
          }
        } catch {
          case e: DatalakeException => throw e
          case e: Exception =>
            logger.error(s"Unexpected error checking Delta table existence for '$table'. " +
              "Refusing to default to full overwrite to prevent data loss.", e)
            throw e
        }
    }
  }

  /**
    * Checks whether the destination exists and holds at least one row.
    *
    * Existence is decided by isFirstRun and inherits its semantics: a missing table, schema
    * or catalog counts as "does not exist", an existing non-Delta table throws
    * NotADeltaTableException, and unexpected errors are re-thrown. The row check is a
    * limit(1) read, so it stays cheap. Only call this when the answer is needed (for
    * example on an empty slice), never on the happy path.
    */
  protected def destinationHasRows(destination: OutputLocation): Boolean = {
    if (isFirstRun(destination)) {
      false
    } else {
      val table = destination match {
        case PathLocation(path)  => DeltaTable.forPath(spark, path)
        case TableLocation(name) => DeltaTable.forName(name)
      }
      !table.toDF.isEmpty
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

  def Process(
      processing: Processing
  )(implicit spark: SparkSession): Option[StreamingQuery]
}
