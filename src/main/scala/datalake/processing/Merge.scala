package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, Row, SaveMode }

import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp

import io.delta.tables._

import datalake.core._
import datalake.core.implicits._
import datalake.metadata._
import datalake.log.{DatalakeLogManager, ProcessingSummary}



final object Merge extends ProcessStrategy {

  def Process(processing: Processing)(implicit spark: SparkSession): Unit = {
    implicit val env:Environment = processing.environment

    val isFirstRun = this.isFirstRun(processing.destination)

    if (isFirstRun) {
      logger.info("Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df
      val recordCount = source.count()
      

      val partition_values: Array[String] = datalake_source.partition_columns match {
        case Some(part) => part.toArray.map(c => s"target.${c._1} IN(${c._2.toString()})")
        case None => Array.empty[String]
      }

      val deltaTable = processing.destination match {
        case PathLocation(path) => DeltaTable.forPath(path)
        case TableLocation(table) => DeltaTable.forName(table)
      }
      val explicit_partFilter = partition_values.mkString(" AND ")
      val partitionFilterColumn = if (partition_values.nonEmpty) Some(expr(explicit_partFilter)) else None
      // Build the watermark window condition to scope delete detection
      // This creates a filter like: target.watermark_col >= previousValue AND target.watermark_col <= currentValue
      val watermarkCondition = watermarkWindowCondition(
        processing,
        datalake_source.current_watermark_values,
        datalake_source.watermark_values,
        deltaTable.toDF
      )

      // inferDeletesFromMissing: Automatically soft-delete records that exist in the target
      // but are missing from the source slice, within the watermark window.
      //
      // How it works:
      // 1. The watermark defines the window of data we expect in this slice
      // 2. Any target records in that window that don't match source records are considered deleted
      // 3. Uses whenNotMatchedBySource to find target-only records
      //
      // Safety filters applied:
      // - Watermark window: Only considers records in the expected data range
      // - Partition filter: Only considers records in partitions present in the source
      // - Not already deleted: Avoids repeatedly updating already-deleted records
      //
      // Edge cases to be aware of:
      // - First run: No previous watermark, so no deletes will be inferred (safe default)
      // - NULL watermarks: Records with NULL in watermark columns won't be considered
      // - Gaps in processing: Large gaps between previous and current watermarks may miss deletes
      val deleteCondition =
        if (processing.inferDeletesFromMissing) {
          watermarkCondition.map { wmCondition =>
            // Only mark records as deleted if they're not already deleted
            val notAlreadyDeleted = col(s"target.${env.SystemFieldPrefix}deleted") === lit(false)
            val withPartition = partitionFilterColumn.map(wmCondition && _).getOrElse(wmCondition)
            withPartition && notAlreadyDeleted
          }
        } else None

      val schemaChanges = source.datalakeSchemaCompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Merge processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }

      // Count soft deletes from source alone (no join needed)
      val softDeletes = source.filter(col(s"${env.SystemFieldPrefix}deleted") === true).count()

      logger.debug("Starting Merge operation")
      
      val mergeBuilder = deltaTable
        .as("target")
        .merge(
          source.as("source"),
            f"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName}" +
            (if (partition_values.nonEmpty) s" AND $explicit_partFilter" else "")
        )
        .whenMatched(s"source.${env.SystemFieldPrefix}deleted = true")
        .update(Map(s"${env.SystemFieldPrefix}deleted" -> lit(true), s"${env.SystemFieldPrefix}lastSeen" -> col(s"source.${env.SystemFieldPrefix}lastSeen")))
        .whenMatched(s"source.${env.SystemFieldPrefix}SourceHash != target.${env.SystemFieldPrefix}SourceHash")
        .updateAll()
        .whenMatched(s"source.${env.SystemFieldPrefix}SourceHash == target.${env.SystemFieldPrefix}SourceHash")
        .update(Map(s"${env.SystemFieldPrefix}lastSeen" -> col(s"source.${env.SystemFieldPrefix}lastSeen")))
        .whenNotMatched(s"source.${env.SystemFieldPrefix}deleted = false")
        .insertAll()

      // Add the delete condition handling
      val mergeWithDeletes = deleteCondition match {
        case Some(cond) =>
          mergeBuilder.whenNotMatchedBySource(cond).update(
            Map(
              s"${env.SystemFieldPrefix}deleted" -> lit(true),
              s"${env.SystemFieldPrefix}lastSeen" -> to_timestamp(lit(processing.processingTime))
            )
          )
        case None => mergeBuilder
      }

      // Delta 4.0 returns metrics DataFrame directly from execute()
      val mergeMetrics = mergeWithDeletes.execute()
      val metricsRow = mergeMetrics.first()
      val inserted = metricsRow.getAs[Long]("num_inserted_rows")

      // Derive updated count from source-side arithmetic:
      // Every non-deleted source record either matched an existing target record (update/touch)
      // or was inserted as new. The real-vs-touch distinction is not available from Delta metrics.
      val updated = recordCount - inserted - softDeletes

      val summary = ProcessingSummary(
        recordsInSlice = recordCount,
        inserted = inserted,
        updated = updated,
        deleted = softDeletes,
        unchanged = 0,
        touched = 0,
        durationMs = System.currentTimeMillis() - processing.startTimeMs,
        entityId = Some(processing.entity_id.toString),
        sliceFile = None
      )
      DatalakeLogManager.logSummary(logger, summary)
    }
  }
}
