package datalake.processing

import datalake.metadata._
import datalake.core.implicits._
import datalake.log.{DatalakeLogManager, ProcessingSummary}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode }

import io.delta.tables._

final object Historic extends ProcessStrategy {

  def Process(processing: Processing)(implicit spark: SparkSession): Unit = {
    implicit val env:Environment = processing.environment

    val isFirstRun = this.isFirstRun(processing.destination)

    if (isFirstRun) {
      logger.info("Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      logger.debug("Incremental load (Subsequent Runs)")
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df
      val recordCount = source.count()

      val part_values: List[String] = datalake_source.partition_columns.getOrElse(List.empty).map(_._1)
      val partition_filters: Array[String] = datalake_source.partition_columns match {
        case Some(part) => part.toArray.map(c => s"target.${c._1} IN(${c._2.toString()})")
        case None => Array.empty[String]
      }
      
      val deltaTable = processing.destination match {
        case PathLocation(path) => DeltaTable.forPath(path)
        case TableLocation(table) => DeltaTable.forName(table)
      }
      val explicit_partFilter = partition_filters.mkString(" AND ")
      val partitionFilterColumn = if (partition_filters.nonEmpty) Some(expr(explicit_partFilter)) else None
      // Build the watermark window condition to scope delete detection
      val watermarkCondition = watermarkWindowCondition(
        processing,
        datalake_source.current_watermark_values,
        datalake_source.watermark_values,
        deltaTable.toDF
      )

      // inferDeletesFromMissing for SCD Type 2: Mark current records as deleted if missing from source
      // Similar to Merge strategy, but only operates on current versions (IsCurrent=true)
      // When a record is marked deleted, both deleted=true AND IsCurrent=false are set,
      // which prevents it from being updated again on subsequent runs
      val deleteCondition =
        if (processing.inferDeletesFromMissing) {
          watermarkCondition.map { wmCondition =>
            // IsCurrent=true ensures we only operate on current versions, not historical records
            // This also excludes already-deleted records since they have IsCurrent=false
            val isCurrentFilter = col(s"target.${env.SystemFieldPrefix}IsCurrent") === lit(true)
            val withPartition = partitionFilterColumn.map(wmCondition && _).getOrElse(wmCondition)
            withPartition && isCurrentFilter
          }
        } else None
      
      // Check for schema changes
      val schemaChanges = source.datalakeSchemaCompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Historic processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }

      logger.debug("Starting Historic Merge operation")

      // merge operation for SCD Type 2
      val mergeBuilder = deltaTable.as("target")
        .merge(
          source.as("source"),
          s"target.${processing.primaryKeyColumnName} = source.${processing.primaryKeyColumnName} AND target.${env.SystemFieldPrefix}IsCurrent = true" +
          (if (partition_filters.nonEmpty) 
            s" AND $explicit_partFilter" else "")
        )
        .whenMatched(s"target.${env.SystemFieldPrefix}SourceHash <> source.${env.SystemFieldPrefix}SourceHash")
        .updateExpr(
          Map(
            s"${env.SystemFieldPrefix}ValidTo" -> s"cast('${processing.processingTime}' as timestamp)",
            s"${env.SystemFieldPrefix}IsCurrent" -> "false"
          )
        )
        .whenNotMatched() // Insert actual new records (new PrimaryKey)
        .insertAll()

      val mergeWithDeletes = deleteCondition match {
        case Some(cond) =>
          mergeBuilder.whenNotMatchedBySource(cond).update(
            Map(
              s"${env.SystemFieldPrefix}deleted" -> lit(true),
              s"${env.SystemFieldPrefix}IsCurrent" -> lit(false),
              s"${env.SystemFieldPrefix}ValidTo" -> to_timestamp(lit(processing.processingTime)),
              s"${env.SystemFieldPrefix}lastSeen" -> to_timestamp(lit(processing.processingTime))
            )
          )
        case None => mergeBuilder
      }

      // Execute the merge before computing new version rows so the target reflects closed records
      val mergeMetrics = mergeWithDeletes.execute()

      // Insert new versions for updated records
      val updatedRecords = source.as("source")
        .join(
          deltaTable.toDF.as("target"),
          expr(s"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName} AND target.${env.SystemFieldPrefix}IsCurrent = false AND target.${env.SystemFieldPrefix}ValidTo = cast('${processing.processingTime}' as timestamp)")
        )
        .select("source.*")
        .cache()

      val newVersionCount = updatedRecords.count()

      val updatewriter = updatedRecords.write
        .format("delta")
        .mode("append")

      processing.destination match {
        case PathLocation(path) =>
          updatewriter.save(path)
        case TableLocation(table) =>
          updatewriter.saveAsTable(table)
      }

      updatedRecords.unpersist()

      // Derive all metrics from source-side data to ensure the identity:
      //   inserted + updated + unchanged = recordCount
      //
      // updated = newVersionCount (source records that matched a current target record with
      //           a different hash, causing the old version to be closed and a new one inserted)
      // unchanged = source records that matched a current target record with the same hash
      //             (after the merge, these still have IsCurrent=true in the target)
      // inserted = genuinely new source records (no matching PK in target)
      // deleted = target-side metric from whenNotMatchedBySource (reported separately)
      val updated = newVersionCount
      val unchanged = source.as("src")
        .join(
          deltaTable.toDF
            .filter(col(s"${env.SystemFieldPrefix}IsCurrent") === true)
            .as("tgt"),
          col(s"src.${processing.primaryKeyColumnName}") === col(s"tgt.${processing.primaryKeyColumnName}")
        )
        .count()
      val inserted = recordCount - updated - unchanged

      val metricsRow = mergeMetrics.first()
      val deleted = metricsRow.getAs[Long]("num_deleted_rows")

      val summary = ProcessingSummary(
        recordsInSlice = recordCount,
        inserted = inserted,
        updated = updated,
        deleted = deleted,
        unchanged = unchanged,
        durationMs = System.currentTimeMillis() - processing.startTimeMs,
        entityId = Some(processing.entity_id.toString),
        sliceFile = None
      )
      DatalakeLogManager.logSummary(logger, summary)
    }
  }
}
