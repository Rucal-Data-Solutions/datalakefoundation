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



final object Merge extends ProcessStrategy {

  def Process(processing: Processing)(implicit spark: SparkSession): Unit = {
    implicit val env:Environment = processing.environment

    // first time? Do A full load
    val isFirstRun = processing.destination match {
      case PathLocation(path) => !DeltaTable.isDeltaTable(spark, path)
      case TableLocation(table) => 
        // For table locations, we need a more robust check
        // isDeltaTable with table names can be unreliable, so we check if table exists and is Delta
        try {
          // First check if table exists at all
          if (!spark.catalog.tableExists(table)) {
            true // Table doesn't exist, so it's first run
          } else {
            // Table exists, check if it's a Delta table by trying to create a DeltaTable reference
            DeltaTable.forName(table)
            false // If we can create DeltaTable reference, it's Delta and not first run
          }
        } catch {
          case _: Exception => 
            // If we can't create DeltaTable reference or any other error, consider it first run
            true
        }
    }

    if (isFirstRun) {
      logger.info("Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df
      

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
      val watermarkCondition = watermarkWindowCondition(
        processing,
        datalake_source.current_watermark_values,
        datalake_source.watermark_values,
        deltaTable.toDF
      )

      val deleteCondition =
        if (processing.inferDeletesFromMissing) {
          watermarkCondition.map { wmCondition =>
            partitionFilterColumn.map(wmCondition && _).getOrElse(wmCondition)
          }
        } else None

      val schemaChanges = source.datalakeSchemaCompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Merge processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }
      
      logger.debug("Starting Merge operation")
      
      val mergeBuilder = deltaTable
        .as("target")
        .merge(
          source.as("source"),
            f"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName}" +
            (if (partition_values.nonEmpty) s" AND $explicit_partFilter" else "")
        )
        .whenMatched(s"source.${env.SystemFieldPrefix}deleted = true")
        .update(Map(s"${env.SystemFieldPrefix}deleted" -> lit("true"), s"${env.SystemFieldPrefix}lastSeen" -> col(s"source.${env.SystemFieldPrefix}lastSeen")))
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

      mergeWithDeletes.execute()
    }
    


  }
}
