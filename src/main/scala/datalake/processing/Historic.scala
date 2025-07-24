package datalake.processing

import datalake.metadata._
import datalake.core.implicits._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode }

import io.delta.tables._

final object Historic extends ProcessStrategy {

  def Process(processing: Processing)(implicit spark: SparkSession): Unit = {
    implicit val env:Environment = processing.environment

    val isFirstRun = processing.destination match {
      case PathLocation(path) => 
        !DeltaTable.isDeltaTable(spark, path)
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
      logger.debug("Incremental load (Subsequent Runs)")
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df

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
      
      // Check for schema changes
      val schemaChanges = source.datalake_schemacompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Historic processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }

      logger.debug("Starting Historic Merge operation")

      // merge operation for SCD Type 2
      deltaTable.as("target")
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
        .execute()

      // Insert new versions for updated records
      val updatedRecords = source.as("source")
        .join(
          deltaTable.toDF.as("target"),
          expr(s"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName} AND target.${env.SystemFieldPrefix}IsCurrent = false AND target.${env.SystemFieldPrefix}ValidTo = cast('${processing.processingTime}' as timestamp)")
        )
        .select("source.*")

      val updatewriter  =  updatedRecords.write
        .format("delta")
        .mode("append")

      processing.destination match {
        case PathLocation(path) => 
          updatewriter.save(path)
        case TableLocation(table) =>
          updatewriter.saveAsTable(table)
      }
    }
  }
}