package datalake.processing

import datalake.metadata._
import datalake.core.implicits._
import datalake.core.FileOperations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode }

import io.delta.tables._

final object Historic extends ProcessStrategy {

  def Process(processing: Processing): Unit = {
    implicit val env:Environment = processing.environment

    if (!FileOperations.exists(processing.destination)) {
      logger.info("Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df

      val part_values: List[String] = datalake_source.partition_columns.getOrElse(List.empty).map(_._1)
      val partition_filters: Array[String] = datalake_source.partition_columns match {
        case Some(part) => part.toArray.map(c => s"target.${c._1} IN(${c._2.toString()})")
        case None => Array.empty[String]
      }
      
      val deltaTable = DeltaTable.forPath(processing.destination)
      val explicit_partFilter = partition_filters.mkString(" AND ")
      
      // Check for schema changes
      val schemaChanges = source.datalake_schemacompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Historic processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }


      // merge operation for SCD Type 2
      deltaTable.as("target")
        .merge(
          source.as("source"),
          s"target.${processing.primaryKeyColumnName} = source.${processing.primaryKeyColumnName} AND target.IsCurrent = true" +
          (if (partition_filters.nonEmpty) 
            s" AND $explicit_partFilter" else "")
        )
        .whenMatched("target.SourceHash <> source.SourceHash")
        .updateExpr(
          Map(
            "ValidTo" -> s"cast('${processing.processingTime}' as timestamp)",
            "IsCurrent" -> "false"
          )
        )
        .whenNotMatched() // Insert actual new records (new PrimaryKey)
        .insertAll()
        .execute()

      // Insert new versions for updated records
      val updatedRecords = source.as("source")
        .join(
          deltaTable.toDF.as("target"),
          expr(s"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName} AND target.IsCurrent = false AND target.ValidTo = cast('${processing.processingTime}' as timestamp)")
        )
        .select("source.*")

      updatedRecords.write
        .format("delta")
        .mode("append")
        .save(processing.destination)
    }
  }
}