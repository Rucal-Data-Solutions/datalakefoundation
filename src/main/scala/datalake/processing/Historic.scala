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
      val source: DataFrame = datalake_source.source

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
      
      // Use same timestamp as initial processing
      val processingTime = processing.getProcessingTime

      // merge operation for SCD Type 2
      deltaTable.as("target")
        .merge(
          source.as("source"),
          s"target.${processing.primaryKeyColumnName} = source.${processing.primaryKeyColumnName} AND target.IsCurrent = true" +
          (if (partition_filters.nonEmpty) s" AND $explicit_partFilter" else "")
        )
        .whenMatched("target.SourceHash <> source.SourceHash")
        .updateExpr(
          Map(
            "ValidTo" -> s"$processingTime",
            "IsCurrent" -> "false"
          )
        )
        .whenNotMatched()
        .insertExpr(
          source.columns
            .map(c => c.toString -> s"source.`${c.toString}`")
            .toMap ++ Map(
                "ValidFrom" -> s"$processingTime",
                "ValidTo" -> "cast('2999-12-31' as timestamp)",
              "IsCurrent" -> "true"
            )
        )
        .execute()
    }
  }
}