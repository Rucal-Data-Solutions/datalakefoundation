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

  def Process(processing: Processing): Unit = {
    implicit val env:Environment = processing.environment

    // first time? Do A full load
    if (!FileOperations.exists(processing.destination)) {
      logger.info("Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source_df

      val partition_values: Array[String] = datalake_source.partition_columns match {
        case Some(part) => part.toArray.map(c => s"target.${c._1} IN(${c._2.toString()})")
        case None => Array.empty[String]
      }

      val deltaTable = DeltaTable.forPath(processing.destination)
      val explicit_partFilter = partition_values.mkString(" AND ")

      val schemaChanges = source.datalake_schemacompare(deltaTable.toDF.schema)
      if (schemaChanges.nonEmpty) {
        logger.warn(s"Schema changes detected during Merge processing:")
        schemaChanges.foreach(change => logger.warn(s"  ${change.toString}"))
      }
      
      logger.debug("Starting Merge operation")
      
      deltaTable
        .as("target")
        .merge(
          source.as("source"),
            f"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName}" +
            (if (partition_values.nonEmpty) s" AND $explicit_partFilter" else "")
        )
        .whenMatched(s"source.${env.SystemFieldPrefix}deleted = true")
        .update(Map(s"${env.SystemFieldPrefix}deleted" -> lit("true"), s"${env.SystemFieldPrefix}lastSeen" -> col(s"source.${env.SystemFieldPrefix}lastSeen")))
        .whenMatched(s"source.${env.SystemFieldPrefix}SourceHash != target.${env.SystemFieldPrefix}SourceHash")
        .updateAll
        .whenMatched(s"source.${env.SystemFieldPrefix}SourceHash == target.${env.SystemFieldPrefix}SourceHash")
        .update(Map(s"${env.SystemFieldPrefix}lastSeen" -> col(s"source.${env.SystemFieldPrefix}lastSeen")))
        .whenNotMatched(s"source.${env.SystemFieldPrefix}deleted = false")
        .insertAll
        .execute()

    }
    


  }
}
