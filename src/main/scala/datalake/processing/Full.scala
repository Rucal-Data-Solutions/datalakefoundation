package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, Row }

import java.util.TimeZone
import java.time.LocalDateTime
import io.delta.tables._
import io.delta.implicits._

import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.streaming.StreamingQuery

import datalake.core._
import datalake.metadata._
import datalake.log.{DatalakeLogManager, ProcessingSummary}

final object Full extends ProcessStrategy {

  def Process(
      processing: Processing
  )(implicit spark: SparkSession): Option[StreamingQuery] = {
    implicit val env: Environment = processing.environment

    val datalake_source = processing.getSource
    val source: DataFrame = datalake_source.source_df
    val recordCount = source.count()

    // Only an empty slice needs a look at the destination; the happy path stays untouched.
    if (recordCount == 0) {
      val target = processing.destination match {
        case PathLocation(path)  => s"path '$path'"
        case TableLocation(name) => s"table '$name'"
      }
      if (destinationHasRows(processing.destination)) {
        if (!processing.allowEmptyFullLoad) {
          throw EmptyFullLoadException(
            s"Entity ${processing.entity_id} (${processing.getEntity.Name}): slice " +
              s"'${processing.sliceName}' contains no rows. A full load would overwrite " +
              s"$target, which still holds data. Nothing was written. If empty slices are " +
              "expected for this entity, set \"allow_empty_full_load\": true in the entity " +
              "or connection settings."
          )
        }
        logger.warn(
          s"Empty slice '${processing.sliceName}' with allow_empty_full_load enabled: " +
            s"overwriting $target with 0 rows."
        )
      } else {
        logger.info(
          s"Empty slice '${processing.sliceName}': $target does not exist or is already " +
            "empty, writing an empty table."
        )
      }
    }

    val part_values: List[String] =
      datalake_source.partition_columns.getOrElse(List.empty).map(_._1)

    if(part_values.length > 0){
      logger.debug(s"Defined partitions: ${part_values.mkString(", ")}")
    }
    else {
      logger.debug(s"No partitions defined")
    }

    val writer = source.write
      .partitionBy(part_values: _*)
      .mode(SaveMode.Overwrite)
      .options(
      Map[String, String](
        ("partitionOverwriteMode", "dynamic")
      )
      )

    processing.destination match {
      case PathLocation(path) => 
        logger.info(s"Writing to path: $path")
        writer.delta(path)
      case TableLocation(table) =>
        logger.info(s"Writing to table: $table")
        // Extract database name and create if it doesn't exist
        val (databaseName, tableName) = table.split("\\.") match {
          case Array(db, tbl) => (db, tbl)
          case Array(tbl) => ("default", tbl)
          case _ => throw new IllegalArgumentException(s"Invalid table name format: $table")
        }
        
        // Create database if it doesn't exist
        logger.debug(s"Creating database '$databaseName' if it doesn't exist")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS `$databaseName`")
        
        // Use format("delta") to ensure we create a Delta table, not a regular Hive table
        writer.format("delta").saveAsTable(table)
    }

    val summary = ProcessingSummary(
      recordsInSlice = recordCount,
      inserted = recordCount,
      durationMs = System.currentTimeMillis() - processing.startTimeMs,
      entityId = Some(processing.entity_id.toString),
      sliceFile = None
    )
    DatalakeLogManager.logSummary(logger, summary)
    None
  }
}
