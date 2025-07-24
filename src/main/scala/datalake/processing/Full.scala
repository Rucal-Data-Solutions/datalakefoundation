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

import datalake.core._
import datalake.metadata._

final object Full extends ProcessStrategy {

  def Process(processing: Processing)(implicit spark: SparkSession): Unit = {
    implicit val env: Environment = processing.environment

    val datalake_source = processing.getSource
    val source: DataFrame = datalake_source.source_df

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

  }
}
