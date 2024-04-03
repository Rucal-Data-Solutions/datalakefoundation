package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column }
import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.core.implicits._
import datalake.metadata._

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Now


final object Delta extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def Process(processing: Processing): Unit = {
    implicit val env:Environment = processing.environment

    // first time? Do A full load
    if (FileOperations.exists(processing.destination) == false) {
      println("DEBUG: Diverting to full load (First Run)")
      Full.Process(processing)
    } else {
      val datalake_source = processing.getSource
      val source: DataFrame = datalake_source.source

      val partition_values: Array[String] = datalake_source.partition_columns match {
        case Some(part) => part.toArray.map(c => s"target.${c._1} IN(${c._2.toString()})")
        case None => Array.empty[String]
      }

      val deltaTable = DeltaTable.forPath(processing.destination)
      val explicit_partFilter = partition_values.mkString(" AND ")

      deltaTable
        .as("target")
        .merge(
          source.as("source"),
            f"source.${processing.primaryKeyColumnName} = target.${processing.primaryKeyColumnName}" +
            (if (partition_values.nonEmpty) s" AND $explicit_partFilter" else "")
        )
        .whenMatched("source.deleted = true")
        .update(Map("deleted" -> lit("true"), "lastSeen" -> col("source.lastSeen")))
        .whenMatched("source.SourceHash != target.SourceHash")
        .updateAll
        .whenMatched("source.SourceHash == target.SourceHash")
        .update(Map("lastSeen" -> col("source.lastSeen")))
        .whenNotMatched("source.deleted = false")
        .insertAll
        .execute()

        processing.WriteWatermark(datalake_source.watermark_values)
    }
    


  }
}
