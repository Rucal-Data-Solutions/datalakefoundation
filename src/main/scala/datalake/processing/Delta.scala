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
import datalake.metadata._
import datalake.implicits._
import datalake.utils._
import org.apache.spark.sql.SaveMode

final object Delta extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

  def process(processing: Processing) = {
    val source: DataFrame = processing.getSource()

    // first time? Do A full load
    if (FileOperations.exists(processing.destination) == false) {
      Full.process(processing)
    } else {
      val deltaTable = DeltaTable.forPath(processing.destination)

      deltaTable
        .as("target")
        .merge(
          source.as("source"),
          "source." + processing.primaryKeyColumnName + " = target." + processing.primaryKeyColumnName
        )
        .whenMatched("source.deleted = true")
        .update(Map("deleted" -> lit("true")))
        .whenMatched("source.SourceHash != target.SourceHash")
        .updateAll
        .whenMatched("source.SourceHash == target.SourceHash")
        .update(Map("lastSeen" -> col("source.lastSeen")))
        .whenNotMatched("source.deleted = false")
        .insertAll
        .execute()

    }

  }
}