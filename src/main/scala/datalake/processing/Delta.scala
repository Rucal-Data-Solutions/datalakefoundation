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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Now
import datalake.core.FileOperations
import org.apache.hadoop.shaded.org.apache.commons.net.ntp.TimeStamp
import datalake.metadata.Environment

final object Delta extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

  def process(processing: Processing) = {
    implicit val env:Environment = processing.environment

    val datalake_source = processing.getSource
    val source: DataFrame = datalake_source.source

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

        // Write the watermark values to system table
        val watermarkData: WatermarkData = new WatermarkData
        val timezoneId = env.Timezone.toZoneId
        val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

        val current_watermark = datalake_source.watermark_values.get.map( wm => Row(processing.entity_id, wm._1, timestamp_now, wm._2.toString()))
        watermarkData.Append(current_watermark.toSeq)

    }
    


  }
}
