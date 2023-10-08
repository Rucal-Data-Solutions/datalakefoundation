package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, Row }

import java.util.TimeZone
import java.time.LocalDateTime
import io.delta.tables._
import org.apache.spark.sql.SaveMode

import datalake.core._
import datalake.metadata._

final object Full extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def process(processing: Processing) {
    implicit val env: Environment = processing.environment

    val datalake_source = processing.getSource
    val source: DataFrame = datalake_source.source

    FileOperations.remove(processing.destination, true)
    source.write.format("delta").mode(SaveMode.Overwrite).save(processing.destination)

    // Write the watermark values to system table
    val watermarkData: WatermarkData = new WatermarkData
    val timezoneId = env.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

    val current_watermark = datalake_source.watermark_values.get.map(wm =>
      Row(processing.entity_id, wm._1, timestamp_now, wm._2.toString())
    )
    watermarkData.Append(current_watermark.toSeq)
  }
}
