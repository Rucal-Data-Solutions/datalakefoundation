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

  def Process(processing: Processing) {
    implicit val env: Environment = processing.environment

    val datalake_source = processing.getSource
    val source: DataFrame = datalake_source.source

    val part_values: List[String] = datalake_source.partition_values.getOrElse(List.empty).map(_._1)

    source.write.format("delta").partitionBy(part_values:_*).mode(SaveMode.Overwrite).save(processing.destination)

    processing.WriteWatermark(datalake_source.watermark_values)
  }
}
