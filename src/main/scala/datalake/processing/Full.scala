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
import datalake.core.implicits._
import org.apache.spark.sql.SaveMode
import datalake.core.FileOperations

final object Full extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def process(processing: Processing) {
    val source: DataFrame = processing.getSource.source
    FileOperations.remove(processing.destination, true)
    source.write.format("delta").mode(SaveMode.Overwrite).save(processing.destination)
  }
}