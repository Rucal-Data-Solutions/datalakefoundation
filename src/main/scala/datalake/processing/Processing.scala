package datalake.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession }

import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._


trait ProcessStrategy {
  def process(processing: Processing): Unit
}

case class DatalakeSource(source: DataFrame, watermark_values: Option[List[(String, Any)]])

// Bronze(Source) -> Silver(Target)
class Processing(entity: Entity, sliceFile: String) {
  implicit val environment = entity.Environment
  val entity_id = entity.Id
  val primaryKeyColumnName: String = s"PK_${entity.Name}"
  val columns = entity.Columns
  val paths = entity.getPaths
  val watermarkColumns = entity.Watermark.map(wm => wm.Column_Name)
  val sliceFileFullPath: String = s"${paths.BronzePath}/${sliceFile}"
  val destination: String = paths.SilverPath

  val columnsToRename = columns
    .filter(c => c.NewName != "")
    .map(c => (c.Name, c.NewName))
    .toMap

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def getSource: DatalakeSource = {

    println(f"loading slice: ${sliceFileFullPath}")
    var dfSlice = spark.read.format("parquet").load(sliceFileFullPath)

    val timezoneId = environment.Timezone.toZoneId
    val now = LocalDateTime.now(timezoneId)

    // Check for HashColumn(SourceHash) in slice and add if it doesnt exits. (This must come first, the rest of fields are calculated)
    if (Utils.hasColumn(dfSlice, "SourceHash") == false) {
      dfSlice = dfSlice.withColumn(
        "SourceHash",
        sha2(concat_ws("", dfSlice.columns.map(c => col("`" + c + "`").cast("string")): _*), 256)
      )
    }

    // Check PK in slice, add if it doesnt exits.
    if (primaryKeyColumnName != null && Utils.hasColumn(dfSlice, primaryKeyColumnName) == false) {
      val pkColumns = List(entity.getBusinessKey map col: _*)
      dfSlice = dfSlice.withColumn(primaryKeyColumnName, sha2(concat_ws("_", pkColumns: _*), 256))
    }

    val watermark_values = if (watermarkColumns.nonEmpty) 
      Some(watermarkColumns.map(colName => (colName, dfSlice.agg(max(colName)).head().get(0))))
    else
      None

    // Cast all columns according to metadata (if available)
    dfSlice = columns.foldLeft(dfSlice) { (tempdf, column) =>
      tempdf.withColumn(
        column.Name,
        col(s"`${column.Name}`").cast(column.DataType)
      )
    }

    // Rename columns that need renaming
    dfSlice = dfSlice.select(
      dfSlice.columns.map(c => col("`" + c + "`").as(columnsToRename.getOrElse(c, c))): _*
    )

    // check for the deleted column (source can identify deletes with this record) add if it doesn't exist
    if (Utils.hasColumn(dfSlice, "deleted") == false) {
      dfSlice = dfSlice.withColumn("deleted", lit("false").cast("Boolean"))
    }

    // finaly, add lastseen date
    dfSlice = dfSlice.withColumn("lastSeen", to_timestamp(lit(now.toString)))

    new DatalakeSource(dfSlice, watermark_values)
  }

  def WriteWatermark(watermark_values: Option[List[(String, Any)]]): Unit ={
    // Write the watermark values to system table
    val watermarkData: WatermarkData = new WatermarkData
    val timezoneId = environment.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

    val current_watermark = watermark_values match {
      case Some(watermarkList) =>
        val data = watermarkList.map(wm => Row(entity_id, wm._1, timestamp_now, wm._2.toString()))
        watermarkData.Append(data.toSeq)
      case None => println("no watermark defined")
    }
  }

  def process(stategy: ProcessStrategy = entity.ProcessType): Unit = {
    stategy.process(this)
  }

  
}






