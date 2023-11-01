package datalake.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession, Dataset }
import scala.util.{ Try, Success, Failure }

import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._

abstract class ProcessStrategy {

  final val Name: String = {
    val cls = this.getClass()
    cls.getSimpleName().dropRight(1).toLowerCase()
  }
  def Process(processing: Processing): Unit
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
    val dfSlice = spark.read.format("parquet").load(sliceFileFullPath)

    val watermark_values = if (watermarkColumns.nonEmpty)
      Some(watermarkColumns.map(colName => (colName, dfSlice.agg(max(colName)).head().get(0))))
    else
      None

    val transformedDF = dfSlice
      .transform(addCalculatedColumns)
      .transform(calculateSourceHash)
      .transform(addPrimaryKey)
      .transform(castColumns)
      .transform(renameColumns)
      .transform(addDeletedColumn)
      .transform(addLastSeen)

    new DatalakeSource(transformedDF, watermark_values)
  }

  // Check for HashColumn(SourceHash) in slice and add if it doesnt exits. (This must come first, the rest of fields are calculated)
  private def calculateSourceHash(input: Dataset[Row]): Dataset[Row] =
    if (Utils.hasColumn(input, "SourceHash") == false) {
      return input.withColumn(
        "SourceHash",
        sha2(concat_ws("", input.columns.map(c => col("`" + c + "`").cast("string")): _*), 256)
      )
    } else
      return input

  // Check PK in slice, add if it doesnt exits.
  private def addPrimaryKey(input: Dataset[Row]): Dataset[Row] =
    if (primaryKeyColumnName != null && Utils.hasColumn(input, primaryKeyColumnName) == false) {
      val pkColumns = entity.Columns("businesskey").map(c => col(c.Name))
      input.withColumn(primaryKeyColumnName, sha2(concat_ws("_", pkColumns: _*), 256))
    } else {
      input
    }

  // Cast all columns according to metadata (if available)
  private def castColumns(input: Dataset[Row]): Dataset[Row] =
    columns.foldLeft(input) { (tempdf, column) =>
      val newDataType = column.DataType

      newDataType match {
        case Some(dtype) => tempdf.withColumn(column.Name, col(s"`${column.Name}`").cast(dtype))
        case None        => tempdf
      }
    }

  // Rename columns that need renaming
  private def renameColumns(input: Dataset[Row]): Dataset[Row] =
    columnsToRename.foldLeft(input) {(tempdb, rencol) =>
      input.withColumnRenamed(rencol._1, rencol._2)  
    }


  // check for the deleted column (source can identify deletes with this record) add if it doesn't exist
  private def addDeletedColumn(input: Dataset[Row]): Dataset[Row] =
    if (Utils.hasColumn(input, "deleted") == false) {
      input.withColumn("deleted", lit("false").cast("Boolean"))
    } else {
      input
    }

  // add lastseen date
  private def addLastSeen(input: Dataset[Row]): Dataset[Row] = {
    val timezoneId = environment.Timezone.toZoneId
    val now = LocalDateTime.now(timezoneId)
    input.withColumn("lastSeen", to_timestamp(lit(now.toString)))
  }

  private def addCalculatedColumns(input: Dataset[Row]): Dataset[Row] =
    entity.Columns("calculated").foldLeft(input) { (tempdf, column) =>
      Try {
        tempdf.withColumn(column.Name, expr(column.Expression).cast(column.DataType.getOrElse(StringType)))
      } match {
        case Success(newDf) =>
          newDf
        case Failure(e) =>
          // Log the error message and the failing expression
          println(
            s"Failed to add calculated column ${column.Name} with expression ${column.Expression}. Error: ${e.getMessage}"
          )
          // Continue processing with the DataFrame as it was before the failure
          tempdf.withColumn(column.Name, lit(null).cast(column.DataType.getOrElse(StringType)))
      }
    }

  def WriteWatermark(watermark_values: Option[List[(String, Any)]]): Unit = {
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

  def Process(stategy: ProcessStrategy = entity.ProcessType): Unit =
    stategy.Process(this)

}
