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

case class DatalakeSource(source: DataFrame, watermark_values: Option[List[(Watermark, Any)]], partition_columns: Option[List[(String, Any)]])
case class DuplicateBusinesskeyException(message: String) extends Exception(message)

// Bronze(Source) -> Silver(Target)
class Processing(entity: Entity, sliceFile: String) {
  implicit val environment = entity.Environment
  val entity_id = entity.Id
  val primaryKeyColumnName: String = s"PK_${entity.Destination}"
  val columns = entity.Columns
  val paths = entity.getPaths
  val watermarkColumns = entity.Watermark
  val sliceFileFullPath: String = s"${paths.bronzepath}/${sliceFile}"
  val destination: String = paths.silverpath

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

    if(dfSlice.count() == 0)
      println("WARNING: Slice contains no data (RowCount=0)")

    val watermark_values = getWatermarkValues(dfSlice, watermarkColumns)

    val transformedDF = dfSlice
      .transform(injectTransformations)
      .transform(addCalculatedColumns)
      .transform(calculateSourceHash)
      .transform(addPrimaryKey)
      .transform(castColumns)
      .transform(renameColumns)
      .transform(addDeletedColumn)
      .transform(addLastSeen)
      .datalake_normalize()

    val part_values = getPartitionValues(transformedDF)

    new DatalakeSource(transformedDF, watermark_values, part_values)
  }

  private def getWatermarkValues(slice: DataFrame, wm_columns: List[Watermark]): Option[List[(Watermark, Any)]] = {
    if (wm_columns.nonEmpty) {
          Some(wm_columns.map(wm => 
              (wm, slice.agg(max(wm.Column_Name)).head().get(0))
          ).filter(_._2 != null))
        } else {
          None
        }
  }

  private def getPartitionValues(slice: DataFrame): Option[List[(String, String)]] = {
    val part_columns = entity.getPartitionColumns

    if (part_columns.nonEmpty) {
      val partitionValues = part_columns.flatMap { column =>
        val values = slice.select(column).distinct().as[String].collect().map(value => s""""${value}"""")
        if (values.nonEmpty) Some((column, values.mkString(","))) else None
      }.toList
      Some(partitionValues)
    } else {
      None
    }
  }

  /**
    * Calculates the source hash for the input dataset.
    *
    * If the input dataset does not have a column named "SourceHash", this method adds the column
    * and calculates the hash value based on the concatenation of all columns in the dataset.
    * The hash value is calculated using the SHA-256 algorithm.
    *
    * @param input The input dataset to calculate the source hash for.
    * @return The input dataset with the "SourceHash" column added, if it didn't exist.
    **/
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
      val returnDF = input.withColumn(primaryKeyColumnName, sha2(concat_ws("_", pkColumns: _*), 256))

      //check if input contains duplicates according to the businesskey, if so, raise an error.
      if(pkColumns.length > 0){
        val duplicates = returnDF.groupBy(pkColumns: _*).agg(count("*").alias("count")).filter("count > 1").select(concat_ws("_", pkColumns: _*).alias("duplicatekey"))
        val dupCount = duplicates.count()
        if(dupCount > 0) {
          duplicates.show(truncate = false)
          throw(new DuplicateBusinesskeyException(f"${dupCount} duplicate key(s) (according to the businesskey) found in slice, can't continue."))
        }
      }

      returnDF
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
    entity.Columns(EntityColumnFilter(HasExpression=true)).foldLeft(input) { (tempdf, column) =>
      Try {
        tempdf.withColumn(column.Name, expr(column.Expression))
      } match {
        case Success(newDf) =>
          newDf
        case Failure(e) =>
          // Log the error message and the failing expression
          println(
            s"Failed to add calculated column ${column.Name} with expression ${column.Expression}. Error: ${e.getMessage}"
          )
          // Continue processing with the DataFrame as it was before the failure
          tempdf
      }
    }
  
  /**
    * Applies transformations from the entity to the input dataset.
    *
    * @param input The input dataset to be transformed.
    * @return The transformed dataset or input if num of transformations=0.
    */
  private def injectTransformations(input: Dataset[Row]): Dataset[Row] ={
    if(!entity.transformations.isEmpty)
      input.selectExpr(entity.transformations:_*)
    else
      input
  }
 
  final def WriteWatermark(watermark_values: Option[List[(Watermark, Any)]]): Unit = {
    watermark_values match {
      case Some(watermarkList) =>
        this.entity.WriteWatermark(watermarkList)
      case None => println("no watermark defined")
    }
  }

  def Process(stategy: ProcessStrategy = entity.ProcessType): Unit =
    stategy.Process(this)

}
