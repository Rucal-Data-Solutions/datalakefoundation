package datalake.processing

import org.apache.logging.log4j.{LogManager, Logger, Level}
import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession, Dataset }
import org.apache.spark.broadcast.Broadcast

import scala.util.{ Try, Success, Failure }

import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._
import datalake.log._


case class DatalakeSource(source_df: DataFrame, watermark_values: Option[List[(Watermark, Any)]], partition_columns: Option[List[(String, Any)]])
case class DuplicateBusinesskeyException(message: String) extends DatalakeException(message, Level.ERROR)

// Bronze(Source) -> Silver(Target)
class Processing(entity: Entity, sliceFile: String, options: Map[String, String] = Map.empty) extends Serializable {
  implicit val environment: datalake.metadata.Environment = entity.Environment
  
  private val columns = entity.Columns

  final val entity_id = entity.Id
  final val primaryKeyColumnName: String = s"PK_${entity.Destination}"

  final val paths = entity.getPaths
  final val watermarkColumns = entity.Watermark
  final val entitySettings = entity.Settings
  
  final lazy val sliceFileFullPath: String = s"${paths.bronzepath}/${sliceFile}"
  final lazy val destination: String = paths.silverpath
  // final lazy val destinationTable: String = entity.Connection.Name + "_" + entity.Name

  final lazy val processingTime = {
    if (options.contains("processing.time")) {
      try {
        LocalDateTime.parse(options("processing.time")).toString
      } catch {
        case _: Exception =>
          logger.error(s"Invalid processing.time value: ${options("processing.time")} - falling back to current time.")
          LocalDateTime.now(environment.Timezone.toZoneId).toString
      }
    } else {
      LocalDateTime.now(environment.Timezone.toZoneId).toString
    }
  }

  private implicit val spark: SparkSession =
    SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  @transient 
  private lazy val logger = LogManager.getLogger(this.getClass)

  def getSource: DatalakeSource = {

    logger.info(f"loading slice: ${sliceFileFullPath}")
    val dfSlice = spark.read.format("parquet").load(sliceFileFullPath)

    if (dfSlice.isEmpty)
      logger.warn("Slice contains no data (RowCount=0)")

    val pre_process = dfSlice.transform(injectTransformations)

    val new_watermark_values = getWatermarkValues(pre_process, watermarkColumns)

    val transformedDF = pre_process
      .transform(addCalculatedColumns)
      .transform(calculateSourceHash)
      .transform(addTemporalTrackingColumns)
      .transform(addPrimaryKey)
      .transform(castColumns)
      .transform(renameColumns)
      .transform(addDeletedColumn)
      .transform(addLastSeen)
      .transform(addFilenameColumn(_, sliceFile))
      .datalake_normalize()

    val part_values = getPartitionValues(transformedDF)


    new DatalakeSource(transformedDF, new_watermark_values, part_values)
  }

  private def getWatermarkValues(slice: DataFrame, wm_columns: List[Watermark]): Option[List[(Watermark, Any)]] = {
    if (wm_columns.nonEmpty) {
      val aggExpressions = wm_columns.map(wm => max(col(wm.Column_Name)).alias(wm.Column_Name))
      val row = slice.agg(aggExpressions.head, aggExpressions.tail: _*).head()
      Some(
        wm_columns
          .map(wm => (wm, row.getAs[Any](wm.Column_Name)))
          .filter(_._2 != null)
      )
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
  private def calculateSourceHash(input: Dataset[Row])(implicit env: Environment): Dataset[Row] ={
    val hashfield = s"${env.SystemFieldPrefix}SourceHash"
    if (Utils.hasColumn(input, hashfield) == false) {
      input.withColumn(
        hashfield,
        sha2(concat_ws("", ArraySeq.unsafeWrapArray(input.columns.map(c => col("`" + c + "`").cast("string"))).toSeq: _*), 256)
      )
    } else
      input
  }

  // Check PK in slice, add if it doesn't exits.
  private def addPrimaryKey(input: Dataset[Row]): Dataset[Row] =
    if (primaryKeyColumnName != null && Utils.hasColumn(input, primaryKeyColumnName) == false) {
      val pkColumns = entity.Columns("businesskey").map(c => col(c.Name))
      val returnDF = input.withColumn(primaryKeyColumnName, sha2(concat_ws("_", pkColumns: _*), 256))

      //check if input contains duplicates according to the businesskey, if so, raise an error.
      if(pkColumns.length > 0){
        val duplicates = returnDF.groupBy(pkColumns: _*).agg(count("*").alias("count")).filter("count > 1").select(concat_ws("_", pkColumns: _*).alias("duplicatekey"), col("count"))
        val dupCount = duplicates.count()
        if(dupCount > 0) {
          duplicates.show(truncate = false)

          val error_msg = f"${dupCount} duplicate key(s) (according to the businesskey) found in slice, can't continue."
          throw(DuplicateBusinesskeyException(error_msg))
        }
      }

      returnDF
    } else {
      input
    }

  /**
   * Adds temporal tracking columns (ValidFrom, ValidTo, IsCurrent) to the input Dataset[Row] if the process type is Historic.
   * 
   * @param input The input Dataset[Row] to which the columns will be added.
   * @return The modified Dataset[Row] with temporal tracking columns if the process type is Historic, 
   *         otherwise returns the original input Dataset[Row].
   */
  private def addTemporalTrackingColumns(input: Dataset[Row])(implicit env: Environment): Dataset[Row] =
    if (entity.ProcessType == Historic) {
      input
        .withColumn(s"${env.SystemFieldPrefix}ValidFrom", lit(processingTime).cast(TimestampType))
        .withColumn(s"${env.SystemFieldPrefix}ValidTo", lit("2999-12-31").cast(TimestampType))
        .withColumn(s"${env.SystemFieldPrefix}IsCurrent", lit(true).cast("Boolean"))
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
  private def renameColumns(input: Dataset[Row]): Dataset[Row] = {
    val columnsToRename = columns
      .filter(c => c.NewName != "")
      .map(c => (c.Name, c.NewName))

    columnsToRename.foldLeft(input) { case (tempDb, (oldName, newName)) =>
      tempDb.withColumnRenamed(oldName, newName)
    }
  }


  // check for the deleted column (source can identify deletes with this record) add if it doesn't exist
  private def addDeletedColumn(input: Dataset[Row])(implicit env: Environment): Dataset[Row] =
    if (Utils.hasColumn(input, s"${env.SystemFieldPrefix}deleted") == false) {
      input.withColumn(s"${env.SystemFieldPrefix}deleted", lit("false").cast("Boolean"))
    } else {
      input
    }

  // add lastseen date
  private def addLastSeen(input: Dataset[Row])(implicit env: Environment): Dataset[Row] = {
    val timezoneId = environment.Timezone.toZoneId
    val now = LocalDateTime.now(timezoneId)
    input.withColumn(s"${env.SystemFieldPrefix}lastSeen", to_timestamp(lit(processingTime)))
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
          logger.error(
            s"Failed to add calculated column ${column.Name} with expression ${column.Expression}.", e
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
      entity.transformations.foldLeft(input) { (df, transformation) =>
        df.selectExpr(transformation.expressions: _*)
      }
    else
      input
  }

  private def addFilenameColumn(input: Dataset[Row], filename: String)(implicit env: Environment): Dataset[Row] = {
    val filenameField = s"${env.SystemFieldPrefix}source_filename"
    if (!Utils.hasColumn(input, filenameField)) {
      input.withColumn(filenameField, lit(filename))
    } else {
      input
    }
  }

  final def WriteWatermark(watermark_values: Option[List[(Watermark, Any)]]): Unit = {
    watermark_values match {
      case Some(watermarkList) =>
        this.entity.WriteWatermark(watermarkList)
      case None => logger.info("no watermark defined")
    }
  }

  final def Process(strategy: ProcessStrategy = entity.ProcessType): Unit =
    try {
      strategy.Process(this)
      WriteWatermark(getSource.watermark_values) 
    }
    catch {
      case e:Throwable => {
        logger.error("Unhandled exception during processing", e)
        // e.printStackTrace()
        throw e
      }
    }

}
