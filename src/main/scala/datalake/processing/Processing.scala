package datalake.processing

import org.apache.logging.log4j.{Logger, Level}

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
// import datalake.log._

import org.apache.logging.log4j.LogManager
import datalake.log.DatalakeLogManager


case class DatalakeSource(
    source_df: DataFrame,
    watermark_values: Option[Array[(Watermark, Any)]],
    partition_columns: Option[List[(String, Any)]],
    current_watermark_values: Option[Array[(Watermark, Any)]]
)
case class DuplicateBusinesskeyException(message: String) extends DatalakeException(message, Level.ERROR)

// Bronze(Source) -> Silver(Target)
class Processing(private val entity: Entity, sliceFile: String, options: Map[String, String] = Map.empty) extends Serializable {
  implicit val environment: datalake.metadata.Environment = entity.Environment
  
  // def getOutputMethod: Output = entity.getOutput
  
  private val columns = entity.Columns

  final val entity_id = entity.Id
  final val primaryKeyColumnName: String = s"PK_${entity.Destination}"

  final val ioLocations = entity.getOutput
  final val watermarkColumns = entity.Watermark
  final val entitySettings = entity.Settings
  
  private val inferMissingDeletes: Boolean = entity.Settings
    .get("delete_missing")
    .flatMap(v => Try(v.toString.toBoolean).toOption)
    .getOrElse(false)

  // Memoization for getSource to prevent creating multiple cached DataFrames
  private var _cachedSource: Option[DatalakeSource] = None

  // final lazy val sliceFileFullPath: String = s"${paths.bronzepath}/${sliceFile}"
  final lazy val destination: OutputLocation = ioLocations.silver

  final lazy val processingTime: String = {
    val now = LocalDateTime.now(environment.Timezone.toZoneId).toString

    options.get("processing.time") match {
      case Some(value) =>
        Try(LocalDateTime.parse(value)) match {
          case Success(_) =>
            logger.debug(s"Using $value for processing time.")
            value
          case Failure(_) =>
            logger.error(s"Invalid processing.time value: $value - falling back to current time.")
            now
        }
      case None => now
    }
  }

  private implicit val spark: SparkSession =
    SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  @transient 
  private lazy val logger = DatalakeLogManager.getLogger(this.getClass, environment)

  def inferDeletesFromMissing: Boolean = inferMissingDeletes

  def getSource: DatalakeSource = {
    _cachedSource.getOrElse {
      logger.debug(s"getSource called by ${Thread.currentThread().getName} - computing source")

      val dfSlice = ioLocations.bronze match {
        case PathLocation(path) => spark.read.parquet(s"$path/$sliceFile")
        case TableLocation(table) => spark.read.table(table)
      }

      // Combine all transformations into a single chain before any actions
      val transformedDF = dfSlice
        .transform(injectTransformations)
        .transform(addCalculatedColumns)
        .transform(calculateSourceHash)
        .transform(addTemporalTrackingColumns)
        .transform(addFilenameColumn(_, sliceFile))
        .transform(addPrimaryKey)
        .transform(castColumns)
        .transform(renameColumns)
        .transform(addDeletedColumn)
        .transform(addLastSeen)
        .datalakeNormalize()
        .cache() // Cache the DataFrame since it will be used multiple times

      // Now trigger actions after all transformations are done
      if (transformedDF.isEmpty) {
        logger.warn("Slice contains no data (RowCount=0)")
      }

      val new_watermark_values = getWatermarkValues(transformedDF, watermarkColumns)
      val current_watermark_values = getCurrentWatermarkValues(watermarkColumns)
      val part_values = getPartitionValues(transformedDF)

      val source = new DatalakeSource(transformedDF, new_watermark_values, part_values, current_watermark_values)
      _cachedSource = Some(source)
      source
    }
  }

  private def getWatermarkValues(slice: DataFrame, wm_columns: Array[Watermark]): Option[Array[(Watermark, Any)]] = {
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

  private def getCurrentWatermarkValues(wm_columns: Array[Watermark]): Option[Array[(Watermark, Any)]] = {
    if (wm_columns.nonEmpty) {
      val values: Array[(Watermark, Any)] = wm_columns.flatMap(wm => wm.Value.map(v => (wm, v.asInstanceOf[Any])))
      if (values.nonEmpty) Some(values) else None
    } else None
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
      val nonSystemColumns = {
        // If prefix is empty, filter out known system column names
        val systemColumnNames = Set(s"${env.SystemFieldPrefix}source_filename", s"${env.SystemFieldPrefix}metadata")
        input.columns.filterNot(systemColumnNames.contains)
      }
      input.withColumn(
        hashfield,
        sha2(concat_ws("", nonSystemColumns.map(c => col("`" + c + "`").cast("string")): _*), 256)
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
        
        if(!duplicates.isEmpty) {
          duplicates.show(truncate = false)

          val error_msg = f"${duplicates.count()} duplicate key(s) (according to the businesskey) found in slice, can't continue."
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
    val inputWithFilename = if (!Utils.hasColumn(input, filenameField)) {
      logger.warn(
        s"Bronze table is missing column '$filenameField' for slice filtering. " +
        s"Adding column with value '$filename'."
      )
      input.withColumn(filenameField, lit(filename))
    } else {
      input
    }

    ioLocations.bronze match {
      case TableLocation(_) => inputWithFilename.filter(col(filenameField) === sliceFile)
      case _ => inputWithFilename
    }
  }

  final def WriteWatermark(watermark_values: Option[Array[(Watermark, Any)]]): Unit = {
    watermark_values match {
      case Some(watermarkArray) =>
        this.entity.WriteWatermark(watermarkArray)
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
    finally {
      // Clean up cached DataFrame to free memory
      _cachedSource.foreach { source =>
        logger.debug("Unpersisting cached DataFrame")
        source.source_df.unpersist()
      }
      _cachedSource = None
    }

}
