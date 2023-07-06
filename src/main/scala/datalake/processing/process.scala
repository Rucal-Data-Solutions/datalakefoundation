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

// Bronze(Source) -> Silver(Target)
class Processing(entity: Entity, sliceFile: String) {
  val environment = entity.Environment()
  val primaryKeyColumnName: String = s"PK_${entity.Name()}"
  val columns = entity.Columns()
  val paths = entity.getPaths
  val sliceFileFullPath: String = s"${paths.BronzePath}/${sliceFile}"
  val destination: String = paths.SilverPath

  val columnsToRename = columns
    .filter(c => c.NewName != "")
    .map(c => (c.Name, c.NewName))
    .toMap

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def getSource(): DataFrame = {

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
      val pkColumns = List(entity.getBusinessKey() map col: _*)
      dfSlice = dfSlice.withColumn(primaryKeyColumnName, sha2(concat_ws("_", pkColumns: _*), 256))
    }

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

    dfSlice
  }

  def process(stategy: ProcessStrategy = entity.ProcessType()) {
    stategy.process(this)
  }
}

trait ProcessStrategy {
  def process(processing: Processing)
}

final object Full extends ProcessStrategy {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def process(processing: Processing) {
    val source: DataFrame = processing.getSource()
    FileOperations.remove(processing.destination, true)
    source.write.format("delta").mode(SaveMode.Overwrite).save(processing.destination)
  }
}

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
