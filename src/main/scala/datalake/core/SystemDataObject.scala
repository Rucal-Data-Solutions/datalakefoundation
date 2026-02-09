package datalake.core

import datalake.metadata._

import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import org.apache.logging.log4j.Logger
import datalake.log.DatalakeLogManager

import io.delta.tables._
import java.sql.Timestamp


class SystemDataTableDefinition(name: String, schema: List[DatalakeColumn]) extends Serializable {

  final def Name: String =
    this.name

  final def Columns: List[DatalakeColumn] =
    this.schema

  final def Schema: StructType =
    StructType(
      schema.map(f => StructField(f.name, f.dataType, f.nullable))
    )
}

class SystemDataObject(table_definition: SystemDataTableDefinition)(implicit
    environment: Environment
) extends Serializable {
  private implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  @transient
  private lazy val logger: Logger = DatalakeLogManager.getLogger(this.getClass, environment)

  private val unityCatalogEnabled: Boolean =
    spark.conf.getOption("spark.databricks.unityCatalog.enabled").exists(_.toBoolean)

  private val useCatalogSystemTable: Boolean =
    environment.OutputMethod.equalsIgnoreCase("output") && unityCatalogEnabled

  private val ucNamespace: String =
    spark.conf.getOption("spark.databricks.defaultCatalog") match {
      case Some(cat) if cat.nonEmpty => s"$cat.system"
      case _                         => "system"
    }

  val deltaTablePath = s"${environment.RootFolder}/system/${table_definition.Name}"
  private val catalogTableName = s"$ucNamespace.${table_definition.Name}"
  val partition = table_definition.Columns.filter(c => c.partOfPartition == true).map(c => c.name)
  val schema = table_definition.Schema

  final def Append(rows: Seq[Row]): Unit = {
    // val data = spark.sparkContext.parallelize(rows)
    val append_df = spark.createDataFrame(rows.asJava, schema)

    val writer = append_df.write.format("delta").partitionBy(partition: _*).mode("append")

    if (useCatalogSystemTable) {
      writer.saveAsTable(catalogTableName)
    } else {
      writer.save(deltaTablePath)
    }
  }

  final def Append(row: Row): Unit =
    this.Append(Seq(row))

  final def getDataFrame: Option[DataFrame] =
    try
      Some(
        if (useCatalogSystemTable)
          spark.table(catalogTableName)
        else
          spark.read.format("delta").load(deltaTablePath)
      )
    catch {
      case e: org.apache.spark.sql.AnalysisException =>
        val target = if (useCatalogSystemTable) s"catalog table $catalogTableName" else deltaTablePath
        logger.debug(s"System table not found at $target (expected on first run)")
        None
      case NonFatal(e) =>
        val target = if (useCatalogSystemTable) s"catalog table $catalogTableName" else s"delta table at $deltaTablePath"
        logger.error(s"Error reading $target", e)
        None
    }

}
