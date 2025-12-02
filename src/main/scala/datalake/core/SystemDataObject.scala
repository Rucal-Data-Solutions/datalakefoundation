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
  private implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  @transient
  private lazy val logger: Logger = DatalakeLogManager.getLogger(this.getClass, environment)

  private val isUnityCatalog: Boolean = {
    val ucFlag = spark.conf.getOption("spark.databricks.unityCatalog.enabled").exists(_.toBoolean)
    val sparkCatalog = spark.conf.getOption("spark.sql.catalog.spark_catalog").getOrElse("").toLowerCase
    ucFlag || sparkCatalog.contains("managedcatalog")
  }

  private val (ucCatalog, ucSchema) = {
    val parts = environment.RootFolder.split("\\.", 2)
    if (parts.length == 2) (Some(parts(0)), Some(parts(1))) else (None, None)
  }

  private val ucTableIdentifier = (ucCatalog, ucSchema) match {
    case (Some(cat), Some(schema)) => Some(s"`$cat`.`$schema`.`${table_definition.Name}`")
    case _                         => None
  }

  val deltaTablePath = s"${environment.RootFolder}/system/${table_definition.Name}"
  val partition = table_definition.Columns.filter(c => c.partOfPartition == true).map(c => c.name)
  val schema = table_definition.Schema

  private def ensureUnityCatalogTable(): Unit = {
    ucTableIdentifier.foreach { tableId =>
      // Create schema/catalog if needed before first write
      (ucCatalog, ucSchema) match {
        case (Some(cat), Some(schemaName)) =>
          spark.sql(s"CREATE CATALOG IF NOT EXISTS `$cat`")
          spark.sql(s"CREATE SCHEMA IF NOT EXISTS `$cat`.`$schemaName`")
        case _ => // Not enough info to create schema
      }

      if (!spark.catalog.tableExists(tableId.replace("`", ""))) {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          .write
          .format("delta")
          .partitionBy(partition: _*)
          .mode("overwrite")
          .saveAsTable(tableId)
      }
    }
  }

  final def Append(rows: Seq[Row]): Unit = {
    val append_df = spark.createDataFrame(rows.asJava, schema)

    if (isUnityCatalog && ucTableIdentifier.isDefined) {
      ensureUnityCatalogTable()
      append_df.write.format("delta").partitionBy(partition: _*).mode("append").saveAsTable(ucTableIdentifier.get)
    }
    else {
      append_df.write.format("delta").partitionBy(partition: _*).mode("append").save(deltaTablePath)
    }
  }

  final def Append(row: Row): Unit =
    this.Append(Seq(row))

  final def getDataFrame: Option[DataFrame] =
    try {
      if (isUnityCatalog && ucTableIdentifier.isDefined) {
        ensureUnityCatalogTable()
        Some(spark.table(ucTableIdentifier.get))
      }
      else {
        Some(spark.read.format("delta").load(deltaTablePath))
      }
    }
    catch {
      case NonFatal(e) =>
        val target = if (isUnityCatalog && ucTableIdentifier.isDefined) ucTableIdentifier.get else deltaTablePath
        logger.error(s"Error reading delta table at $target", e)
        None
    }

}
