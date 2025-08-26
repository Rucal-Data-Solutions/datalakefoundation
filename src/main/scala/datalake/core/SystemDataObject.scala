package datalake.core

import datalake.metadata._

import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import scala.jdk.CollectionConverters._
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

  val deltaTablePath = s"${environment.RootFolder}/system/${table_definition.Name}"
  val partition = table_definition.Columns.filter(c => c.partOfPartition == true).map(c => c.name)
  val schema = table_definition.Schema

  final def Append(rows: Seq[Row]): Unit = {
    // val data = spark.sparkContext.parallelize(rows)
    val append_df = spark.createDataFrame(rows.asJava, schema)

    append_df.write.format("delta").partitionBy(partition: _*).mode("append").save(deltaTablePath)
  }

  final def Append(row: Row): Unit = {
    val rows = Seq(row)
    this.Append(rows)
  }

  final def getDataFrame: Option[DataFrame] =
    try
      Some(spark.read.format("delta").load(deltaTablePath))
    catch {
      case e: Throwable =>
        logger.error(s"Error reading delta table at $deltaTablePath", e)
        None
    }

}
