package datalake.core

import datalake.metadata._

import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types.{ StructType, DataType, StructField }
import org.apache.spark.sql.functions.{col}

import io.delta.tables._
import java.sql.Timestamp

case class SystemDataColumn (
    name: String,
    data_type: DataType,
    nullable: Boolean,
    part_of_partition: Boolean
)

class SystemDataTable_Definition(name: String, schema: List[SystemDataColumn]) extends Serializable {

  final def Name: String =
    this.name

  final def Columns: List[SystemDataColumn] =
    this.schema

  final def Schema: StructType =
    StructType(
      schema.map(f => StructField(f.name, f.data_type, f.nullable))
    )
}

class SystemDataObject(table_definition: SystemDataTable_Definition)(implicit environment: Environment) extends Serializable {
  private val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  val deltaTablePath = s"${environment.RootFolder}/system/${table_definition.Name}"
  val partition = table_definition.Columns.filter(c => c.part_of_partition == true).map(c => c.name)

  // final def Append(rows: Seq[Row]): Unit = {
  //   val data = spark.sparkContext.parallelize(rows)
  //   val append_df = spark.createDataFrame(data, table_definition.Schema)

  //   append_df.write.format("delta").partitionBy(partition:_*).mode("append").save(deltaTablePath)
  // }
  
  final def Append(rows: Seq[Row]): Unit = {
    // Convert rows to DataFrame
    var initialDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(rows.head.schema))

    // Cast each column to its defined data type
    val castedDF = table_definition.Columns.foldLeft(initialDF) { (df, column) =>
      df.withColumn(column.name, col(column.name).cast(column.data_type))
    }

    // Write to Delta table
    castedDF.write.format("delta").partitionBy(partition: _*).mode("append").save(deltaTablePath)
  }

  final def Append(row: Row): Unit = {
    var rows = Seq(row)
    this.Append(rows)
  }

  final def getDataFrame: Option[DataFrame] =
    try {
      Some(spark.read.format("delta").load(deltaTablePath))
    }
    catch {
      case e: Throwable => None
    }

}
