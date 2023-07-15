package datalake.metadata

import datalake.processing._
import datalake.utils._
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST
import scala.tools.cmd.Meta
import org.apache.arrow.flatbuf.Bool

class EntityColumn(
    name: String,
    newname: Option[String],
    datatype: String,
    fieldroles: Array[String]
) {

  override def toString(): String =
    this.name

  def Name(): String =
    this.name

  def NewName: String =
    this.newname.getOrElse("")

  def DataType: DataType = {
    val split_datatype = this.datatype.split("""[\(\),]+""")
    val base_type = split_datatype(0)

    base_type match {
      case "string"  => StringType
      case "integer" => IntegerType
      case "date"    => DateType
      case "decimal" =>
        DecimalType(split_datatype(1).toInt, split_datatype(2).toInt)
      case _ => StringType
    }
  }

  def FieldRoles: Array[String] =
    this.fieldroles
}