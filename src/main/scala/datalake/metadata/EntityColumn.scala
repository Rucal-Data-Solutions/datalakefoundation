package datalake.metadata

import datalake.core._
import datalake.processing._
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST


class EntityColumn(
    name: String,
    newname: Option[String],
    datatype: Option[String],
    fieldroles: Array[String],
    expression: Option[String]
) {

  override def toString(): String =
    this.name

  def Name: String =
    this.name

  def NewName: String =
    this.newname.getOrElse("")

  def DataType: Option[DataType] = {
    this.datatype match {
      case Some(value) => {
        val split_datatype = value.split("""[\(\),]+""")
        val base_type = split_datatype(0)

        val _datatype = base_type match {
          case "string"     => StringType
          case "integer"    => IntegerType
          case "date"       => DateType
          case "timestamp"  => TimestampType
          case "float"      => FloatType
          case "double"     => DoubleType
          case "boolean"    => BooleanType
          case "decimal" =>
            DecimalType(split_datatype(1).toInt, split_datatype(2).toInt)
          case unknown => {
            println(s"Warning, unsupported type in column definition (${unknown}) casting using StringType.")
            StringType
          }
        }
        Some(_datatype)
      }
      case None => None
    }
    

  }

  def FieldRoles: Array[String] =
    this.fieldroles

  def Expression: String =
    this.expression.getOrElse("")
}