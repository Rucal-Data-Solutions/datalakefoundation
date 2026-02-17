package datalake.metadata

import datalake.core._
import datalake.processing._
import datalake.log.DatalakeLogManager
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
    fieldroles: Seq[String],
    expression: Option[String]
) {

  override def toString(): String =
    this.Name

  override def equals(obj: Any): Boolean =
    obj match {
      case ec: EntityColumn => this.hashCode() == ec.hashCode()
      case filter: EntityColumnFilter =>
        filter.fieldrole.exists(fr => this.fieldroles.contains(fr)) | filter.HasExpression.exists(
          x => this.expression.exists(e => e.isEmpty() != x)
        )
      case _ => false
    }

  final def Name: String =
    if (this.name.isEmpty) NewName else this.name

  final def NewName: String =
    this.newname.getOrElse("")

  final def DataType(implicit
      environment: Environment,
      spark: SparkSession
  ): Option[DataType] = {
    @transient lazy val logger =
      DatalakeLogManager.getLogger(this.getClass, environment)

    this.datatype match {
      case Some(value) =>
        val split_datatype = value.split("""[\(\),]+""")
        val base_type = split_datatype(0)

        val _datatype = base_type match {
          case "string"    => StringType
          case "integer"   => IntegerType
          case "long"      => LongType
          case "date"      => DateType
          case "timestamp" => TimestampType
          case "float"     => FloatType
          case "double"    => DoubleType
          case "boolean"   => BooleanType
          case "decimal" =>
            if (split_datatype.length >= 3)
              DecimalType(split_datatype(1).toInt, split_datatype(2).toInt)
            else {
              logger.warn(
                "Decimal type without precision and scale in column definition. Using default DecimalType(38, 18)."
              )
              DecimalType(38, 18)
            }
          case unknown =>
            logger.warn(
              s"Unsupported type in column definition (${unknown}) casting using StringType."
            )
            StringType
        }
        Some(_datatype)
      case None => None
    }
  }

  final def FieldRoles: Seq[String] =
    this.fieldroles

  final def Expression: String =
    this.expression.getOrElse("")
}
