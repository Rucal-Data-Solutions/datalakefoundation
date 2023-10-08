package datalake.metadata

import datalake.processing._
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST
import org.apache.commons.lang.NotImplementedException
import datalake.processing.ProcessStrategy
import datalake.core.Utils

class Entity(
    metadata: Metadata,
    id: Int,
    name: String,
    enabled: Boolean,
    secure: Option[Boolean],
    connection: String,
    processtype: String,
    watermark: List[Watermark],
    columns: List[EntityColumn],
    settings: JsonAST.JArray
) extends Serializable {

  implicit val environment:Environment = metadata.getEnvironment

  override def toString(): String =
    s"Entity: (${this.id}) - ${this.name}"

  def Id: Int =
    this.id

  def Name: String =
    this.name.toLowerCase()

  def isEnabled(): Boolean =
    this.enabled

  def Secure: Boolean =
    this.secure.getOrElse(false)

  def Connection: Connection =
    metadata.getConnection(this.connection)

  def Environment: Environment =
    metadata.getEnvironment

  def Columns: List[EntityColumn] =
    this.columns

  def Watermark: List[Watermark] =
    this.watermark

  def ProcessType: ProcessStrategy =
    this.processtype.toLowerCase match {
      case "full"  => Full
      case "delta" => Delta
      case _ => throw ProcessStrategyNotSupportedException(
          s"Process Type ${this.processtype} not supported"
        )
    }

  def Settings: JsonAST.JArray =
    this.settings

  def getSchema: StructType =
    StructType(
      this.columns.map(row => StructField(row.Name, row.DataType, true))
    )

  def getPaths: Paths = {
    val today =
      java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))

    val _settings = this.Settings
    val _connection = this.Connection
    val _securehandling = this.Secure

    val root_folder: String = environment.RootFolder
    val bronzePath = new StringBuilder(s"$root_folder/bronze")
    val silverPath = new StringBuilder(s"$root_folder/silver")

    if (_securehandling) {
      bronzePath ++= "-secure"
      silverPath ++= "-secure"
    }

    bronzePath ++= s"/${_connection.Name}"
    silverPath ++= s"/${_connection.Name}"

    // overrides for bronze
    _connection.getSettings.get("bronzepath") match {
      case Some(value) => bronzePath ++= s"/$value"
      case None =>
        println("no bronzepath in connection")
        _connection.getSettings.get("bronzepath") match {
          case Some(value) => bronzePath ++= s"/$value"
          case None =>
            println("no bronzepath in entity settings")
            bronzePath ++= s"/${this.Name}"
        }
    }

    // overrides for silver
    _connection.getSettings.get("silverpath") match {
      case Some(value) => silverPath ++= s"/$value"
      case None =>
        println("no silverpath in connection")
        _connection.getSettings.get("silverpath") match {
          case Some(value) => silverPath ++= s"/$value"
          case None =>
            println("no silverpath in entity settings")
            silverPath ++= s"/${this.Name}"
        }
    }

    // // interpret variables
    val availableVars = Map("today" -> today, "entity" -> this.Name)
    val retBronzePath = Utils.EvaluateText(bronzePath.toString, availableVars)
    val retSilverPath = Utils.EvaluateText(silverPath.toString, availableVars)

    return Paths(retBronzePath, retSilverPath)
  }

  def getBusinessKey: Array[String] = {
    this.columns
      .filter(c => c.FieldRoles.contains("businesskey"))
      .map(column => column.Name)
      .toArray
  }

  def getRenamedColumns: scala.collection.Map[String, String] =
    this.columns
      .filter(c => c.NewName != "")
      .map(c => (c.Name, c.NewName))
      .toMap

  def getWatermark: Unit =
    throw new NotImplementedException

}
