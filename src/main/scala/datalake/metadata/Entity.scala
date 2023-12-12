package datalake.metadata

import datalake.core._
import datalake.processing._

import java.util.TimeZone
import java.time.LocalDateTime
import scala.util.Try
import scala.reflect.runtime._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._

import org.json4s.CustomSerializer
import org.json4s.jackson.JsonMethods.{ render, parse }
import org.json4s.jackson.Serialization.{ read, write }
import org.json4s.JsonAST.{ JField, JObject, JInt, JNull, JValue, JString, JBool }

case class Paths(RawPath: String, BronzePath: String, SilverPath: String) extends Serializable

class Entity(
    metadata: Metadata,
    id: Int,
    name: String,
    destination: Option[String],
    enabled: Boolean,
    secure: Option[Boolean],
    connection: String,
    processtype: String,
    watermark: List[Watermark],
    columns: List[EntityColumn],
    val settings: JObject
) extends Serializable {

  implicit val environment: Environment = metadata.getEnvironment

  override def toString(): String =
    s"Entity: (${this.id}) - ${this.name}"

  def Id: Int =
    this.id

  def Name: String =
    this.name.toLowerCase()

  /** Get the destination name for this entity
    * @return String containing the destination name.
    */
  def Destination: String ={
    this.destination.getOrElse(this.name).toLowerCase()
  }

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

  def Columns(fieldrole: String*): List[EntityColumn] =
    this.columns
      .filter(c => fieldrole.exists(fr => c.FieldRoles.contains(fr)))

  def Watermark: List[Watermark] =
    this.watermark

  def ProcessType: ProcessStrategy =
    this.processtype.toLowerCase match {
      case Full.Name  => Full
      case Delta.Name => Delta
      case _ => throw ProcessStrategyNotSupportedException(
          s"Process Type ${this.processtype} not supported"
        )
    }

  def Settings: Map[String, Any] = {
    val mergedSettings = this.Connection.settings merge this.settings
    mergedSettings.values
  }


  def getPaths: Paths = {
    val today =
      java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))

    val _settings = this.Settings
    val _connection = this.Connection
    val _securehandling = this.Secure

    val root_folder: String = environment.RootFolder
    val defaultPath = "/${connection}/${entity}"
    val rawPath = new StringBuilder(s"$root_folder/raw")
    val bronzePath = new StringBuilder(s"$root_folder/bronze")
    val silverPath = new StringBuilder(s"$root_folder/silver")

    if (_securehandling) {
      bronzePath ++= "-secure"
      silverPath ++= "-secure"
    }

    // rawPath ++= s"/${_connection.Name}"
    // bronzePath ++= s"/${_connection.Name}"
    // silverPath ++= s"/${_connection.Name}"

    // overrides for bronze
    _settings.get("rawpath") match {
      case Some(value) => rawPath ++= s"/$value"
      case None =>
        println("DEBUG: no rawpath in entity settings")
        bronzePath ++= defaultPath
    }

    // overrides for bronze
    _settings.get("bronzepath") match {
      case Some(value) => bronzePath ++= s"/$value"
      case None =>
        println("DEBUG: no bronzepath in entity settings")
        bronzePath ++= defaultPath
    }

    // overrides for silver
    _settings.get("silverpath") match {
      case Some(value) => silverPath ++= s"/$value"
      case None =>
        println("DEBUG: no silverpath in entity settings")
        silverPath ++= defaultPath
    }

    // // interpret variables
    val availableVars = Map("today" -> today, "entity" -> this.Destination, "connection" -> _connection.Name)
    val retRawPath = Utils.EvaluateText(rawPath.toString, availableVars)
    val retBronzePath = Utils.EvaluateText(bronzePath.toString, availableVars)
    val retSilverPath = Utils.EvaluateText(silverPath.toString, availableVars)

    return Paths(retRawPath, retBronzePath, retSilverPath)
  }

  def getBusinessKey: Array[String] =
    this
      .Columns("businesskey")
      .map(column => column.Name)
      .toArray

  def getRenamedColumns: scala.collection.Map[String, String] =
    this.columns
      .filter(c => c.NewName != "")
      .map(c => (c.Name, c.NewName))
      .toMap

  def WriteWatermark(watermark_values: List[(String, Any)]): Unit = {
    // Write the watermark values to system table
    val watermarkData: WatermarkData = new WatermarkData
    val timezoneId = environment.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

    if(watermark_values.size > 0) {
      val data = watermark_values.map(wm => Row(this.id, wm._1, timestamp_now, wm._2.toString()))
      watermarkData.Append(data.toSeq)
    }

  }

}

class EntitySerializer(metadata: Metadata)
    extends CustomSerializer[Entity](implicit formats =>
      (
        { case j: JObject =>
          val entity_id = (j \ "id").extract[Int]
          val watermarkJson = (j \ "watermark") map {
            case JObject(fields) => JObject(("entity_id", JInt(entity_id)) :: fields)
            case other           => other
          }

          new Entity(
            metadata = metadata,
            id = entity_id,
            name = (j \ "name").extract[String],
            destination = (j \ "destination").extract[Option[String]],
            enabled = (j \ "enabled").extract[Boolean],
            secure = (j \ "secure").extract[Option[Boolean]],
            connection = (j \ "connection").extract[String],
            processtype = (j \ "processtype").extract[String],
            watermark = watermarkJson.extract[List[Watermark]],
            columns = (j \ "columns").extract[List[EntityColumn]],
            settings = (j \ "settings").extract[JObject]
          )

        },
        { case entity: Entity =>
          val combinedSettings = entity.Connection.settings merge entity.settings

          JObject(
            JField("id", JInt(entity.Id)),
            JField("name", JString(entity.Name)),
            JField("destination", JString(entity.Destination)),
            JField("enabled", JBool(entity.isEnabled)),
            JField("connection", JString(entity.Connection.Code)),
            JField("connection_name", JString(entity.Connection.Name)),
            JField("processtype", JString(entity.ProcessType.Name)),
            JField("watermark", parse(write(entity.Watermark))),
            JField("columns", parse(write(entity.Columns))),
            JField("settings", combinedSettings)
          )
        }
      )
    )
