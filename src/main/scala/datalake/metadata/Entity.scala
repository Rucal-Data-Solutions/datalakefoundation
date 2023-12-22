package datalake.metadata

import datalake.core._
import datalake.processing._
import datalake.core.implicits._

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

  final def Id: Int =
    this.id

  final def Name: String =
    this.name.toLowerCase()

  /** Get the destination name for this entity
    * @return String containing the destination name.
    */
  final def Destination: String ={
    this.destination.getOrElse(this.name).toLowerCase()
  }

  final def isEnabled(): Boolean =
    this.enabled

  final def Secure: Boolean =
    this.secure.getOrElse(false)

  final def Connection: Connection =
    metadata.getConnection(this.connection)

  final def Environment: Environment =
    metadata.getEnvironment

  final def Columns: List[EntityColumn] =
    this.columns

  /**
   * Filters the columns of the entity based on the specified field roles.
   *
   * @param fieldrole The field role or array of fieldrole to filter the columns by.
   * @return A list of EntityColumn objects that match the specified field roles.
   */
  final def Columns(fieldrole: String*): List[EntityColumn] =
    this.columns
      .filter(c => fieldrole.exists(fr => c.FieldRoles.contains(fr)))

  final def Watermark: List[Watermark] =
    this.watermark

  final def ProcessType: ProcessStrategy =
    this.processtype.toLowerCase match {
      case Full.Name  => Full
      case Delta.Name => Delta
      case _ => throw ProcessStrategyNotSupportedException(
          s"Process Type ${this.processtype} not supported"
        )
    }

  final def Settings: Map[String, Any] = {
    val mergedSettings = this.Connection.settings merge this.settings
    mergedSettings.values
  }


  /**
   * Retrieves the paths associated with the entity.
   *
   * @return The paths associated with the entity.
   */
  final def getPaths: Paths = {
    val today =
      java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))

    val _settings = this.Settings
    val _connection = this.Connection
    val _securehandling = this.Secure

    val root_folder: String = environment.RootFolder
    val rawPath = new StringBuilder(s"$root_folder/raw")
    val bronzePath = new StringBuilder(s"$root_folder/bronze")
    val silverPath = new StringBuilder(s"$root_folder/silver")

    if (_securehandling) {
      bronzePath ++= "-secure"
      silverPath ++= "-secure"
    }

    // overrides for raw
    _settings.get("rawpath") match {
      case Some(value:  String) => rawPath ++= value.normalized_path
      case _ =>
        rawPath ++= environment.RawPath.normalized_path
    }

    // overrides for bronze
    _settings.get("bronzepath") match {
      case Some(value: String) => bronzePath ++= value.normalized_path
      case _ =>
        bronzePath ++= environment.BronzePath.normalized_path
    }

    // overrides for silver
    _settings.get("silverpath") match {
      case Some(value: String) => silverPath ++= value.normalized_path
      case _ =>
        silverPath ++= environment.SilverPath.normalized_path
    }

    // // interpret variables
    val availableVars = Map("today" -> today, "entity" -> this.Name, "destination" -> this.Destination, "connection" -> _connection.Name)
    val retRawPath = Utils.EvaluateText(rawPath.toString, availableVars)
    val retBronzePath = Utils.EvaluateText(bronzePath.toString, availableVars)
    val retSilverPath = Utils.EvaluateText(silverPath.toString, availableVars)

    return Paths(retRawPath, retBronzePath, retSilverPath)
  }

  /**
   * Retrieves the business key of the entity.
   *
   * @return A list of strings representing the business key columns.
   */
  final def getBusinessKey: List[String] =
    this
      .Columns("businesskey")
      .map(column => column.Name)

      
  /**
   * Retrieves the list of partition columns for this entity.
   *
   * @return The list of partition column names.
   */
  final def getPartitionColumns: List[String] =
    this
      .Columns("partition")
      .map(column => column.NewName)
      .toList


  /**
   * Returns a map of column names to rename.
   *
   * The method filters the columns based on the conditions:
   * - The column's `NewName` is not empty.
   * - The column's `NewName` is different from its `Name`.
   * - The column's `Name` is not empty.
   *
   * It then maps the filtered columns to a key-value pair, where the key is the column's `Name`
   * and the value is the column's `NewName`. Finally, it converts the mapped pairs to a map.
   *
   * @return A map of renamed columns, where the key is the original column name and the value is the new column name.
   */
  final def getRenamedColumns: scala.collection.Map[String, String] =
    this.columns
      .filter(c => (c.NewName.toString() != "" && c.NewName != c.Name && c.Name != ""))
      .map(c => (c.Name, c.NewName))
      .toMap

  final def WriteWatermark(watermark_values: List[(String, Any)]): Unit = {
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
            JField("settings", combinedSettings),
            JField("paths", parse(write(entity.getPaths)))
          )
        }
      )
    )
