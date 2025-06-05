package datalake.metadata

import datalake.core._
import datalake.core.Utils._
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
import org.apache.logging.log4j.{LogManager, Logger, Level}

import org.json4s.CustomSerializer
import org.json4s.jackson.JsonMethods.{ render, parse }
import org.json4s.jackson.Serialization.{ read, write }
import org.json4s.JsonAST.{ JField, JObject, JInt, JNull, JValue, JString, JBool }

case class Paths(rawpath: String, bronzepath: String, silverpath: String) extends Serializable


class Entity(
    metadata: datalake.metadata.Metadata,
    id: Int,
    name: String,
    group: Option[String],
    destination: Option[String],
    enabled: Boolean,
    secure: Option[Boolean],
    connection: String,
    processtype: String,
    watermark: List[Watermark],
    columns: List[EntityColumn],
    val settings: JObject,
    val transformations: List[EntityTransformation]
) extends Serializable {
  implicit val environment: Environment = metadata.getEnvironment
  @transient 
  implicit val logger: Logger = LogManager.getLogger(this.getClass)

  private val resolved_paths: Paths = parsePaths

  override def toString(): String =
    s"(${this.id}) - ${this.name}"

  final def Id: Int =
    this.id

  final def Name: String =
    this.name.toLowerCase()

  final def Group: String =
    this.group.getOrElse("").toLowerCase()

  /** Get the destination name for this entity
    * @return String containing the destination name.
    */
  final def Destination: String ={
    this.destination.getOrElse(this.name).toLowerCase()
  }

  final def isEnabled(): Boolean =
    this.enabled & this.Connection.isEnabled

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

  final def Columns(column_filter: EntityColumnFilter): List[EntityColumn]=
    this.columns.filter(c => c == column_filter)

  final def Watermark: List[Watermark] =
    this.watermark

  final def ProcessType: ProcessStrategy =
    this.processtype.toLowerCase match {
      case Full.Name  => Full
      case Merge.Name => Merge
      case Historic.Name => Historic
      case "delta" => Merge //allow old delta for backwards compatibility
      case _ => throw ProcessStrategyNotSupportedException(
          s"Process Type ${this.processtype} not supported"
        )
    }

  final def Settings: Map[String, Any] = {
    val mergedSettings = this.Connection.settings merge this.settings
    mergedSettings.values
  }

  final def getPaths: Paths = resolved_paths

  private def parsePaths: Paths = {
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
      bronzePath ++= environment.SecureContainerSuffix
      silverPath ++= environment.SecureContainerSuffix
    }

    // environment override for raw
    _settings.get("raw_path") match {
      case Some(value:  String) => rawPath ++= value.normalized_path
      case _ =>
        rawPath ++= environment.RawPath.normalized_path
    }

    // environment override for bronze
    _settings.get("bronze_path") match {
      case Some(value: String) => bronzePath ++= value.normalized_path
      case _ =>
        bronzePath ++= environment.BronzePath.normalized_path
    }

    // environment override for silver
    _settings.get("silver_path") match {
      case Some(value: String) => silverPath ++= value.normalized_path
      case _ =>
        silverPath ++= environment.SilverPath.normalized_path
    }

    // // interpret variables
    val settingsVars = _settings.map(s => Try(LiteralEvalParameter(s"settings_${s._1}", s._2.toString())).getOrElse(LiteralEvalParameter(s"settings_${s._1}", "Invalid_Config_Value")) ).toSeq
    val availableVars = Seq(LiteralEvalParameter("today", today), LiteralEvalParameter("entity", this.Name), LiteralEvalParameter("destination", this.Destination), LiteralEvalParameter("connection", _connection.Name))
    val expr = new Expressions(settingsVars ++ availableVars)
    val retRawPath = expr.EvaluateExpression(rawPath.toString)
    val retBronzePath = expr.EvaluateExpression(bronzePath.toString)
    val retSilverPath = expr.EvaluateExpression(silverPath.toString)

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
      .map(column => column.Name)
      .toList


  final def getRenamedColumns: scala.collection.Map[String, String] =
    this.columns
      .filter(c => (c.NewName.toString() != "" && c.NewName != c.Name && c.Name != ""))
      .map(c => (c.Name, c.NewName))
      .toMap

  final def WriteWatermark(watermark_values: List[(Watermark, Any)]): Unit = {
    // Write the watermark values to system table
    val watermarkData: WatermarkData = new WatermarkData(this.id)
    watermarkData.WriteWatermark(watermark_values)
  }

}

class EntitySerializer(metadata: datalake.metadata.Metadata)
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
            group = (j \ "group").extract[Option[String]],
            destination = (j \ "destination").extract[Option[String]],
            enabled = (j \ "enabled").extractOrElse[Boolean](true),
            secure = (j \ "secure").extract[Option[Boolean]],
            connection = (j \ "connection").extract[String],
            processtype = (j \ "processtype").extract[String],
            watermark = watermarkJson.extract[List[Watermark]],
            columns = (j \ "columns").extract[List[EntityColumn]],
            settings = (j \ "settings").extract[JObject],
            transformations = (j \ "transformations").extract[List[EntityTransformation]]
          )

        },
        { case entity: Entity =>
          val combinedSettings = entity.Connection.settings merge entity.settings


          JObject(
            JField("id", JInt(entity.Id)),
            JField("name", JString(entity.Name)),
            JField("group", JString(entity.Group)),
            JField("destination", JString(entity.Destination)),
            JField("enabled", JBool(entity.isEnabled())),
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
