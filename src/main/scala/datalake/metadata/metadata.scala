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

// import org.apache.arrow.flatbuf.Bool
// import org.json4s._
// import org.json4s.jackson.JsonMethods._

trait DatalakeMetadataSettings {
  type initParam
  def initialize(initParameter: initParam)
  def isInitialized: Boolean
  def setMetadata(metadata: Metadata): Unit
  def getEntity(id: Int): Option[Entity]
  def getConnection(connectionCode: String): Option[Connection]
  def getEnvironment: Environment
}
case class Paths(BronzePath: String, SilverPath: String) extends Serializable

case class MetadataNotInitializedException(message: String) extends Exception(message)
case class EntityNotFoundException(message: String) extends Exception(message)
case class ConnectionNotFoundException(message: String) extends Exception(message)
case class ProcessStrategyNotSupportedException(message: String) extends Exception(message)

class Connection(
    metadata: Metadata,
    code: String,
    name: String,
    enabled: Option[Boolean],
    settings: Map[String, Any],
    entities: List[Entity]
) {

  override def toString(): String =
    s"Connection_name:${this.name}"

  def Name: String =
    this.name

  def isEnabled: Boolean =
    this.enabled.getOrElse(true)

  def getSettings: Map[String, Any] =
    this.settings

  def getSettingAs[T](name: String): T = {
    val setting = this.settings.get(name)

    setting match {
      case Some(value) => value.asInstanceOf[T]
      case None        => None.asInstanceOf[T]
    }
  }
  def getEntities: List[Entity] = {
    this.entities
  } 
}


class Environment(
  name: String,
  root_folder: String,
  timezone: String
){
    def Name: String =
    this.name

    def RootFolder: String =
      this.root_folder

    def Timezone: TimeZone = 
      TimeZone.getTimeZone(timezone)

}

class Entity(
    metadata: Metadata,
    id: Int,
    name: String,
    enabled: Boolean,
    secure: Option[Boolean],
    connection: String,
    processtype: String,
    columns: List[EntityColumn],
    settings: JsonAST.JArray
) extends Serializable {

  override def toString(): String =
    s"Entity: (${this.id}) - ${this.name}"

  def Id: Int =
    this.id

  def Name: String =
    this.name

  def isEnabled(): Boolean =
    this.enabled

  def Secure: Boolean =
    this.secure.getOrElse(false)

  def Connection: Connection =
    metadata.getConnection(this.connection)

  def Environment:Environment =
    metadata.getEnvironment

  def Columns: List[EntityColumn] =
    this.columns

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

  def getPaths(): Paths = {
    val env: Environment = metadata.getEnvironment
    val today =
      java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))

    val _settings = this.Settings
    val _connection = this.Connection
    val _securehandling = this.Secure

    val root_folder: String = env.RootFolder
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

  def getBusinessKey(): Array[String] =
    this.columns
      .filter(c => c.FieldRole == "businesskey")
      .map(column => column.Name())
      .toArray

  def getRenamedColumns: scala.collection.Map[String, String] =
    this.columns
      .filter(c => c.NewName != "")
      .map(c => (c.Name, c.NewName))
      .toMap
}

class EntityColumn(
    name: String,
    newname: Option[String],
    datatype: String,
    fieldrole: String
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

  def FieldRole: String =
    this.fieldrole
}

class Metadata(metadataSettings: DatalakeMetadataSettings) extends Serializable {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  if (!metadataSettings.isInitialized) {
    throw new MetadataNotInitializedException("Config is not initialized")
  }

  metadataSettings.setMetadata(this)

  def getEntity(id: Int): Entity = {
    val entity = metadataSettings.getEntity(id)
    entity match {
      case Some(entity) => entity
      case None         => throw EntityNotFoundException("Entity not found")
    }
  }

  def getConnection(connectionName: String): Connection = {
    val connection = metadataSettings.getConnection(connectionName)
    connection match {
      case Some(connection) => connection
      case None             => throw ConnectionNotFoundException("Connection not found")
    }
  }

  def getEnvironment: Environment ={
    val environment = metadataSettings.getEnvironment
    environment
  }

}
