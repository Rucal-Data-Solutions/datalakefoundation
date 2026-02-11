package datalake.metadata

import datalake.core._
import datalake.core.implicits._
import datalake.processing._

import scala.util.Try

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.{Logger, Level, LogManager}



abstract class DatalakeMetadataSettings extends Serializable {
  private var _metadata: Metadata = _

  private var _isInitialized: Boolean = false

  private var _connections: List[JValue] = _
  private var _entities: List[JValue] = _
  private var _entitiesById: Map[Int, JValue] = _
  private var _environment_settings: JValue = _

  implicit val spark: SparkSession =
    SparkSession.builder().getOrCreate()
  import spark.implicits._

  @transient 
  lazy final val logger: Logger = LogManager.getLogger(this.getClass())

  // def initialize(initParameter: initParam)
  type ConfigString
  def initialize(jsonConfig: ConfigString): Unit = {
    implicit var formats: Formats = DefaultFormats
    val _json = jsonConfig.toString()

    logger.info("Parsing datalake config")

    // Parse the JSON string using safer extraction pattern
    val json = parse(_json)
    logger.debug(pretty(json))

    _connections = (json \ "connections").extract[List[JValue]]
    _entities = (json \ "entities").extract[List[JValue]]

    // Build cached map for efficient lookups
    _entitiesById = _entities.map(j => (j \ "id").extract[Int] -> j).toMap

    _environment_settings = json \ "environment"

    // Check if all entity IDs are unique using safer extraction
    val entityIds = _entities.flatMap(j => (j \ "id").extractOpt[Int])
    if (entityIds.size != entityIds.toSet.size) {
      throw new DatalakeException("Duplicate EntityIDs found in JSON.", Level.ERROR)
    }

    _isInitialized = true
  }

  // def isInitialized: Boolean
  final def isInitialized(): Boolean =
    _isInitialized

  final def setMetadata(metadata: datalake.metadata.Metadata) : Unit = {
    _metadata = metadata
  }
  
  final def getEntitiesById(id: Int): Option[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata) + new EntityTransformationSerializer(_metadata)
    _entitiesById.get(id).map(_.extract[Entity])
  }

  final def getEntitiesById(ids: Array[Int]): Option[List[Entity]] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata) + new EntityTransformationSerializer(_metadata)
    val entities = ids.flatMap(_entitiesById.get).map(_.extract[Entity]).toList
    if(entities.nonEmpty) Some(entities) else None
  }

  final def getConnectionEntities(connection: Connection): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata) + new EntityTransformationSerializer(_metadata)
    _entities
      .filter(e => (e \ "connection").extract[String] == connection.Code)
      .map(j => j.extract[Entity])
  }

  final def getGroupEntities(group: EntityGroup): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata) + new EntityTransformationSerializer(_metadata)

    _entities
      .filter(e => (e \ "group").toOption.exists(_.extract[String].equalsIgnoreCase(group.Name)))
      .map(j => j.extract[Entity])
  }

  final def getConnectionGroupEntities(connectionGroup: EntityConnectionGroup): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata) + new EntityTransformationSerializer(_metadata)

    _entities
      .filter(e => (e \ "connectiongroup").toOption.exists(_.extract[String].equalsIgnoreCase(connectionGroup.Name)))
      .map(j => j.extract[Entity])
  }

  final def getConnection(connectionCode: String): Option[Connection] = {
    implicit var formats: Formats =
      DefaultFormats + new ConnectionSerializer(_metadata)
    val connection =
      _connections
        .find(j => (j \ "name").extract[String] == connectionCode)
        .map(j => j.extract[Connection])
    connection
  }

  final def getConnectionByName(connectionName: String): Option[Connection] = {
    implicit var formats: Formats =
      DefaultFormats + new ConnectionSerializer(_metadata)
    val connection =
      _connections
        .find(j => (j \ "name").extract[String].toLowerCase() == connectionName.toLowerCase())
        .map(j => j.extract[Connection])
    connection
  }

  final def getEnvironment(): Environment = {
    implicit var formats: Formats = DefaultFormats
    val environment_settings =
      _environment_settings.extract[Environment]
    environment_settings
  }
}