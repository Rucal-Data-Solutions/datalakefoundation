package datalake.metadata

import datalake.core._
import datalake.core.implicits._
import datalake.processing._
import scala.util.Try

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.logging.log4j.{LogManager, Level}
import org.apache.logging.log4j.core.LoggerContext

abstract class DatalakeMetadataSettings {
  private var _metadata: Metadata = _

  private var _isInitialized: Boolean = false

  private var _connections: List[JValue] = _
  private var _entities: List[JValue] = _
  private var _environment_settings: JValue = _


  val selector = new org.apache.logging.log4j.core.async.AsyncLoggerContextSelector();
  val factory = new org.apache.logging.log4j.core.impl.Log4jContextFactory(selector);
  val context = factory.getContext(this.getClass().getName(), null,null, true);

  implicit val logger = context.getLogger("DatalakeMetadataSettings");


  // def initialize(initParameter: initParam)
  type ConfigString
  def initialize(jsonConfig: ConfigString): Unit = {
    implicit var formats: Formats = DefaultFormats
    val _json = jsonConfig.toString()

    logger.info("Parsing datalake config")

    // Parse the JSON string
    val json = parse(_json)
    logger.debug(pretty(json))

    _connections = (json \ "connections").extract[List[JValue]]
    _entities = (json \ "entities").extract[List[JValue]]
    _environment_settings = json \ "environment"

    // Check if all entity IDs are unique
    val entityIds = _entities.map(j => (j \ "id").extract[Int])
    if (entityIds.size != entityIds.toSet.size) {
      throw new DatalakeException("Duplicate EntityIDs found in JSON.", Level.ERROR, logger)
    }

    _isInitialized = true
  }

  // def isInitialized: Boolean
  final def isInitialized(): Boolean =
    _isInitialized

  final def setMetadata(metadata: Metadata) {
    _metadata = metadata
  }
  
  final def getEntity(id: Int): Option[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)
    val entity = _entities.find(j => (j \ "id").extract[Int] == id).map(j => j.extract[Entity])
    entity
  }

  final def getConnectionEntities(connection: Connection): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)
    _entities
      .filter(e => (e \ "connection").extract[String] == connection.Code)
      .map(j => j.extract[Entity])
  }

  final def getGroupEntities(group: EntityGroup): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)

    _entities
      .filter(e => (e \ "group").toOption.exists(_.extract[String].equalsIgnoreCase(group.Name)))
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
    val enviroment_settings =
      _environment_settings.extract[Environment]
    enviroment_settings
  }
}