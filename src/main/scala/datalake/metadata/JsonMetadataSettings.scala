package datalake.metadata

import datalake.core._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.reflect.runtime.universe._
import java.io.File



class JsonMetadataSettings extends DatalakeMetadataSettings {
  private var _isInitialized: Boolean = false
  private var _connections: List[JValue] = _
  private var _entities: List[JValue] = _
  private var _metadata: Metadata = _
  private var _environment_settings: JValue = _

  def setMetadata(metadata: Metadata) {
    _metadata = metadata
  }

  type initParam = String

  def initialize(initParameter: initParam): Unit = {
    implicit var formats: Formats = DefaultFormats
    val jsonFile = new File(initParameter)
    val jsonString = scala.io.Source.fromFile(jsonFile).mkString

    // Parse the JSON string
    val json = parse(jsonString)

    _connections = (json \ "connections").extract[List[JValue]]
    _entities = (json \ "entities").extract[List[JValue]]
    _environment_settings = json \ "environment_settings"

    // Check if all entity IDs are unique
    val entityIds = _entities.map(j => (j \ "id").extract[Int])
    if (entityIds.size != entityIds.toSet.size) {
      throw new Exception("Entity IDs are not unique in the JSON file.")
    }

    _isInitialized = true
  }

  def isInitialized(): Boolean =
    _isInitialized

  def getEntity(id: Int): Option[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)
    val entity = _entities.find(j => (j \ "id").extract[Int] == id).map(j => j.extract[Entity])
    entity
  }

  def getConnectionEntities(connection: Connection): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)
    _entities
      .filter(e => (e \ "connection").extract[String] == connection.Code)
      .map(j => j.extract[Entity])
  }

  def getGroupEntities(group: EntityGroup): List[Entity] = {
    implicit var formats: Formats =
      DefaultFormats + new EntitySerializer(_metadata) + new WatermarkSerializer(_metadata)

    _entities
      .filter(e => (e \ "group").toOption.exists(_.extract[String].equalsIgnoreCase(group.Name)))
      .map(j => j.extract[Entity])
  }

  def getConnection(connectionCode: String): Option[Connection] = {
    implicit var formats: Formats =
      DefaultFormats + new ConnectionSerializer(_metadata)
    val connection =
      _connections
        .find(j => (j \ "name").extract[String] == connectionCode)
        .map(j => j.extract[Connection])
    connection
  }

  def getConnectionByName(connectionName: String): Option[Connection] = {
    implicit var formats: Formats =
      DefaultFormats + new ConnectionSerializer(_metadata)
    val connection =
      _connections
        .find(j => (j \ "name").extract[String] == connectionName)
        .map(j => j.extract[Connection])
    connection
  }

  def getEnvironment(): Environment = {
    implicit var formats: Formats = DefaultFormats
    val enviroment_settings =
      _environment_settings.extract[Environment]
    enviroment_settings
  }

}
