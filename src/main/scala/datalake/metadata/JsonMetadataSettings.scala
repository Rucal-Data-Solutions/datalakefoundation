package datalake.metadata

import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.reflect.runtime.universe._
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JObject
import java.awt

class EntityDeserializer(metadata: Metadata)
    extends CustomSerializer[Entity](implicit formats =>
      (
        { case j: JObject =>
          new Entity(
            metadata = metadata,
            id = (j \ "id").extract[Int],
            name = (j \ "name").extract[String].toLowerCase(),
            enabled = (j \ "enabled").extract[Boolean],
            secure = (j \ "secure").extract[Option[Boolean]],
            connection = (j \ "connection").extract[String],
            processtype = (j \ "processtype").extract[String].toLowerCase(),
            columns = (j \ "columns").extract[List[EntityColumn]],
            settings = (j \ "settings").extract[JArray]
          )
        },
        { case _: Entity =>
          JObject()
        }
      )
    )

class ConnectionDeserializer(metadata: Metadata, entities: List[Entity])
    extends CustomSerializer[Connection](implicit formats =>
      (
        { case j: JObject =>
          new Connection(
            metadata = metadata,
            code = (j \ "name").extract[String],
            name = (j \ "name").extract[String].toLowerCase(),
            enabled = (j \ "enabled").extract[Option[Boolean]],
            settings = (j \ "settings").extract[Map[String, Any]],
            entities = entities
          )
        },
        { case _: Connection =>
          JObject()
        }
      )
    )

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
    implicit var formats: Formats = DefaultFormats + new EntityDeserializer(_metadata)
    val entity = _entities.find(j => (j \ "id").extract[Int] == id).map(j => j.extract[Entity])
    entity
  }

  private def getEntities(connectionName: String): List[Entity] = {
    implicit var formats: Formats = DefaultFormats + new EntityDeserializer(_metadata)
    _entities
      .filter(e => (e \ "connection").extract[String] == connectionName)
      .map(j => j.extract[Entity])
  }

  def getConnection(connectionCode: String): Option[Connection] = {
    val _entities = getEntities(connectionCode)
    implicit var formats: Formats =
      DefaultFormats + new ConnectionDeserializer(_metadata, _entities)
    val connection =
      _connections.find(j => (j \ "name").extract[String] == connectionCode).map(j => j.extract[Connection])
    connection
  }

  def getConnectionByName(connectionName: String): Option[Connection] = {
    val _entities = getEntities(connectionName)
    implicit var formats: Formats =
      DefaultFormats + new ConnectionDeserializer(_metadata, _entities)
    val connection =
      _connections.find(j => (j \ "name").extract[String] == connectionName).map(j => j.extract[Connection])
    connection
  }

  def getEnvironment(): Environment = {
    implicit var formats: Formats = DefaultFormats
    val enviroment_settings =
      _environment_settings.extract[Environment]
    enviroment_settings
  }

}
