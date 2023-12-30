package datalake.metadata

import datalake.core._
import datalake.processing._
import scala.util.Try

trait DatalakeMetadataSettings {
  type initParam
  def initialize(initParameter: initParam)
  def isInitialized: Boolean
  def setMetadata(metadata: Metadata): Unit
  def getEntity(id: Int): Option[Entity]
  def getConnectionEntities(connection: Connection): List[Entity]
  def getConnection(connectionCode: String): Option[Connection]
  def getConnectionByName(connectionName: String): Option[Connection]
  def getGroupEntities(group: EntityGroup): List[Entity]
  def getEnvironment: Environment
}