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
      case None         => throw EntityNotFoundException(s"Entity (${id}) not found")
    }
  }

  def getConnection(connectionName: String): Connection = {
    val connection = metadataSettings.getConnection(connectionName)
    connection match {
      case Some(connection) => connection
      case None             => throw ConnectionNotFoundException(s"Connection (${connectionName}) not found")
    }
  }

  def getEnvironment: Environment ={
    val environment = metadataSettings.getEnvironment
    environment
  }

}
