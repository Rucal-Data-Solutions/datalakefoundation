package datalake.metadata

import datalake.core._
import datalake.processing._

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


case class MetadataNotInitializedException(message: String) extends Exception(message)
case class EntityNotFoundException(message: String) extends Exception(message)
case class ConnectionNotFoundException(message: String) extends Exception(message)
case class ProcessStrategyNotSupportedException(message: String) extends Exception(message)

class EntityGroup(name: String) extends Serializable {
  override def toString(): String = this.name.toLowerCase()
  def Name: String = this.toString()
}
object EntityGroup{
  def apply(name: String): EntityGroup ={
    new EntityGroup(name)
  }
}

class Metadata(metadataSettings: DatalakeMetadataSettings) extends Serializable {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  if (!metadataSettings.isInitialized) {
    throw new MetadataNotInitializedException("Config is not initialized")
  }

  metadataSettings.setMetadata(this)
  implicit val environment: Environment = metadataSettings.getEnvironment

  def getEntity(id: Int): Entity = {
    val entity = metadataSettings.getEntity(id)
    entity match {
      case Some(entity) => entity
      case None         => throw EntityNotFoundException(s"Entity (${id}) not found")
    }
  }

  def getEntities(connection: Connection): List[Entity] = {
    metadataSettings.getConnectionEntities(connection)
  }

    def getEntities(group: EntityGroup): List[Entity] = {
      metadataSettings.getGroupEntities(group)
    }

  def getConnectionEntities(connection: Connection): List[Entity] = {
    getEntities(connection)
  }

  def getConnection(connectionCode: String): Connection = {
    val connection = metadataSettings.getConnection(connectionCode)
    connection match {
      case Some(connection) => connection
      case None             => throw ConnectionNotFoundException(s"ConnectionId (${connectionCode}) not found")
    }
  }

  def getConnectionByName(connectionName: String): Connection = {
    val connection = metadataSettings.getConnectionByName(connectionName)
    connection match {
      case Some(connection) => connection
      case None             => throw ConnectionNotFoundException(s"ConnectionName (${connectionName}) not found")
    }
  }

  def getEnvironment: Environment ={
    val environment = metadataSettings.getEnvironment
    environment
  }


}
