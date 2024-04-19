package datalake.metadata

import datalake.core._
import datalake.processing._

import java.util.TimeZone
import org.apache.logging.log4j.{LogManager, Logger, Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST


case class MetadataNotInitializedException(message: String)(implicit logger: Logger) extends DatalakeException(message, Level.FATAL, logger)
case class EntityNotFoundException(message: String)(implicit logger: Logger) extends DatalakeException(message, Level.ERROR, logger)
case class ConnectionNotFoundException(message: String)(implicit logger: Logger) extends DatalakeException(message, Level.ERROR, logger)
case class ProcessStrategyNotSupportedException(message: String)(implicit logger: Logger) extends DatalakeException(message, Level.FATAL, logger)

class Metadata(metadataSettings: DatalakeMetadataSettings) extends Serializable {
  private implicit val logger = LogManager.getLogger(this.getClass()) 

  if (!metadataSettings.isInitialized) {
    val e = new MetadataNotInitializedException("Config is not initialized")
    throw e
  }
  
  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

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

  def getEntities(group: datalake.metadata.EntityGroup): List[Entity] = {
    metadataSettings.getGroupEntities(group)
  }

  def getEntities(entityId: Int): List[Entity] = {
    metadataSettings.getEntity(entityId) match{
      case Some(value) => List(value)
      case None => List.empty[Entity]
    }
  }

  @deprecated("This function is deprecated. Use getEntities(connection: Connection) instead.", "0.6.8")
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
