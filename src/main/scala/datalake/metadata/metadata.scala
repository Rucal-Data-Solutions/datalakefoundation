package datalake.metadata

import datalake.core._
import datalake.processing._

import java.util.TimeZone

import org.apache.logging.log4j.{Logger, Level, LogManager}
import datalake.log.DatalakeLogManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._


case class MetadataNotInitializedException(message: String) extends DatalakeException(message, Level.ERROR)
case class EntityNotFoundException(message: String) extends DatalakeException(message, Level.ERROR)
case class ConnectionNotFoundException(message: String) extends DatalakeException(message, Level.ERROR)
case class ProcessStrategyNotSupportedException(message: String) extends DatalakeException(message, Level.ERROR)

class Metadata(metadataSettings: DatalakeMetadataSettings, env: Environment) extends Serializable {
  
  def this(metadataSettings: DatalakeMetadataSettings) ={
    this(metadataSettings, metadataSettings.getEnvironment())
  }

  private implicit val spark: SparkSession =
    SparkSession.builder().getOrCreate()
  import spark.implicits._

  metadataSettings.setMetadata(this)
  implicit val environment: Environment = env

  @transient 
  lazy private val logger: Logger = DatalakeLogManager.getLogger(this.getClass, environment)

  if (!metadataSettings.isInitialized()) {
    val e = new MetadataNotInitializedException("Config is not initialized")
    logger.error(e.getMessage, e)
    throw e
  }
  else {
    logger.info("Datalake metadata class Initialized.")
  }

  def getEntity(id: Int): Entity = {
    val entity = metadataSettings.getEntitiesById(id)
    entity match {
      case Some(entity) => entity
      case None         => throw EntityNotFoundException(s"Entity (${id}) not found")
    }
  }

  def getEntities(connection: Connection): List[Entity] = {
    logger.debug("Get getEntities(Connection)")
    metadataSettings.getConnectionEntities(connection)
  }

  def getEntities(group: datalake.metadata.EntityGroup): List[Entity] = {
    metadataSettings.getGroupEntities(group)
  }

  def getEntities(connectionGroup: datalake.metadata.EntityConnectionGroup): List[Entity] = {
    metadataSettings.getConnectionGroupEntities(connectionGroup)
  }

  def getEntities(entityId: Int): List[Entity] = {
    metadataSettings.getEntitiesById(entityId) match{
      case Some(value) => List(value)
      case None => List.empty[Entity]
    }
  }

  def getEntities(entityIds: Array[Int]): List[Entity] = {
    metadataSettings.getEntitiesById(entityIds) match{
      case Some(value) => value
      case None => List.empty[Entity]
    }
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
    environment
  }


}
