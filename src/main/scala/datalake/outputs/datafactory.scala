package datalake.outputs

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }

import datalake.metadata._
import datalake.log.DatalakeLogManager
import org.apache.spark.sql.SparkSession

object DataFactory {
  @transient private lazy val logger = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    DatalakeLogManager.getLogger(this.getClass)
  }


  /**
   * Retrieves the configuration items for the given object.
   *
   * @param arg The object for which configuration items are to be retrieved.
   *            Accepted data types: EntityGroup, Connection, EntityId(INT)
   * @param metadata Instance of the metadata class containing information about the entities and connections.
   * @return A string representation of the enabled entities for the given object. (Ignored if only a entityId(Int) is provided)
   * @throws IllegalArgumentException if the object type is invalid.
   */
  def getConfigItems(arg: Any)(implicit metadata: Metadata): String = {
    val argDescription = arg match {
      case group: EntityGroup => s"EntityGroup(${group.Name})"
      case connection: Connection => s"Connection(${connection.Name})"
      case connectionGroup: EntityConnectionGroup => s"EntityConnectionGroup($connectionGroup)"
      case entityId: Int => s"EntityId($entityId)"
      case entities: Array[Int] => s"EntityIds(${entities.mkString(", ")})"
      case other => s"Unknown(${other.getClass.getTypeName})"
    }
    logger.info(s"getConfigItems called with: $argDescription")

    implicit val formats: Formats = DefaultFormats + FieldSerializer[EntityColumn]() + new EntitySerializer(metadata) + new WatermarkSerializer(metadata)

    val entities: List[Entity] = arg match {
      case group: EntityGroup => metadata.getEntities(group).filter(_.isEnabled())
      case connection: Connection => metadata.getEntities(connection).filter(_.isEnabled())
      case connectionGroup: EntityConnectionGroup => metadata.getEntities(connectionGroup).filter(_.isEnabled())
      case entityId: Int => metadata.getEntities(entityId)
      case entities: Array[Int] => metadata.getEntities(entities)
      case _ => throw new Exception(s"Invalid parameter type ${arg.getClass().getTypeName()}")
    }

    write(entities)
  }

}