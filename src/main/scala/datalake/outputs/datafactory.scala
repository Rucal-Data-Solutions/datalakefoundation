package datalake.outputs

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }

import datalake.metadata._

object DataFactory {


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
    implicit val formats: Formats = DefaultFormats + FieldSerializer[EntityColumn]() + new EntitySerializer(metadata) + new WatermarkSerializer(metadata)

    val entities: List[Entity] = arg match {
      case group: EntityGroup => metadata.getEntities(group).filter(_.isEnabled())
      case connection: Connection => metadata.getEntities(connection).filter(_.isEnabled())
      case entityId: Int => metadata.getEntities(entityId)
      case entities: Array[Int] => metadata.getEntities(entities)
      case _ => throw new Exception(s"Invalid parameter type ${arg.getClass().getTypeName()}")
    }

    write(entities)
  }

}