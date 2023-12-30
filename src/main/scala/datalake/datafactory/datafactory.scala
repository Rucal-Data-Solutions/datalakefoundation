package datalake.datafactory

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }
import scala.reflect.ClassTag

import datalake.core._
import datalake.metadata._

object DataFactory {


  /**
   * Retrieves the configuration items for the given object.
   *
   * @param obj The object for which configuration items are to be retrieved.
   *            Accepted data types: EntityGroup, Connection.
   * @param metadata The metadata containing information about the entities and connections.
   * @return A string representation of the enabled entities for the given object.
   * @throws IllegalArgumentException if the object type is invalid.
   */
  def getConfigItems(obj: Any)(implicit metadata: Metadata): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[EntityColumn]() + new EntitySerializer(metadata) + new WatermarkSerializer(metadata)

    val entities = obj match {
      case group: EntityGroup => metadata.getEntities(group)
      case connection: Connection => metadata.getEntities(connection)
      case _ => throw new IllegalArgumentException("Invalid type.")
    }

    val enabledEntities = entities.filter(_.isEnabled)
    write(enabledEntities)
  }

}