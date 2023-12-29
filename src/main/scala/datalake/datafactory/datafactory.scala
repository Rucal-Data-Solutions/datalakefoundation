package datalake.datafactory

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }
import scala.reflect.ClassTag

import datalake.core._
import datalake.metadata._
import org.apache.commons.lang.NotImplementedException

object DataFactory {


  def getConfigItems(obj: Any)(implicit metadata: Metadata): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[EntityColumn]() + new EntitySerializer(metadata) + new WatermarkSerializer(metadata)

    val entities = obj match {
      case group: EntityGroup => metadata.getEntities(group)
      case connection: Connection => metadata.getEntities(connection)
      case _ => throw new NotImplementedException("Invalid type.")
    }

    val enabledEntities = entities.filter(_.isEnabled)
    write(enabledEntities)
  }

}