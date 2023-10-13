package datalake.metadata

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }
import scala.reflect.ClassTag

object DataFactory {


  def getConfigItems(connection: Connection)(implicit metadata: Metadata): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[Entity](
      ignore("metadata") orElse ignore("environment")
    ) + FieldSerializer[EntityColumn]() + new WatermarkSerializer(metadata)

    // val _settings = write(connection.getSettings)
    val enabled_entities = connection.getEntities.filter(p => p.isEnabled() == true)
    write(enabled_entities)

  }

}
