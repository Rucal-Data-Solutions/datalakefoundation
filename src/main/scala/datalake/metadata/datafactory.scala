package datalake.metadata

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer.{ignore}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }

object DataFactory {

  private def watermarkFunction: PartialFunction[(String, Any), Option[(String, Any)]] = {
    case ("environment", _) => None
    case ("entity_id", _) => None
    case ("function", f) => Some(("function", f))
  }

  def getConfigItems(connection: Connection): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[Entity](
      ignore("metadata") orElse ignore("environment")
    ) + FieldSerializer[EntityColumn]() + FieldSerializer[Watermark](watermarkFunction)

    // val _settings = write(connection.getSettings)
    val enabled_entities = connection.getEntities.filter(p => p.isEnabled() == true)
    write(enabled_entities)

  }

}
