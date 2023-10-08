package datalake.metadata

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read, write }
import org.json4s.FieldSerializer.{ renameTo, ignore, renameFrom }

object DataFactory {

  def watermarkFunction: PartialFunction[(String, Any), Option[(String, Any)]] = { 
    case ("environment", _) => None
    case ("function", f) => Some(("fun", f))
  }


  def getConfigItems(connection: Connection): String = {
    implicit val formats: Formats = DefaultFormats + FieldSerializer[Entity](ignore("metadata") orElse ignore("environment")) + FieldSerializer[EntityColumn]() + FieldSerializer[Watermark](watermarkFunction)

    // val _settings = write(connection.getSettings)
    val _entities = write(connection.getEntities.filter(p => p.isEnabled() == true))

    _entities

  }

}
