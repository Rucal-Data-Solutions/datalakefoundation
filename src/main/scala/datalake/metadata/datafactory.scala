package datalake.metadata

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

object DataFactory {
    def getConfigItems(connection: Connection): String ={
        implicit val formats: Formats = DefaultFormats + FieldSerializer[Entity]() + FieldSerializer[EntityColumn]()

        val _entities = connection.getEntities().filter(p => p.isEnabled() == true)

        write(_entities)

    }
}
