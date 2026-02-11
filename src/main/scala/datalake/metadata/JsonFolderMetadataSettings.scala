package datalake.metadata

import scala.io.Source
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

class JsonFolderMetadataSettings extends DatalakeMetadataSettings {
    implicit val formats: Formats = DefaultFormats

    def initialize(directoryPath: String): Unit = {
        val mergedJson = mergeJsonFiles(directoryPath)
        val jsonString = compact(render(mergedJson))
        super.initialize(jsonString.asInstanceOf[ConfigString])
    }

    def mergeJsonFiles(directoryPath: String): JValue = {
        val dir = new File(directoryPath)
        require(dir.exists && dir.isDirectory, s"$directoryPath is not a valid directory")

        val jsonFiles = dir.listFiles().filter(_.getName.endsWith(".json"))
        val jsonContents = jsonFiles.map { file =>
            val source = Source.fromFile(file)
            val content = try source.mkString finally source.close()
            parse(content)
        }

        // Merge all JSON objects
        jsonContents.foldLeft(JObject(Nil)) { (acc, json) =>
            acc.merge(json.asInstanceOf[JObject])
        }
    }
}