package datalake.metadata

import scala.io.Source
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s._
import org.json4s.jackson.JsonMethods._

class JsonFolderMetadataSettings extends DatalakeMetadataSettings {
    implicit val formats: Formats = DefaultFormats

    def initialize(directoryPath: String): Unit = {
        val mergedJson = mergeJsonFiles(directoryPath)
        val jsonString = compact(render(mergedJson))
        super.initialize(jsonString.asInstanceOf[ConfigString])
    }

    def mergeJsonFiles(directoryPath: String): JValue = {
        val dirPath = new Path(directoryPath)
        val fs = dirPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        require(fs.exists(dirPath) && fs.isDirectory(dirPath), s"$directoryPath is not a valid directory")

        val jsonFiles = fs.listStatus(dirPath).filter(_.getPath.getName.endsWith(".json"))
        val jsonContents = jsonFiles.map { fileStatus =>
            val stream = fs.open(fileStatus.getPath)
            val content = try Source.fromInputStream(stream).mkString finally stream.close()
            parse(content)
        }

        // Merge all JSON objects
        jsonContents.foldLeft(JObject(Nil)) { (acc, json) =>
            acc.merge(json.asInstanceOf[JObject])
        }
    }
}
