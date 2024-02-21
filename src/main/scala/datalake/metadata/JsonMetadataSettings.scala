package datalake.metadata
import java.io.File


class JsonMetadataSettings extends DatalakeMetadataSettings {

  def initialize(filepath: String): Unit = {
    val jsonFile = new File(filepath)
    val jsonString = scala.io.Source.fromFile(jsonFile).mkString

    initialize(jsonString.asInstanceOf[ConfigString])
  }

}
