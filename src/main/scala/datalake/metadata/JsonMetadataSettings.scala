package datalake.metadata

import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Source

class JsonMetadataSettings extends DatalakeMetadataSettings {

  def initialize(filepath: String): Unit = {
    val path = new Path(filepath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val stream = fs.open(path)
    val jsonString = try Source.fromInputStream(stream).mkString finally stream.close()

    super.initialize(jsonString.asInstanceOf[ConfigString])
  }

}
