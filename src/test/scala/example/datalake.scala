
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SparkSession, DataFrame }

import datalake.metadata._
import datalake.processing._

// import org.apache.spark.sql._
// import org.apache.spark.sql.functions.{col}

object DatalakeTestApp {

  val spark = SparkSession
    .builder()
    .appName("Rucal Test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val metadatasettings = new JsonMetadataSettings()

    val user_dir = System.getProperty("user.dir")
    println(user_dir)

    metadatasettings.initialize(f"${user_dir}\\src\\test\\scala\\example\\metadata.json")

    implicit val metadata = new Metadata(metadatasettings)
    println(metadata.getEnvironment.Name)

    val connection = metadata.getConnectionByName("sql_test")

    val adf_config = DataFactory.getConfigItems(connection)

    println(adf_config)
  
 }

}
