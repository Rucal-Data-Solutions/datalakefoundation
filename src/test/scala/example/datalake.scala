import datalake.datafactory.DataFactory

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SparkSession, DataFrame }

import datalake.metadata._
import datalake.processing._
import datalake.datafactory._

import org.json4s.jackson.JsonMethods._

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

    val connection = metadata.getConnectionByName("exact")

    // val adf_config = DataFactory.getConfigItems(connection)
    // println(pretty(parse(adf_config)))
    

    val entity = metadata.getEntity(2)
    println(entity)

    val proc = new Processing(entity, "test.parquet")
    proc.Process()

 }

}
