import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, SparkSession, DataFrame, Encoder }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

import datalake.metadata._
import datalake.processing._
import datalake.outputs._

import org.json4s.jackson.JsonMethods._

class DatalakeJsonMetadataTest extends AnyFunSuite with BeforeAndAfterAll  {
  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Rucal DatalakeMetadataTest")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Rucal SimpleSparkTest")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

test("Generate ADF Json") {
   try {
     val metadatasettings = new JsonMetadataSettings()
     val user_dir = System.getProperty("user.dir")

     metadatasettings.initialize(f"${user_dir}\\src\\test\\scala\\example\\metadata.json")

     implicit val metadata = new Metadata(metadatasettings)
     println(metadata.getEnvironment.Name)

     val connection = metadata.getConnectionByName("AdventureWorksSql")
     val adf_config = DataFactory.getConfigItems(EntityGroup("aVw"))

     
     println(pretty(parse(adf_config)))

   } catch {
     case e: Exception =>
       fail(s"Test failed with exception: ${e.getMessage}")
   }
 }

}


class SparkEnvironmentTests extends AnyFunSuite with BeforeAndAfterAll {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Rucal Unit Tests")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Spark Version") {
    import spark.implicits._  // Now spark is a stable var, so we import implicits inside the test

    info(s"spark version: ${spark.version}")
    assert(spark.version === "3.5.1", s"spark version: ${spark.version}")

    // spark.conf.getAll.foreach(println)

  }

  test("simple range test") {
    import spark.implicits._

    try {
      val df = spark.range(0, 10, 1)
      assert(df.count() === 10)
    } catch {
      case e: Exception =>
        fail(s"Test failed with exception: ${e.getMessage}")
    }
  }
}
