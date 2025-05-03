package unit_tests


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
import org.apache.hadoop.fs.Path
import java.sql.Timestamp

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

class HistoricProcessingTests extends AnyFunSuite with BeforeAndAfterAll {
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


  test("Historic processing should maintain temporal integrity") {
    import spark.implicits._
    
    // Create test directories
    val testBasePath = java.nio.file.Files.createTempDirectory("historic_test").toString
    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) {
      bronzeFolder.mkdirs()
    }

    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) {
      silverFolder.mkdirs()
    }
    
    // Create initial and updated test data
    val initialData = Seq(
      (1, "John", "Data1")
    ).toDF("id", "name", "data")
    
    val updatedData = Seq(
      (1, "John", "Data2")
    ).toDF("id", "name", "data")

    try {
      // Create test environment
      val env = new Environment(
        "DEBUG (OVERRIDE)",
        testBasePath.replace("\\", "/"),
        "Europe/Amsterdam",
        "/${connection}/${entity}",
        "/${connection}/${entity}",
        "/${connection}/${destination}",
        "-secure"
      )


      val settings = new JsonMetadataSettings()
      val user_dir = System.getProperty("user.dir")
      settings.initialize(f"${user_dir}\\src\\test\\scala\\example\\metadata.json")
      
      val metadata = new Metadata(settings, env)

      val testEntity = metadata.getEntity(1)
      val paths = testEntity.getPaths
      
      // Save test data as parquet files
      val initialSlice = "initial_slice.parquet"
      val updatedSlice = "updated_slice.parquet"
      initialData.write.parquet(s"${paths.bronzepath}/$initialSlice")
      updatedData.write.parquet(s"${paths.bronzepath}/$updatedSlice")

      // Process initial load
      val proc1 = new Processing(testEntity, initialSlice)
      proc1.Process()

      // Process update
      val proc2 = new Processing(testEntity, updatedSlice)
      proc2.Process()

      
      // Verify results
      val result_df = spark.read.format("delta").load(paths.silverpath).orderBy("ValidFrom")
      result_df.show(truncate = false)

      val result = result_df.collect()

      // // Verify temporal integrity
      assert(result.length == 2, "Should have two records after update")
      assert(result(0).getAs[Timestamp]("ValidTo") === result(1).getAs[Timestamp]("ValidFrom"), 
        "ValidTo of first record should equal ValidFrom of second record")
      assert(result(0).getAs[Boolean]("IsCurrent") === false, "First record should not be current")
      assert(result(1).getAs[Boolean]("IsCurrent") === true, "Second record should be current")
      
    } finally {
      // Cleanup
      org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testBasePath))
    }
  }
}
