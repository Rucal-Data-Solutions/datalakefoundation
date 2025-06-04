package unit_tests

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SQLContext, SparkSession, DataFrame, Encoder }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import datalake.metadata._
import datalake.processing._
import datalake.outputs._

import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.fs.Path
import java.sql.Timestamp

trait SparkSessionTest extends Suite with BeforeAndAfterAll with Matchers {
  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Rucal Unit Tests")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // .set("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
    .set("spark.driver.host", "localhost")

  lazy val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val testBasePath = java.nio.file.Files.createTempDirectory("dlf_tempdir").toString

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testBasePath))
    super.afterAll()
  }
}

class DatalakeJsonMetadataTest extends AnyFunSuite with SparkSessionTest {

  test("Generate ADF Json") {
    try {
      val metadatasettings = new JsonMetadataSettings()
      val user_dir = System.getProperty("user.dir")

      metadatasettings.initialize(f"${user_dir}/src/test/scala/example/metadata.json")

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

class SparkEnvironmentTests extends AnyFunSuite with SparkSessionTest {

  test("Spark Version") {
    import spark.implicits._ // Now spark is a stable var, so we import implicits inside the test

    info(s"spark version: ${spark.version}")
    assert(spark.version === "3.5.2", s"spark version: ${spark.version}")

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

class ProcessingTests extends AnyFunSuite with SparkSessionTest {

  test("Historic processing should maintain temporal integrity") {
    import spark.implicits._

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) {
      bronzeFolder.mkdirs()
    }

    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) {
      silverFolder.mkdirs()
    }

    val initialData = Seq((1, "John", "Data1")).toDF("id", "name", "data")
    val updatedData = Seq((1, "John", "Data2")).toDF("id", "name", "data")

    val processingTimeOption = "2025-05-05T12:00:00"

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

    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val testEntity = metadata.getEntity(1)
    val paths = testEntity.getPaths

    val initialSlice = "initial_slice.parquet"
    val updatedSlice = "updated_slice.parquet"
    initialData.write.parquet(s"${paths.bronzepath}/$initialSlice")
    updatedData.write.parquet(s"${paths.bronzepath}/$updatedSlice")

    val proc1 = new Processing(testEntity, initialSlice, Map("processing.time" -> processingTimeOption))
    proc1.Process()

    val newTime = java.time.LocalDateTime
      .parse(processingTimeOption, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
      .plusMinutes(1)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

    val proc2 = new Processing(testEntity, updatedSlice, Map("processing.time" -> newTime))
    proc2.Process()

    val result_df = spark.read.format("delta").load(paths.silverpath).orderBy("ValidFrom")
    val result = result_df.collect()

    result_df.show(false)

    assert(result.length == 2, "Should have two records after update")
    assert(
      result(0).getAs[Timestamp]("ValidTo") === result(1).getAs[Timestamp]("ValidFrom"),
      "ValidTo of first record should equal ValidFrom of second record"
    )
    assert(result(0).getAs[Boolean]("IsCurrent") === false, "First record should not be current")
    assert(result(1).getAs[Boolean]("IsCurrent") === true, "Second record should be current")
    assert(result(0).getAs[Timestamp]("ValidFrom") === Timestamp.valueOf("2025-05-05 12:00:00"), "ValidFrom of the first record should match the processing.time option")
    assert(result(1).getAs[Timestamp]("ValidFrom") === Timestamp.valueOf("2025-05-05 12:01:00"), "ValidFrom of the second record should be 1 minute after the processing.time option")

  }

  test("System field prefix should reflect the prefix in the environment") {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.commons.io.FileUtils

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) { bronzeFolder.mkdirs() }
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) { silverFolder.mkdirs() }


    val randomPrefix = scala.util.Random.alphanumeric.filter(_.isLetter).take(3).mkString.toLowerCase + "_"
    val df = Seq((1, "John", "Data1")).toDF("id", "name", "data")

    val env = new Environment(
      "DEBUG (OVERRIDE)",
      testBasePath.replace("\\", "/"),
      "Europe/Amsterdam",
      "/${connection}/${entity}",
      "/${connection}/${entity}",
      "/${connection}/${destination}",
      "-secure",
      systemfield_prefix = randomPrefix
    )

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(f"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val testEntity = metadata.getEntity(1)
    val inMemoryDataFile = "inmemory_data.parquet"
    df.write.mode("overwrite").parquet(s"${testEntity.getPaths.bronzepath}/$inMemoryDataFile")


    org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testEntity.getPaths.silverpath))
    val proc = new Processing(testEntity, inMemoryDataFile)
    proc.Process()

    val silverDf = spark.read.format("delta").load(testEntity.getPaths.silverpath)

    val expectedColumns = Seq(
      "id",
      "name",
      "data",
      "PK_testentity",
      randomPrefix + "SourceHash",
      randomPrefix + "ValidFrom",
      randomPrefix + "ValidTo",
      randomPrefix + "IsCurrent",
      randomPrefix + "deleted",
      randomPrefix + "lastSeen"
    )

    expectedColumns.foreach { colName =>
      assert(silverDf.columns.contains(colName), s"Expected column: '$colName'")
    }

  }

  test("Check Watermark") {
    import spark.implicits._

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) { bronzeFolder.mkdirs() }
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) { silverFolder.mkdirs() }

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
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val testEntity = metadata.getEntity(2)
    val sliceFile = "wm_slice.parquet"
    Seq((1, 10L), (2, 5L)).toDF("ID", "SeqNr")
      .write.mode("overwrite").parquet(s"${testEntity.getPaths.bronzepath}/$sliceFile")

    val proc = new Processing(testEntity, sliceFile)
    val src = proc.getSource

    val wmValue = src.watermark_values.get.find(_._1.Column_Name == "SeqNr").get._2
    assert(wmValue == 10L)

    val partitions = src.partition_columns.get.toMap
    assert(partitions.size == 1)
    assert(partitions("Administration") == "\"950\"")

    assert(src.source_df.count() == 2)
  }
}
