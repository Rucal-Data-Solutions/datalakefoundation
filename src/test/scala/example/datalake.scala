package datalake.metadata

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


  lazy val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val testBasePath = java.nio.file.Files.createTempDirectory("dlf_tempdir").toString

  val randomPrefix = scala.util.Random.alphanumeric.filter(_.isLetter).take(3).mkString.toLowerCase + "_"
  val override_env = new Environment(
    "DEBUG (OVERRIDE)",
    testBasePath.replace("\\", "/"),
    "Europe/Amsterdam",
    "/${connection}/${entity}",
    "/${connection}/${entity}",
    "/${connection}/${destination}",
    "-secure",
    systemfield_prefix = randomPrefix,
    output_method = "paths"
  )

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.streams.active.foreach(_.stop())  // Stop any active streams
        spark.stop()
        spark.sparkContext.stop()
      }
      SparkSession.clearActiveSession()  // Clear the active session
      SparkSession.clearDefaultSession() // Clear the default session
      org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testBasePath))
    } finally {
      super.afterAll()
    }
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
      val adf_config = DataFactory.getConfigItems(EntityGroup("avw"))

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

  test("Full Load Processing") {
    import spark.implicits._
    import org.apache.commons.io.FileUtils

    // Set up test directories
    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) bronzeFolder.mkdirs()
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) silverFolder.mkdirs()

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(1)
    val paths = testEntity.getPaths

    val (bronzePath, silverPath) = (paths.bronzepath, paths.silverpath)

    // Create test data
    val testData = (1 to 10000)
      .map(i => (i, s"Name_$i", s"Data_$i"))
      .toDF("id", "name", "data")

    val fullSlice = "full_load.parquet"
    testData.write.mode("overwrite").parquet(s"$bronzePath/$fullSlice")

    // Process full load
    val proc = new Processing(testEntity, fullSlice)
    proc.Process(Full)

    // Verify results
    val result = spark.read.format("delta").load(silverPath)

    // Check row count
    assert(result.count() === 10000, "Full load should process all records")

    // Check schema and data integrity
    assert(result.columns.contains(s"${randomPrefix}ValidFrom"), "Should have ValidFrom timestamp")
    assert(result.columns.contains(s"${randomPrefix}ValidTo"), "Should have ValidTo timestamp")
    assert(result.columns.contains(s"${randomPrefix}IsCurrent"), "Should have IsCurrent flag")

    // Sample validation
    val sampleRow = result.filter($"id" === 1).first()
    assert(sampleRow.getAs[String]("name") === "Name_1")
    assert(sampleRow.getAs[String]("data") === "Data_1")
    assert(sampleRow.getAs[Boolean](s"${randomPrefix}IsCurrent") === true)
  }

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

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")

    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
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

    val result_df = spark.read.format("delta").load(paths.silverpath).orderBy(s"${randomPrefix}ValidFrom")
    val result = result_df.collect()

    result_df.show(false)

    assert(result.length == 2, "Should have two records after update")
    assert(
      result(0).getAs[Timestamp](s"${randomPrefix}ValidTo") === result(1).getAs[Timestamp](s"${randomPrefix}ValidFrom"),
      "ValidTo of first record should equal ValidFrom of second record"
    )
    assert(result(0).getAs[Boolean](s"${randomPrefix}IsCurrent") === false, "First record should not be current")
    assert(result(1).getAs[Boolean](s"${randomPrefix}IsCurrent") === true, "Second record should be current")
    assert(result(0).getAs[Timestamp](s"${randomPrefix}ValidFrom") === Timestamp.valueOf("2025-05-05 12:00:00"), "ValidFrom of the first record should match the processing.time option")
    assert(result(1).getAs[Timestamp](s"${randomPrefix}ValidFrom") === Timestamp.valueOf("2025-05-05 12:01:00"), "ValidFrom of the second record should be 1 minute after the processing.time option")

  }

  test("System field prefix") {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.commons.io.FileUtils

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) { bronzeFolder.mkdirs() }
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) { silverFolder.mkdirs() }



    val df = Seq((1, "John", "Data1")).toDF("id", "name", "data")


    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(f"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
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
      "-secure",
      systemfield_prefix = randomPrefix,
      output_method = "paths"
    )

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val testEntity = metadata.getEntity(2)
    val om = testEntity.OutputMethod 


    val sliceFile = "wm_slice.parquet"
    Seq((1, 10L), (2, 5L)).toDF("ID", "SeqNr")
      .write.mode("overwrite").parquet(s"${om.asInstanceOf[Output].bronze.asInstanceOf[PathLocation].path}/$sliceFile")

    val proc = new Processing(testEntity, sliceFile)
    proc.Process()

    // val wmValue = src.watermark_values.get.find(_._1.Column_Name == "SeqNr").get._2
    // wmValue shouldBe 10L

    // val partitions = src.partition_columns.get.toMap

    // partitions.size shouldBe 1
    // partitions("Administration") shouldBe "\"950\""
    // src.source_df.count() shouldBe 2
    val seqWM = testEntity.Watermark.find(wm => wm.Column_Name == "SeqNr").get.Value.getOrElse("0")
    seqWM shouldBe "'10'"
  }

  test("Processing should handle blank slices appropriately") {
    import spark.implicits._
    import org.apache.commons.io.FileUtils

    // Set up test directories
    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) bronzeFolder.mkdirs()
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) silverFolder.mkdirs()

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(1)
    val paths = testEntity.getPaths

    // Create and write a blank slice
    val emptyData = Seq.empty[(Int, String, String)].toDF("id", "name", "data")
    val blankSlice = "blank_slice.parquet"
    emptyData.write.parquet(s"${paths.bronzepath}/$blankSlice")

    // Test 1: Processing a blank slice as first run
    val proc1 = new Processing(testEntity, blankSlice)
    proc1.Process(Full)

    // Verify no data was written
    val result1 = spark.read.format("delta").load(paths.silverpath)
    assert(result1.count() === 0, "No data should be written when processing a blank slice as first run")

    // Test 2: Processing a blank slice with existing data
    val existingData = Seq((1, "John", "Data1")).toDF("id", "name", "data")
    val dataSlice = "data_slice.parquet"
    existingData.write.parquet(s"${paths.bronzepath}/$dataSlice")

    // First process the data slice
    val procData = new Processing(testEntity, dataSlice)
    procData.Process(Full)

    // Then process the blank slice
    val proc2 = new Processing(testEntity, blankSlice)
    proc2.Process(Merge)

    // Verify existing data was preserved
    val result2 = spark.read.format("delta").load(paths.silverpath)
    assert(result2.count() === 1, "Existing data should be preserved when processing a blank slice")
    val row = result2.collect()(0)
    assert(row.getAs[Int]("id") === 1, "Existing data should remain unchanged")
    assert(row.getAs[String]("name") === "John", "Existing data should remain unchanged")
    assert(row.getAs[String]("data") === "Data1", "Existing data should remain unchanged")
  }
}
