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

trait SparkSessionTest extends Suite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
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
    secure_container_suffix = Some("-secure"),
    systemfield_prefix = Some(randomPrefix),
    output_method = "paths"
  )

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    // Clean up any existing test data before each test
    cleanupTestData()
    super.beforeEach()
  }

  protected def cleanupTestData(): Unit = {
    import org.apache.commons.io.FileUtils
    
    try {
      // Clean up file system paths
      val silverFolder = new java.io.File(s"$testBasePath/silver")
      if (silverFolder.exists()) {
        FileUtils.cleanDirectory(silverFolder)
      }
      
      // Clean up any test tables in the metastore
      // Get list of databases that might contain test tables
      val testDatabases = spark.sql("SHOW DATABASES").collect()
        .map(_.getString(0))
        .filter(db => db.startsWith("test_") || db.startsWith("silver_"))
      
      testDatabases.foreach { dbName =>
        try {
          val tables = spark.sql(s"SHOW TABLES IN `$dbName`").collect()
          tables.foreach { tableRow =>
            val tableName = tableRow.getString(1)
            try {
              spark.sql(s"DROP TABLE IF EXISTS `$dbName`.`$tableName`")
            } catch {
              case _: Exception => // Ignore individual table cleanup errors
            }
          }
        } catch {
          case _: Exception => // Ignore if database doesn't exist or can't be accessed
        }
      }
    } catch {
      case _: Exception => // Ignore cleanup errors - tests should still run
    }
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
    val ioLocations = testEntity.OutputMethod

    val (bronzePath, silverPath) = (paths.bronzepath, paths.silverpath)

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"full_load_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Create test data with test identifier
    val testData = (1 to 10000)
      .map(i => (i, s"Name_$i", s"Data_$i", testId))
      .toDF("id", "name", "data", "test_id")

    val fullSlice = s"full_load_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"$bronzePath/$fullSlice")

    // Process full load
    val proc = new Processing(testEntity, fullSlice)
    proc.Process(Full)

    // Verify results - filter by test_id to avoid interference from other tests
    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

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
    import org.apache.commons.io.FileUtils

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) {
      bronzeFolder.mkdirs()
    }

    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) {
      silverFolder.mkdirs()
    }

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"historic_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val initialData = Seq((1, "John", "Data1", testId)).toDF("id", "name", "data", "test_id")
    val updatedData = Seq((1, "John", "Data2", testId)).toDF("id", "name", "data", "test_id")

    val processingTimeOption = "2025-05-05T12:00:00"

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")

    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(1)
    val paths = testEntity.getPaths
    val ioLocations = testEntity.OutputMethod

    val initialSlice = s"initial_slice_${testId}.parquet"
    val updatedSlice = s"updated_slice_${testId}.parquet"
    initialData.write.parquet(s"${paths.bronzepath}/$initialSlice")
    updatedData.write.parquet(s"${paths.bronzepath}/$updatedSlice")

    // First processing - should create the delta table (first run diverts to Full)
    val proc1 = new Processing(testEntity, initialSlice, Map("processing.time" -> processingTimeOption))
    proc1.Process(Historic) // Explicitly specify Historic strategy

    // Check what's in the table after first load
    val afterFirstLoad = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    println("=== After First Load ===")
    afterFirstLoad.filter($"test_id" === testId).show(false)

    val newTime = java.time.LocalDateTime
      .parse(processingTimeOption, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
      .plusMinutes(1)
      .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))

    // Second processing - should now use Historic merge logic
    val proc2 = new Processing(testEntity, updatedSlice, Map("processing.time" -> newTime))
    proc2.Process(Historic) // Explicitly specify Historic strategy


    val result_df = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    // Filter results by test_id to avoid interference from other tests
    val filtered_result_df = result_df
      .filter($"test_id" === testId)
      .orderBy(s"${randomPrefix}ValidFrom")
    val result = filtered_result_df.collect()

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

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"system_prefix_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val df = Seq((1, "John", "Data1", testId)).toDF("id", "name", "data", "test_id")

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(f"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(1)
    val ioLocations = testEntity.OutputMethod

    val inMemoryDataFile = s"inmemory_data_${testId}.parquet"
    df.write.mode("overwrite").parquet(s"${testEntity.getPaths.bronzepath}/$inMemoryDataFile")

    org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testEntity.getPaths.silverpath))
    val proc = new Processing(testEntity, inMemoryDataFile)
    proc.Process()

    // Filter results by test_id to avoid interference from other tests
    val silver_df = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val testsetDf = silver_df.filter($"test_id" === testId)

    val expectedColumns = Seq(
      "id",
      "name",
      "data",
      "test_id",
      "PK_testentity",
      randomPrefix + "SourceHash",
      randomPrefix + "ValidFrom",
      randomPrefix + "ValidTo",
      randomPrefix + "IsCurrent",
      randomPrefix + "deleted",
      randomPrefix + "lastSeen"
    )

    expectedColumns.foreach { colName =>
      assert(testsetDf.columns.contains(colName), s"Expected column: '$colName'")
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
      secure_container_suffix = Some("-secure"),
      systemfield_prefix = Some(randomPrefix),
      output_method = "paths"
    )

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val testEntity = metadata.getEntity(2)
    val om = testEntity.OutputMethod 

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"watermark_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val sliceFile = s"wm_slice_${testId}.parquet"
    Seq((1, 10L, testId), (2, 5L, testId)).toDF("ID", "SeqNr", "test_id")
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

  test("Merge processing should handle incremental updates correctly") {
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
    val testEntity = metadata.getEntity(3) // Using entity 3 which has delta processtype but no silver_table setting
    val paths = testEntity.getPaths
    val ioLocations = testEntity.OutputMethod

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"merge_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Clean up any existing data for this entity to ensure clean test
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial full load - create baseline data
    val initialData = Seq(
      (1, 100L, "John", "Initial Data", testId),
      (2, 200L, "Jane", "Initial Data", testId),
      (3, 300L, "Bob", "Initial Data", testId)
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val initialSlice = s"initial_slice_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process() // This should create the initial delta table

    // Verify initial load
    val initialResult = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    assert(initialResult.count() === 3, "Initial load should create 3 records")

    // Step 2: Merge operation with updates, inserts, and deletes
    val mergeData = Seq(
      // Update existing record (ID=1, new SeqNr will trigger update)
      (1, 110L, "John Updated", "Updated Data", testId), 
      // Keep existing record unchanged (ID=2, same SeqNr should update lastSeen only)
      (2, 200L, "Jane", "Initial Data", testId),
      // Insert new record (ID=4)
      (4, 400L, "Alice", "New Data", testId)
      // ID=3 is missing from this batch - should be marked as deleted if it has deletion tracking
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val mergeSlice = s"merge_slice_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process()

    // Step 3: Verify merge results
    val mergeResult = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    mergeResult.show(false)
    
    val resultData = mergeResult.orderBy("ID").collect()

    // Should have 4 records total (3 original + 1 new)
    assert(resultData.length === 4, s"Should have 4 records after merge, but got ${resultData.length}")

    // Verify ID=1 was updated
    val record1 = resultData.find(_.getAs[Int]("ID") == 1).get
    assert(record1.getAs[String]("name") === "John Updated", "Record 1 should be updated")
    assert(record1.getAs[String]("data") === "Updated Data", "Record 1 data should be updated")
    assert(record1.getAs[Long]("SeqNr") === 110L, "Record 1 SeqNr should be updated")

    // Verify ID=2 exists and has updated lastSeen (data unchanged)
    val record2 = resultData.find(_.getAs[Int]("ID") == 2).get
    assert(record2.getAs[String]("name") === "Jane", "Record 2 should remain unchanged")
    assert(record2.getAs[String]("data") === "Initial Data", "Record 2 data should remain unchanged")
    assert(record2.getAs[Long]("SeqNr") === 200L, "Record 2 SeqNr should remain unchanged")

    // Verify ID=3 still exists (not deleted unless explicit deletion tracking is implemented)
    val record3 = resultData.find(_.getAs[Int]("ID") == 3).get
    assert(record3.getAs[String]("name") === "Bob", "Record 3 should still exist")
    assert(record3.getAs[Long]("SeqNr") === 300L, "Record 3 SeqNr should remain unchanged")

    // Verify ID=4 was inserted
    val record4 = resultData.find(_.getAs[Int]("ID") == 4).get
    assert(record4.getAs[String]("name") === "Alice", "Record 4 should be inserted")
    assert(record4.getAs[String]("data") === "New Data", "Record 4 data should be correct")
    assert(record4.getAs[Long]("SeqNr") === 400L, "Record 4 SeqNr should be correct")

    // Verify all records have system fields
    resultData.foreach { row =>
      assert(row.schema.fieldNames.contains(s"${randomPrefix}lastSeen"), "Should have lastSeen field")
      assert(row.schema.fieldNames.contains(s"${randomPrefix}SourceHash"), "Should have SourceHash field")
      assert(row.schema.fieldNames.contains(s"${randomPrefix}deleted"), "Should have deleted field")
    }
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
    val ioLocations = testEntity.OutputMethod

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"blank_slice_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Create and write a blank slice
    val emptyData = Seq.empty[(Int, String, String, String)].toDF("id", "name", "data", "test_id")
    val blankSlice = s"blank_slice_${testId}.parquet"
    emptyData.write.parquet(s"${paths.bronzepath}/$blankSlice")

    // Test 1: Processing a blank slice as first run
    val proc1 = new Processing(testEntity, blankSlice)
    proc1.Process(Full)

    // Verify no data was written for this test
    val result1 = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => 
        try {
          spark.read.table(tableLoc.table).filter($"test_id" === testId)
        } catch {
          case _: Exception => spark.emptyDataFrame.filter($"test_id" === testId) // Table might not exist for blank slice
        }
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    assert(result1.count() === 0, "No data should be written when processing a blank slice as first run")

    // Test 2: Processing a blank slice with existing data
    val existingData = Seq((1, "John", "Data1", testId)).toDF("id", "name", "data", "test_id")
    val dataSlice = s"data_slice_${testId}.parquet"
    existingData.write.parquet(s"${paths.bronzepath}/$dataSlice")

    // First process the data slice
    val procData = new Processing(testEntity, dataSlice)
    procData.Process(Full)

    // Then process the blank slice
    val proc2 = new Processing(testEntity, blankSlice)
    proc2.Process(Merge)

    // Verify existing data was preserved for this test
    val result2 = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    assert(result2.count() === 1, "Existing data should be preserved when processing a blank slice")
    val row = result2.collect()(0)
    assert(row.getAs[Int]("id") === 1, "Existing data should remain unchanged")
    assert(row.getAs[String]("name") === "John", "Existing data should remain unchanged")
    assert(row.getAs[String]("data") === "Data1", "Existing data should remain unchanged")
    assert(row.getAs[String]("test_id") === testId, "Test ID should match")
  }
}
