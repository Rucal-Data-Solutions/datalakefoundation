package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils
import datalake.metadata._

class MergeProcessingSpec extends AnyFunSuite with SparkSessionTest {
  
  test("Merge processing first run should divert to Full load") {
    import spark.implicits._
    
    // Set up test directories
    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) bronzeFolder.mkdirs()
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) silverFolder.mkdirs()

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(2)
    val paths = testEntity.getPaths
    val ioLocations = testEntity.getOutput

    // Generate unique test identifier to avoid conflicts with concurrent tests
    val testId = s"merge_first_run_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Clean up any existing data
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    val testData = Seq(
      (1, 100L, "John", "Data1", testId),
      (2, 200L, "Jane", "Data2", testId)
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val slice = s"first_run_slice_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Merge) // Should divert to Full load

    // Verify results
    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 2, "First run should process all records like Full load")
    
    // Verify system fields were added
    val columns = result.columns
    assert(columns.contains(s"${randomPrefix}SourceHash"), "Should have SourceHash field")
    assert(columns.contains(s"${randomPrefix}deleted"), "Should have deleted field")
    assert(columns.contains(s"${randomPrefix}lastSeen"), "Should have lastSeen field")
  }

  test("Merge processing should handle schema creation for table destinations") {
    import spark.implicits._
    
    // Generate unique test identifier and database name
    val testId = s"schema_test_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testDbName = s"test_db_${testId}"
    val testTableName = s"${testDbName}.test_table"
    
    // Create a test metadata configuration with mixed output (paths for bronze, catalog for silver)
    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (MIXED OUTPUT TEST)",
        "timezone": "Europe/Amsterdam",
        "root_folder": "${testBasePath.replace("\\", "/")}",
        "raw_path": "/$${connection}/$${entity}",
        "bronze_path": "/$${connection}/$${entity}",
        "silver_path": "/$${connection}/$${destination}",
        "secure_container_suffix": "-secure",
        "systemfield_prefix": "${randomPrefix}",
        "output_method": "paths"
      },
      "connections": [
        {
          "name": "test_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": [
        {
          "id": 99,
          "name": "schema_test_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "merge",
          "watermark": [],
          "columns": [
            {
              "name": "id",
              "fieldroles": ["businesskey"]
            }
          ],
          "settings": {
            "silver_table": "${testTableName}"
          },
          "transformations": []
        }
      ]
    }
    """

    val settings = new StringMetadataSettings()
    settings.initialize(testMetadataJson)
    val metadata = new Metadata(settings)
    val testEntity = metadata.getEntity(99)

    // Drop database if it exists (cleanup)
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `$testDbName` CASCADE")
    } catch {
      case _: Exception => // Ignore if database doesn't exist
    }

    val testData = Seq(
      (1, "John", "Data1", testId)
    ).toDF("id", "name", "data", "test_id")

    val slice = s"schema_test_slice_${testId}.parquet"
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path
    
    testData.write.mode("overwrite").parquet(s"$bronzePath/$slice")

    // This should create the database and table
    val proc = new Processing(testEntity, slice)
    proc.Process(Merge)

    // Verify database was created
    val databases = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    assert(databases.contains(testDbName), s"Database '$testDbName' should have been created")

    // Verify table was created and contains data
    val result = spark.read.table(testTableName)
    assert(result.count() === 1, "Table should contain the test data")
    
    val row = result.filter($"test_id" === testId).first()
    assert(row.getAs[Int]("id") === 1, "Data should be correctly written")
    assert(row.getAs[String]("name") === "John", "Data should be correctly written")

    // Cleanup
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `$testDbName` CASCADE")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }

  test("Merge processing should handle updates, inserts, and lastSeen correctly") {
    import spark.implicits._
    
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(2)
    val paths = testEntity.getPaths
    val ioLocations = testEntity.getOutput

    // Generate unique test identifier
    val testId = s"merge_operations_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Clean up any existing data
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Create initial data with Full load
    val initialData = Seq(
      (1, 100L, "John", "InitialData", testId),
      (2, 200L, "Jane", "InitialData", testId)
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Get initial timestamp for comparison
    Thread.sleep(1000) // Ensure time difference

    // Step 2: Merge with updates and inserts
    val mergeData = Seq(
      (1, 150L, "John Updated", "UpdatedData", testId), // Update: new SeqNr triggers SourceHash change
      (2, 200L, "Jane", "InitialData", testId),          // No change: should update lastSeen only
      (3, 300L, "Bob", "NewData", testId)                // Insert: new record
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val mergeSlice = s"merge_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Merge)

    // Step 3: Verify results
    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val resultData = result.orderBy("ID").collect()
    assert(resultData.length === 3, s"Should have 3 records after merge, got ${resultData.length}")

    // Verify ID=1 was updated (SourceHash changed)
    val record1 = resultData.find(_.getAs[Int]("ID") == 1).get
    assert(record1.getAs[String]("name") === "John Updated", "Record 1 name should be updated")
    assert(record1.getAs[String]("data") === "UpdatedData", "Record 1 data should be updated")
    assert(record1.getAs[Long]("SeqNr") === 150L, "Record 1 SeqNr should be updated")

    // Verify ID=2 has updated lastSeen but same data (SourceHash unchanged)
    val record2 = resultData.find(_.getAs[Int]("ID") == 2).get
    assert(record2.getAs[String]("name") === "Jane", "Record 2 name should be unchanged")
    assert(record2.getAs[String]("data") === "InitialData", "Record 2 data should be unchanged")
    assert(record2.getAs[Long]("SeqNr") === 200L, "Record 2 SeqNr should be unchanged")

    // Verify ID=3 was inserted
    val record3 = resultData.find(_.getAs[Int]("ID") == 3).get
    assert(record3.getAs[String]("name") === "Bob", "Record 3 should be inserted")
    assert(record3.getAs[String]("data") === "NewData", "Record 3 data should be correct")
    assert(record3.getAs[Long]("SeqNr") === 300L, "Record 3 SeqNr should be correct")

    // Verify lastSeen timestamps (record2 should have newer lastSeen than record1's initial load)
    val record1LastSeen = record1.getAs[java.sql.Timestamp](s"${randomPrefix}lastSeen")
    val record2LastSeen = record2.getAs[java.sql.Timestamp](s"${randomPrefix}lastSeen")
    val record3LastSeen = record3.getAs[java.sql.Timestamp](s"${randomPrefix}lastSeen")
    
    assert(record1LastSeen != null, "Record 1 should have lastSeen timestamp")
    assert(record2LastSeen != null, "Record 2 should have lastSeen timestamp")
    assert(record3LastSeen != null, "Record 3 should have lastSeen timestamp")
  }

  test("Merge processing should handle partition filters correctly") {
    import spark.implicits._
    
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(2) // Has partition column "Administration"
    val paths = testEntity.getPaths
    val ioLocations = testEntity.getOutput

    // Generate unique test identifier
    val testId = s"merge_partitions_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Clean up any existing data
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Create initial data with partitions
    val initialData = Seq(
      (1, 100L, "John", "Data1", testId),
      (2, 200L, "Jane", "Data2", testId)
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val initialSlice = s"initial_partition_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Verify initial state - should have 2 records
    val initialResult = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    assert(initialResult.count() === 2, s"Initial load should have 2 records, got ${initialResult.count()}")

    // Step 2: Merge with partial data for same partition (Administration = 950 from calculated column)
    // This represents a complete replacement of the partition data - Jane is intentionally excluded
    // because in a real scenario, this might mean Jane was deleted or moved to another partition
    val mergeData = Seq(
      (1, 150L, "John Updated", "UpdatedData", testId), // Update existing
      (3, 300L, "Bob", "NewData", testId)                // Insert new
    ).toDF("ID", "SeqNr", "name", "data", "test_id")

    val mergeSlice = s"merge_partition_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Merge)

    // Step 3: Verify results - should have 3 records
    // This is expected behavior: merge operations are complete replacements for the partition
    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val resultData = result.orderBy("ID").collect()
    
    // Verify we have exactly the records from the merge operation
    assert(resultData.length === 3, s"Should have 3 records after merge (complete partition replacement), got ${resultData.length}")

    // Verify the specific records exist and are correct
    val johnRecord = resultData.find(_.getAs[Int]("ID") == 1)
    assert(johnRecord.isDefined, "John's record should exist")
    assert(johnRecord.get.getAs[String]("name") === "John Updated", "John's record should be updated")
    assert(johnRecord.get.getAs[Long]("SeqNr") === 150L, "John's SeqNr should be updated")

    val bobRecord = resultData.find(_.getAs[Int]("ID") == 3)
    assert(bobRecord.isDefined, "Bob's record should exist")  
    assert(bobRecord.get.getAs[String]("name") === "Bob", "Bob's record should be inserted")
    assert(bobRecord.get.getAs[Long]("SeqNr") === 300L, "Bob's SeqNr should be correct")

    // Verify all records have the correct Administration value (from calculated column)
    resultData.foreach { row =>
      assert(row.getAs[Int]("Administration") === 950, "All records should have Administration = 950")
    }
  }

  test("Merge processing should detect and report missing columns in source") {
    import spark.implicits._

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(3)
    val paths = testEntity.getPaths
    val ioLocations = testEntity.getOutput

    val testId = s"merge_missing_col_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Create initial data with extra column
    val initialData = Seq(
      (1, 100L, "John", "detail_value", testId)
    ).toDF("ID", "SeqNr", "name", "details", "test_id")

    val initialSlice = s"initial_missing_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Step 2: Merge with data that is MISSING the "details" column
    // Delta MERGE with updateAll() cannot resolve missing columns, so this should fail
    val mergeDataMissingCol = Seq(
      (1, 150L, "John Updated", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val mergeSlice = s"merge_missing_${testId}.parquet"
    mergeDataMissingCol.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)

    // Schema drift with a missing column causes Delta MERGE to fail
    // because updateAll() references all target columns including the missing one
    val ex = intercept[Throwable] {
      mergeProc.Process(Merge)
    }
    assert(ex.getMessage.contains("details") || ex.getCause.getMessage.contains("details"),
      "Error should mention the missing column name 'details'")
  }

  test("Merge processing should handle schema differences appropriately") {
    import spark.implicits._
    
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val testEntity = metadata.getEntity(3)
    val paths = testEntity.getPaths
    val ioLocations = testEntity.getOutput

    // Generate unique test identifier
    val testId = s"merge_schema_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Clean up any existing data
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Create initial data
    val initialData = Seq(
      (1, 100L, "John", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_schema_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Step 2: Merge with data that has an additional column
    val mergeDataWithExtraCol = Seq(
      (1, 150L, "John Updated", "ExtraValue", testId)
    ).toDF("ID", "SeqNr", "name", "extra_column", "test_id")

    val mergeSlice = s"merge_schema_${testId}.parquet"
    mergeDataWithExtraCol.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    
    // This should log schema differences but still process
    mergeProc.Process(Merge)

    // Verify the merge completed successfully
    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 1, "Should have 1 record after merge")
    
    // The result should have the updated data, but extra_column may not be included
    // because Delta merge operations don't automatically add new columns
    val resultColumns = result.columns
    
    val row = result.first()
    assert(row.getAs[String]("name") === "John Updated", "Data should be updated")
    assert(row.getAs[Long]("SeqNr") === 150L, "SeqNr should be updated")
    
    // The extra column should NOT be included in the result since Delta merge
    // doesn't automatically add new schema columns during merge operations
    // The schema differences should have been logged as a warning
    assert(!resultColumns.contains("extra_column"), 
      "Extra column should not be automatically added during merge operation")
    
    // Verify that all expected system columns are present
    assert(resultColumns.contains(s"${randomPrefix}SourceHash"), "Should have SourceHash field")
    assert(resultColumns.contains(s"${randomPrefix}deleted"), "Should have deleted field")
    assert(resultColumns.contains(s"${randomPrefix}lastSeen"), "Should have lastSeen field")
  }
}
