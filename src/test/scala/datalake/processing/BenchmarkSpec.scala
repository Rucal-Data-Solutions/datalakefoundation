package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import java.sql.Timestamp
import datalake.core.WatermarkData

class BenchmarkSpec extends AnyFunSuite with SparkSessionTest {
  test("Benchmark Full, Merge and Historic processing") {
    import spark.implicits._
    import org.apache.commons.io.FileUtils

    // Warm up Spark engine
    info("Warming up Spark engine...")
    val warmupData = (1 to 1000).map(i => (i, i.toLong)).toDF("ID", "SeqNr")
    warmupData.cache().count() // Force execution and cache the data
    warmupData.unpersist() // Clean up the cache
    info("Warm-up complete")

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) bronzeFolder.mkdirs()
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) silverFolder.mkdirs()

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, override_env)
    val mergeEntity = metadata.getEntity(2)
    val historicEntity = metadata.getEntity(1)

    FileUtils.deleteDirectory(new java.io.File(mergeEntity.getPaths.silverpath))
    FileUtils.deleteDirectory(new java.io.File(historicEntity.getPaths.silverpath))

    val fullSlice = "full_slice.parquet"
    val fullData = (1 to 10000).map(i => (i, i.toLong)).toDF("ID", "SeqNr")
    fullData.write.mode("overwrite").parquet(s"${mergeEntity.getPaths.bronzepath}/$fullSlice")

    val fullProc = new Processing(mergeEntity, fullSlice)
    val fullStart = System.nanoTime()
    fullProc.Process(Full)
    val fullDuration = (System.nanoTime() - fullStart) / 1e6d
    info(f"Full processing took $fullDuration%.2f ms")
    assert(fullDuration >= 0)

    val mergeSlice = "merge_slice.parquet"
    val mergeData = (1 to 10000).map(i => (i, i.toLong + 1)).toDF("ID", "SeqNr")
    mergeData.write.mode("overwrite").parquet(s"${mergeEntity.getPaths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(mergeEntity, mergeSlice)
    val mergeStart = System.nanoTime()
    mergeProc.Process(Merge)
    val mergeDuration = (System.nanoTime() - mergeStart) / 1e6d
    info(f"Merge processing took $mergeDuration%.2f ms")
    assert(mergeDuration >= 0)

    val historicSlice = "historic_slice.parquet"
    val histData = (1 to 10000).map(i => i).toDF("id")
    histData.write.mode("overwrite").parquet(s"${historicEntity.getPaths.bronzepath}/$historicSlice")

    val historicProc = new Processing(historicEntity, historicSlice)
    val historicStart = System.nanoTime()
    historicProc.Process(Historic)
    val historicDuration = (System.nanoTime() - historicStart) / 1e6d
    info(f"Historic processing took $historicDuration%.2f ms")
    assert(historicDuration >= 0)
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
		val ioLocations = testEntity.getOutput

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
		val ioLocations = testEntity.getOutput

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
		val ioLocations = testEntity.getOutput

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
		val om = testEntity.getOutput 

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
		val ioLocations = testEntity.getOutput

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
		val ioLocations = testEntity.getOutput

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

	test("Processing exposes current watermark values from previous run") {
		import spark.implicits._
		import org.apache.commons.io.FileUtils
		implicit val env: Environment = override_env

		// Ensure a clean system table for watermark tracking
		FileUtils.deleteDirectory(new java.io.File(s"$testBasePath/system"))

		val settings = new JsonMetadataSettings()
		val user_dir = System.getProperty("user.dir")
		settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

		val metadata = new Metadata(settings, env)
		val testEntity = metadata.getEntity(2) // has SeqNr watermark
		val watermark = testEntity.Watermark.head
		val ioLocations = testEntity.getOutput

		// Seed previous watermark value (simulating a prior processing run)
		val wmd = new WatermarkData(testEntity.Id)
		wmd.WriteWatermark(Seq((watermark, 5L)))

		// Create a new slice with higher SeqNr values to produce a new max
		val testId = s"watermark_current_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
		val sliceFile = s"wm_current_slice_${testId}.parquet"
		Seq((1, 10L, testId), (2, 20L, testId)).toDF("ID", "SeqNr", "test_id")
			.write.mode("overwrite").parquet(s"${ioLocations.bronze.asInstanceOf[PathLocation].path}/$sliceFile")

		val proc = new Processing(testEntity, sliceFile)
		val source = proc.getSource

		val currentValues = source.current_watermark_values.getOrElse(Array.empty)
		val seqCurrent = currentValues.find(_._1.Column_Name == "SeqNr").map(_._2.toString)
		val futureValues = source.watermark_values.getOrElse(Array.empty)
		val seqFuture = futureValues.find(_._1.Column_Name == "SeqNr").map(_._2.toString)

		assert(seqCurrent.contains("'5'"), s"Expected current watermark to reflect previous stored value '5', got ${seqCurrent.getOrElse("none")}")
		assert(seqFuture.contains("20"), s"Expected future watermark to reflect slice max '20', got ${seqFuture.getOrElse("none")}")
		assert(seqCurrent != seqFuture, "Current watermark should represent previous value, not the max from the current slice")
	}
}