package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils
import datalake.metadata._
import org.apache.spark.sql.functions._

/**
  * Tests for inferDeletesFromMissing functionality in both Merge and Historic strategies.
  *
  * inferDeletesFromMissing automatically soft-deletes records that exist in the target
  * but are missing from the source slice, within the watermark window.
  */
class InferDeletesSpec extends AnyFunSuite with SparkSessionTest {

  /**
    * Helper to create a test entity with inferDeletesFromMissing enabled
    */
  private def createTestEntityWithInferDeletes(
      entityId: Int,
      processType: String = "delta",
      testId: String
  ): (Entity, Output, Paths) = {
    val testDbName = s"test_db_${testId}"
    val testTableName = s"${testDbName}.test_table"

    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (INFER DELETES TEST)",
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
          "id": ${entityId},
          "name": "infer_delete_test_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "${processType}",
          "watermark": [
            {
              "column_name": "SeqNr",
              "operation": "or",
              "operation_group": 0,
              "expression": "'$${last_value}'"
            }
          ],
          "columns": [
            {
              "name": "ID",
              "fieldroles": ["businesskey"]
            }
          ],
          "settings": {
            "delete_missing": true
          },
          "transformations": []
        }
      ]
    }
    """

    val settings = new StringMetadataSettings()
    settings.initialize(testMetadataJson)
    val metadata = new Metadata(settings, override_env)
    val entity = metadata.getEntity(entityId)
    val output = entity.getOutput
    val paths = entity.getPaths

    (entity, output, paths)
  }

  test("inferDeletesFromMissing: Basic delete inference in Merge strategy") {
    import spark.implicits._

    val testId = s"merge_delete_basic_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1000, "delta", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load with SeqNr=100
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId),
      (3, 100L, "Charlie", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Verify initial state
    val afterInitial = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
    assert(afterInitial.count() === 3, "Initial load should have 3 records")
    assert(afterInitial.filter(col(s"${randomPrefix}deleted") === false).count() === 3,
      "All records should be active initially")

    Thread.sleep(1000) // Ensure time difference

    // Step 2: Merge with SeqNr=200, but only Alice and Bob are present
    // Charlie should be marked as deleted because he's missing from the watermark window
    val mergeData = Seq(
      (1, 200L, "Alice Updated", testId),
      (2, 200L, "Bob", testId)
      // Charlie (ID=3) is intentionally missing
    ).toDF("ID", "SeqNr", "name", "test_id")

    val mergeSlice = s"merge_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Merge)

    // Step 3: Verify Charlie is marked as deleted
    val afterMerge = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(afterMerge.count() === 3, "Should still have 3 records")

    val charlieRecord = afterMerge.filter($"ID" === 3).first()
    assert(charlieRecord.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should be marked as deleted")
    assert(charlieRecord.getAs[String]("name") === "Charlie",
      "Charlie's data should remain unchanged except deleted flag")

    val activeRecords = afterMerge.filter(col(s"${randomPrefix}deleted") === false)
    assert(activeRecords.count() === 2, "Should have 2 active records")
    assert(activeRecords.filter($"ID" === 1).count() === 1, "Alice should be active")
    assert(activeRecords.filter($"ID" === 2).count() === 1, "Bob should be active")
  }

  test("inferDeletesFromMissing: First run should not delete anything") {
    import spark.implicits._

    val testId = s"merge_delete_first_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1001, "delta", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // First run with Merge strategy (will divert to Full load, no previous watermark)
    val firstData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val firstSlice = s"first_${testId}.parquet"
    firstData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$firstSlice")

    val firstProc = new Processing(testEntity, firstSlice)
    firstProc.Process(Merge) // Will divert to Full because table doesn't exist

    // Verify no records are marked as deleted
    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 2, "Should have 2 records")
    assert(result.filter(col(s"${randomPrefix}deleted") === true).count() === 0,
      "No records should be deleted on first run")
  }

  test("inferDeletesFromMissing: Already deleted records should not be updated again") {
    import spark.implicits._

    val testId = s"merge_delete_norepeat_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1002, "delta", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId),
      (3, 100L, "Charlie", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    Thread.sleep(1000)

    // Step 2: Merge without Charlie - marks him as deleted
    val merge1Data = Seq(
      (1, 200L, "Alice", testId),
      (2, 200L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val merge1Slice = s"merge1_${testId}.parquet"
    merge1Data.write.mode("overwrite").parquet(s"${paths.bronzepath}/$merge1Slice")

    val merge1Proc = new Processing(testEntity, merge1Slice)
    merge1Proc.Process(Merge)

    // Get Charlie's lastSeen timestamp after first delete
    val afterFirstDelete = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val charlieFirstDelete = afterFirstDelete.filter($"ID" === 3).first()
    val firstDeleteTime = charlieFirstDelete.getAs[java.sql.Timestamp](s"${randomPrefix}lastSeen")
    assert(charlieFirstDelete.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should be deleted after first merge")

    Thread.sleep(2000) // Wait to ensure timestamp would change if updated

    // Step 3: Another merge without Charlie - should NOT update his lastSeen
    val merge2Data = Seq(
      (1, 300L, "Alice Updated", testId),
      (2, 300L, "Bob Updated", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val merge2Slice = s"merge2_${testId}.parquet"
    merge2Data.write.mode("overwrite").parquet(s"${paths.bronzepath}/$merge2Slice")

    val merge2Proc = new Processing(testEntity, merge2Slice)
    merge2Proc.Process(Merge)

    // Verify Charlie's lastSeen timestamp didn't change
    val afterSecondMerge = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val charlieSecondCheck = afterSecondMerge.filter($"ID" === 3).first()
    val secondCheckTime = charlieSecondCheck.getAs[java.sql.Timestamp](s"${randomPrefix}lastSeen")

    assert(charlieSecondCheck.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should still be deleted")
    assert(firstDeleteTime.equals(secondCheckTime),
      s"Charlie's lastSeen should not have changed (was: $firstDeleteTime, now: $secondCheckTime)")
  }

  test("inferDeletesFromMissing: Records outside watermark window should not be deleted") {
    import spark.implicits._

    val testId = s"merge_delete_window_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1003, "delta", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load with various SeqNr values (excluding future records for now)
    val initialData = Seq(
      (1, 50L, "Old Record", testId),      // Outside window (below)
      (2, 100L, "Alice", testId),          // In window
      (3, 100L, "Bob", testId)             // In window
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    Thread.sleep(1000)

    // Step 2: Merge with SeqNr range 100-200, Alice and a Future Record
    // Bob should be deleted (in window 100-200, missing from source)
    // Old Record should NOT be deleted (SeqNr=50, outside window below)
    // Future Record should NOT be deleted (SeqNr=500, outside window above)
    val mergeData = Seq(
      (2, 200L, "Alice Updated", testId),
      (4, 500L, "Future Record", testId)   // Outside window (above)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val mergeSlice = s"merge_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Merge)

    // Verify results
    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    val records = result.collect()
    assert(records.length === 4, "Should still have all 4 records")

    // Alice should be active and updated
    val aliceRecord = records.find(_.getAs[Int]("ID") == 2).get
    assert(aliceRecord.getAs[Boolean](s"${randomPrefix}deleted") === false, "Alice should be active")
    assert(aliceRecord.getAs[String]("name") === "Alice Updated", "Alice should be updated")

    // Bob should be deleted (was in watermark window, missing from source)
    val bobRecord = records.find(_.getAs[Int]("ID") == 3).get
    assert(bobRecord.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Bob should be deleted (in watermark window)")

    // Old Record should still be active (outside watermark window - below)
    val oldRecord = records.find(_.getAs[Int]("ID") == 1).get
    assert(oldRecord.getAs[Boolean](s"${randomPrefix}deleted") === false,
      "Old Record should NOT be deleted (outside watermark window)")

    // Future Record should still be active (outside watermark window - above)
    val futureRecord = records.find(_.getAs[Int]("ID") == 4).get
    assert(futureRecord.getAs[Boolean](s"${randomPrefix}deleted") === false,
      "Future Record should NOT be deleted (outside watermark window)")
  }

  test("inferDeletesFromMissing: Historic strategy should mark records as deleted with IsCurrent=false") {
    import spark.implicits._

    val testId = s"historic_delete_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1004, "historic", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId),
      (3, 100L, "Charlie", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    Thread.sleep(1000)

    // Step 2: Historic merge without Charlie
    val mergeData = Seq(
      (1, 200L, "Alice Updated", testId),
      (2, 200L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val mergeSlice = s"merge_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Historic)

    // Verify results
    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    // Charlie should have a deleted version with IsCurrent=false
    val charlieRecords = result.filter($"ID" === 3).collect()
    assert(charlieRecords.length === 1, "Charlie should have 1 record (marked deleted)")

    val charlieRecord = charlieRecords.head
    assert(charlieRecord.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should be marked as deleted")
    assert(charlieRecord.getAs[Boolean](s"${randomPrefix}IsCurrent") === false,
      "Charlie should have IsCurrent=false when deleted in Historic")

    // Verify Alice has 2 versions (old and new)
    val aliceRecords = result.filter($"ID" === 1).collect()
    assert(aliceRecords.length === 2, "Alice should have 2 versions after update")

    val currentAlice = aliceRecords.find(_.getAs[Boolean](s"${randomPrefix}IsCurrent")).get
    assert(currentAlice.getAs[String]("name") === "Alice Updated",
      "Current Alice version should have updated name")
    assert(currentAlice.getAs[Boolean](s"${randomPrefix}deleted") === false,
      "Current Alice should not be deleted")
  }

  test("inferDeletesFromMissing: Multiple watermark window iterations") {
    import spark.implicits._

    val testId = s"merge_delete_multi_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val testData = createTestEntityWithInferDeletes(1005, "delta", testId)
    val testEntity = testData._1
    val output: Output = testData._2.asInstanceOf[Output]
    val paths = testData._3

    // Clean up
    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load with SeqNr=100
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId),
      (3, 100L, "Charlie", testId),
      (4, 100L, "Dave", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    Thread.sleep(1000)

    // Step 2: First merge (SeqNr 100->200) - Charlie disappears
    val merge1Data = Seq(
      (1, 200L, "Alice", testId),
      (2, 200L, "Bob", testId),
      (4, 200L, "Dave", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val merge1Slice = s"merge1_${testId}.parquet"
    merge1Data.write.mode("overwrite").parquet(s"${paths.bronzepath}/$merge1Slice")

    val merge1Proc = new Processing(testEntity, merge1Slice)
    merge1Proc.Process(Merge)

    Thread.sleep(1000)

    // Step 3: Second merge (SeqNr 200->300) - Bob disappears
    val merge2Data = Seq(
      (1, 300L, "Alice", testId),
      (4, 300L, "Dave", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val merge2Slice = s"merge2_${testId}.parquet"
    merge2Data.write.mode("overwrite").parquet(s"${paths.bronzepath}/$merge2Slice")

    val merge2Proc = new Processing(testEntity, merge2Slice)
    merge2Proc.Process(Merge)

    // Verify final state
    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 4, "Should have all 4 records")

    // Alice and Dave should be active
    assert(result.filter($"ID" === 1 && col(s"${randomPrefix}deleted") === false).count() === 1,
      "Alice should be active")
    assert(result.filter($"ID" === 4 && col(s"${randomPrefix}deleted") === false).count() === 1,
      "Dave should be active")

    // Charlie and Bob should be deleted
    assert(result.filter($"ID" === 3 && col(s"${randomPrefix}deleted") === true).count() === 1,
      "Charlie should be deleted from first merge")
    assert(result.filter($"ID" === 2 && col(s"${randomPrefix}deleted") === true).count() === 1,
      "Bob should be deleted from second merge")
  }
}
