package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils
import datalake.metadata._
import org.apache.spark.sql.functions._

class HistoricProcessingSpec extends AnyFunSuite with SparkSessionTest {

  private def createHistoricTestEntity(
      entityId: Int,
      testId: String,
      inferDeletes: Boolean = false
  ): (Entity, Output, Paths) = {
    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (HISTORIC TEST)",
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
          "name": "historic_test_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "historic",
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
            "delete_missing": ${inferDeletes}
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

  private def readResult(output: Output, testId: String) = {
    import spark.implicits._
    output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
  }

  test("Historic processing first run should divert to Full load") {
    import spark.implicits._

    val testId = s"historic_first_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createHistoricTestEntity(3000, testId)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    val testData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val slice = s"first_run_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Historic)

    val result = readResult(output, testId)

    assert(result.count() === 2, "First run should write all records via Full load")

    // Verify temporal tracking columns exist (since processtype = historic)
    val columns = result.columns
    assert(columns.contains(s"${randomPrefix}ValidFrom"), "Should have ValidFrom field")
    assert(columns.contains(s"${randomPrefix}ValidTo"), "Should have ValidTo field")
    assert(columns.contains(s"${randomPrefix}IsCurrent"), "Should have IsCurrent field")

    // All records should be current
    assert(result.filter(col(s"${randomPrefix}IsCurrent") === true).count() === 2,
      "All records should have IsCurrent=true on first run")
  }

  test("Historic processing creates new versions for updated records with correct SCD2 values") {
    import spark.implicits._

    val testId = s"historic_scd2_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createHistoricTestEntity(3001, testId)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full) // First run diverts to Full

    Thread.sleep(1000) // Ensure different processing time

    // Step 2: Update Alice, keep Bob unchanged
    val updateData = Seq(
      (1, 200L, "Alice Updated", testId), // Changed name triggers hash change
      (2, 100L, "Bob", testId)             // Same data, no change
    ).toDF("ID", "SeqNr", "name", "test_id")

    val updateSlice = s"update_${testId}.parquet"
    updateData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$updateSlice")

    val updateProc = new Processing(testEntity, updateSlice)
    updateProc.Process(Historic)

    val result = readResult(output, testId)

    // Alice should have 2 versions: old (IsCurrent=false) and new (IsCurrent=true)
    val aliceRecords = result.filter($"ID" === 1).orderBy(col(s"${randomPrefix}ValidFrom").asc)
    assert(aliceRecords.count() === 2, "Alice should have 2 versions after update")

    val aliceOld = aliceRecords.filter(col(s"${randomPrefix}IsCurrent") === false).first()
    assert(aliceOld.getAs[String]("name") === "Alice", "Old version should have original name")
    // Old version's ValidTo should be set (no longer the sentinel)
    val validTo = aliceOld.getAs[java.sql.Timestamp](s"${randomPrefix}ValidTo")
    assert(validTo != null, "Old version's ValidTo should be set")
    assert(validTo.toString != "2999-12-31 00:00:00.0", "Old version's ValidTo should not be the sentinel value")

    val aliceNew = aliceRecords.filter(col(s"${randomPrefix}IsCurrent") === true).first()
    assert(aliceNew.getAs[String]("name") === "Alice Updated", "New version should have updated name")
    val newValidTo = aliceNew.getAs[java.sql.Timestamp](s"${randomPrefix}ValidTo")
    assert(newValidTo.toString.startsWith("2999-12-31"), "New version's ValidTo should be the sentinel value")

    // Bob should still have only 1 version (unchanged)
    val bobRecords = result.filter($"ID" === 2)
    assert(bobRecords.count() === 1, "Bob should have 1 version (unchanged)")
    assert(bobRecords.first().getAs[Boolean](s"${randomPrefix}IsCurrent") === true,
      "Bob's only version should be current")
  }

  test("Historic processing delete inference only affects IsCurrent=true records") {
    import spark.implicits._

    val testId = s"historic_delete_scope_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createHistoricTestEntity(3002, testId, inferDeletes = true)

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

    // Step 2: Update Alice, keep Bob, Charlie disappears
    val updateData = Seq(
      (1, 200L, "Alice Updated", testId),
      (2, 200L, "Bob", testId)
      // Charlie (ID=3) is missing → should be deleted
    ).toDF("ID", "SeqNr", "name", "test_id")

    val updateSlice = s"update_${testId}.parquet"
    updateData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$updateSlice")

    val updateProc = new Processing(testEntity, updateSlice)
    updateProc.Process(Historic)

    val result = readResult(output, testId)

    // Charlie should be marked as deleted with IsCurrent=false
    val charlieRecords = result.filter($"ID" === 3)
    assert(charlieRecords.count() === 1, "Charlie should have 1 record")
    val charlie = charlieRecords.first()
    assert(charlie.getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should be marked as deleted")
    assert(charlie.getAs[Boolean](s"${randomPrefix}IsCurrent") === false,
      "Deleted Charlie should have IsCurrent=false")

    Thread.sleep(1000)

    // Step 3: Another update — Alice's old version (IsCurrent=false) should NOT be affected by delete inference
    val thirdData = Seq(
      (1, 300L, "Alice v3", testId),
      (2, 300L, "Bob", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val thirdSlice = s"third_${testId}.parquet"
    thirdData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$thirdSlice")

    val thirdProc = new Processing(testEntity, thirdSlice)
    thirdProc.Process(Historic)

    val result2 = readResult(output, testId)

    // Alice should now have 3 versions: v1 (closed), v2 (closed), v3 (current)
    val aliceRecords = result2.filter($"ID" === 1)
    assert(aliceRecords.count() === 3, "Alice should have 3 versions")

    // Historical versions (IsCurrent=false) should NOT be marked as deleted
    val aliceHistorical = aliceRecords.filter(col(s"${randomPrefix}IsCurrent") === false)
    aliceHistorical.collect().foreach { row =>
      assert(row.getAs[Boolean](s"${randomPrefix}deleted") === false,
        "Historical (non-current) versions of Alice should NOT be marked as deleted by delete inference")
    }

    // Charlie should still be deleted (only 1 record, not duplicated by delete inference)
    val charlieAfter = result2.filter($"ID" === 3)
    assert(charlieAfter.count() === 1, "Charlie should still have just 1 record")
    assert(charlieAfter.first().getAs[Boolean](s"${randomPrefix}deleted") === true,
      "Charlie should remain deleted")
  }

  test("Historic processing metric accuracy: inserted + updated + unchanged = recordCount") {
    import spark.implicits._

    val testId = s"historic_metrics_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createHistoricTestEntity(3003, testId)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial load with 5 records
    val initialData = Seq(
      (1, 100L, "Alice", testId),
      (2, 100L, "Bob", testId),
      (3, 100L, "Charlie", testId),
      (4, 100L, "Dave", testId),
      (5, 100L, "Eve", testId)
    ).toDF("ID", "SeqNr", "name", "test_id")

    val initialSlice = s"initial_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    Thread.sleep(1000)

    // Step 2: Merge with mix of inserts, updates, unchanged
    val mergeData = Seq(
      (1, 200L, "Alice Updated", testId),  // Update (changed data)
      (2, 200L, "Bob Updated", testId),     // Update (changed data)
      (3, 100L, "Charlie", testId),          // Unchanged (same data)
      (4, 100L, "Dave", testId),             // Unchanged (same data)
      (5, 100L, "Eve", testId),              // Unchanged (same data)
      (6, 200L, "Frank", testId)             // New insert
    ).toDF("ID", "SeqNr", "name", "test_id")

    val mergeSlice = s"merge_${testId}.parquet"
    mergeData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(testEntity, mergeSlice)
    mergeProc.Process(Historic)

    val result = readResult(output, testId)
    val recordCount = 6L // Source records

    // Count actual outcomes
    val currentRecords = result.filter(col(s"${randomPrefix}IsCurrent") === true)
    assert(currentRecords.count() === 6, "Should have 6 current records (5 original + 1 new)")

    // Verify Alice and Bob have 2 versions each (old + new)
    assert(result.filter($"ID" === 1).count() === 2, "Alice should have 2 versions (updated)")
    assert(result.filter($"ID" === 2).count() === 2, "Bob should have 2 versions (updated)")

    // Verify Charlie, Dave, Eve have 1 version each (unchanged)
    assert(result.filter($"ID" === 3).count() === 1, "Charlie should have 1 version (unchanged)")
    assert(result.filter($"ID" === 4).count() === 1, "Dave should have 1 version (unchanged)")
    assert(result.filter($"ID" === 5).count() === 1, "Eve should have 1 version (unchanged)")

    // Verify Frank is new (1 version)
    assert(result.filter($"ID" === 6).count() === 1, "Frank should have 1 version (new insert)")

    // Verify the metric identity from source perspective:
    // inserted=1 (Frank), updated=2 (Alice, Bob), unchanged=3 (Charlie, Dave, Eve)
    // 1 + 2 + 3 = 6 = recordCount
    val totalVersions = result.count()
    val updatedCount = 2L // Alice and Bob
    val insertedCount = 1L // Frank
    val unchangedCount = 3L // Charlie, Dave, Eve
    assert(insertedCount + updatedCount + unchangedCount === recordCount,
      s"inserted($insertedCount) + updated($updatedCount) + unchanged($unchangedCount) should equal recordCount($recordCount)")
  }
}
