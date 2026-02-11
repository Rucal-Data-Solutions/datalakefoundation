package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import org.apache.commons.io.FileUtils
import datalake.metadata._
import org.apache.spark.sql.functions._

class FullProcessingSpec extends AnyFunSuite with SparkSessionTest {

  private def createTestEntity(
      entityId: Int,
      testId: String,
      hasPartition: Boolean = false,
      useCatalog: Boolean = false
  ): (Entity, Output, Paths) = {
    val partitionColumn = if (hasPartition) """
      {
        "name": "",
        "newname": "region",
        "datatype": "string",
        "fieldroles": ["calculated", "partition"],
        "expression": "'EMEA'"
      },""" else ""

    val silverSetting = if (useCatalog) s""" "silver_table": "test_db_${testId}.test_table" """ else ""

    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (FULL TEST)",
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
          "name": "full_test_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "full",
          "watermark": [],
          "columns": [
            ${partitionColumn}
            {
              "name": "ID",
              "fieldroles": ["businesskey"]
            }
          ],
          "settings": {
            ${silverSetting}
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

  test("Full processing writes all records to Delta table at path") {
    import spark.implicits._

    val testId = s"full_basic_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createTestEntity(2000, testId)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    val testData = Seq(
      (1, "Alice", testId),
      (2, "Bob", testId),
      (3, "Charlie", testId)
    ).toDF("ID", "name", "test_id")

    val slice = s"full_basic_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Full)

    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 3, "Full load should write all 3 records")

    val columns = result.columns
    assert(columns.contains(s"${randomPrefix}SourceHash"), "Should have SourceHash field")
    assert(columns.contains(s"${randomPrefix}deleted"), "Should have deleted field")
    assert(columns.contains(s"${randomPrefix}lastSeen"), "Should have lastSeen field")
  }

  test("Full processing with partitioned data uses dynamic partition overwrite") {
    import spark.implicits._

    val testId = s"full_partition_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createTestEntity(2001, testId, hasPartition = true)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    // Step 1: Initial full load
    val initialData = Seq(
      (1, "Alice", testId),
      (2, "Bob", testId)
    ).toDF("ID", "name", "test_id")

    val initialSlice = s"initial_partition_${testId}.parquet"
    initialData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$initialSlice")

    val initialProc = new Processing(testEntity, initialSlice)
    initialProc.Process(Full)

    // Step 2: Overwrite with new data for the same partition
    val newData = Seq(
      (3, "Charlie", testId),
      (4, "Dave", testId)
    ).toDF("ID", "name", "test_id")

    val newSlice = s"overwrite_partition_${testId}.parquet"
    newData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$newSlice")

    val newProc = new Processing(testEntity, newSlice)
    newProc.Process(Full)

    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    // With dynamic partition overwrite, only the EMEA partition is overwritten
    // Old records in the same partition should be replaced
    assert(result.count() === 2, "Dynamic partition overwrite should replace old data in the same partition")
    assert(result.filter($"ID" === 3).count() === 1, "New record Charlie should exist")
    assert(result.filter($"ID" === 4).count() === 1, "New record Dave should exist")
  }

  test("Full processing with table location creates database if needed") {
    import spark.implicits._

    val testId = s"full_catalog_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createTestEntity(2002, testId, useCatalog = true)

    // Clean up any existing database
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `test_db_${testId}` CASCADE")
    } catch {
      case _: Exception =>
    }

    val testData = Seq(
      (1, "Alice", testId),
      (2, "Bob", testId)
    ).toDF("ID", "name", "test_id")

    val slice = s"catalog_${testId}.parquet"
    val bronzePath = output.bronze.asInstanceOf[PathLocation].path
    testData.write.mode("overwrite").parquet(s"$bronzePath/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Full)

    // Verify database was created
    val databases = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    assert(databases.contains(s"test_db_${testId}"), s"Database 'test_db_${testId}' should have been created")

    // Verify table contains data
    val result = spark.read.table(s"test_db_${testId}.test_table")
    assert(result.count() === 2, "Table should contain 2 records")

    // Cleanup
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `test_db_${testId}` CASCADE")
    } catch {
      case _: Exception =>
    }
  }

  test("Full processing with various data types") {
    import spark.implicits._

    val testId = s"full_types_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"
    val (testEntity, output, paths) = createTestEntity(2003, testId)

    FileUtils.deleteDirectory(new java.io.File(paths.silverpath))

    val testData = Seq(
      (1, "text_value", 42L, 3.14, true, testId),
      (2, null, 0L, 0.0, false, testId)
    ).toDF("ID", "string_col", "long_col", "double_col", "bool_col", "test_id")

    val slice = s"types_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Full)

    val result = output.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 2, "Should handle various data types")

    val row1 = result.filter($"ID" === 1).first()
    assert(row1.getAs[String]("string_col") === "text_value")
    assert(row1.getAs[Long]("long_col") === 42L)
    assert(row1.getAs[Double]("double_col") === 3.14)
    assert(row1.getAs[Boolean]("bool_col") === true)

    // Null string should be handled
    val row2 = result.filter($"ID" === 2).first()
    assert(row2.isNullAt(row2.fieldIndex("string_col")), "Null values should be preserved")
  }
}
