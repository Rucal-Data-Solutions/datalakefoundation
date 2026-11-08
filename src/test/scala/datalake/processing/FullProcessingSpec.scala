package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import org.apache.spark.sql.functions._

class FullProcessingSpec extends AnyFunSuite with SparkSessionTest {

  private def createTestEntity(
      entityId: Int,
      testId: String,
      hasPartition: Boolean = false,
      useCatalog: Boolean = false,
      entitySettings: Seq[String] = Seq.empty,
      connectionSettings: Seq[String] = Seq.empty,
      bronzeTable: Option[String] = None
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
    val bronzeSetting = bronzeTable.map(t => s""" "bronze_table": "$t" """).toSeq
    val allEntitySettings =
      (Seq(silverSetting).filter(_.trim.nonEmpty) ++ bronzeSetting ++ entitySettings).mkString(",\n")

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
          "settings": {${connectionSettings.mkString(",")}}
        }
      ],
      "entities": [
        {
          "id": ${entityId},
          "name": "full_test_${Math.abs(testId.hashCode) % 1000000}",
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
            ${allEntitySettings}
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

  // ---- Empty slice protection (#72) ----

  private def readSilver(output: Output) = output.silver match {
    case PathLocation(path)  => spark.read.format("delta").load(path)
    case TableLocation(name) => spark.read.table(name)
  }

  private def writeSlice(paths: Paths, slice: String, ids: Seq[Int], testId: String): Unit = {
    import spark.implicits._
    ids.map(i => (i, s"name_$i", testId)).toDF("ID", "name", "test_id")
      .write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")
  }

  private def writeEmptySlice(paths: Paths, slice: String): Unit = {
    import spark.implicits._
    Seq.empty[(Int, String, String)].toDF("ID", "name", "test_id")
      .write.mode("overwrite").parquet(s"${paths.bronzepath}/$slice")
  }

  private def uniqueId(prefix: String) =
    s"${prefix}_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

  test("Empty slice on first run creates an empty table") {
    val testId = uniqueId("empty_first")
    val (entity, output, paths) = createTestEntity(2100, testId)
    writeEmptySlice(paths, "empty.parquet")

    new Processing(entity, "empty.parquet").Process(Full)

    assert(silverExists(output))
    assert(readSilver(output).count() === 0)
  }

  private def silverExists(output: Output): Boolean = output.silver match {
    case PathLocation(path)  => io.delta.tables.DeltaTable.isDeltaTable(spark, path)
    case TableLocation(name) => spark.catalog.tableExists(name)
  }

  test("Empty slice on a destination with rows throws when allow_empty_full_load is absent") {
    val testId = uniqueId("empty_guard")
    val (entity, output, paths) = createTestEntity(2101, testId)
    writeSlice(paths, "data.parquet", Seq(1, 2, 3), testId)
    writeEmptySlice(paths, "empty.parquet")
    new Processing(entity, "data.parquet").Process(Full)

    val ex = intercept[EmptyFullLoadException] {
      new Processing(entity, "empty.parquet").Process(Full)
    }
    assert(ex.getMessage.contains("Entity 2101"))
    assert(ex.getMessage.contains("allow_empty_full_load"))
    assert(readSilver(output).count() === 3, "Destination must be untouched")
  }

  test("Empty slice overwrites the destination when the entity allows it") {
    val testId = uniqueId("empty_entity_true")
    val (entity, output, paths) = createTestEntity(
      2102, testId, entitySettings = Seq(""""allow_empty_full_load": true"""))
    writeSlice(paths, "data.parquet", Seq(1, 2, 3), testId)
    writeEmptySlice(paths, "empty.parquet")
    new Processing(entity, "data.parquet").Process(Full)

    new Processing(entity, "empty.parquet").Process(Full)

    assert(readSilver(output).count() === 0)
  }

  test("Connection setting allows empty full load and the entity setting wins") {
    val connTrue = Seq(""""allow_empty_full_load": true""")

    val idA = uniqueId("empty_conn_true")
    val (entityA, outputA, pathsA) = createTestEntity(2103, idA, connectionSettings = connTrue)
    writeSlice(pathsA, "data.parquet", Seq(1, 2), idA)
    writeEmptySlice(pathsA, "empty.parquet")
    new Processing(entityA, "data.parquet").Process(Full)
    new Processing(entityA, "empty.parquet").Process(Full)
    assert(readSilver(outputA).count() === 0)

    val idB = uniqueId("empty_entity_false")
    val (entityB, outputB, pathsB) = createTestEntity(
      2104, idB,
      entitySettings = Seq(""""allow_empty_full_load": false"""),
      connectionSettings = connTrue)
    writeSlice(pathsB, "data.parquet", Seq(1, 2), idB)
    writeEmptySlice(pathsB, "empty.parquet")
    new Processing(entityB, "data.parquet").Process(Full)
    intercept[EmptyFullLoadException] {
      new Processing(entityB, "empty.parquet").Process(Full)
    }
    assert(readSilver(outputB).count() === 2)
  }

  test("Empty slice on an already empty destination does not throw") {
    val testId = uniqueId("empty_twice")
    val (entity, output, paths) = createTestEntity(2105, testId)
    writeEmptySlice(paths, "empty.parquet")

    new Processing(entity, "empty.parquet").Process(Full)
    new Processing(entity, "empty.parquet").Process(Full)

    assert(readSilver(output).count() === 0)
  }

  test("Empty slice on a partitioned destination with rows throws") {
    val testId = uniqueId("empty_partitioned")
    val (entity, output, paths) = createTestEntity(2106, testId, hasPartition = true)
    writeSlice(paths, "data.parquet", Seq(1, 2), testId)
    writeEmptySlice(paths, "empty.parquet")
    new Processing(entity, "data.parquet").Process(Full)

    intercept[EmptyFullLoadException] {
      new Processing(entity, "empty.parquet").Process(Full)
    }
    assert(readSilver(output).count() === 2)
  }

  test("Catalog bronze and catalog silver: unknown slice does not wipe silver") {
    import spark.implicits._

    val testId = uniqueId("empty_catalog")
    val bronzeDb = s"tdb_b_${Math.abs(testId.hashCode) % 1000000}"
    val bronzeTable = s"$bronzeDb.bronze_table"
    val (entity, output, _) =
      createTestEntity(2107, testId, useCatalog = true, bronzeTable = Some(bronzeTable))
    val filenameField = s"${randomPrefix}source_filename"
    val silverDb = s"test_db_$testId"

    try {
      spark.sql(s"DROP DATABASE IF EXISTS `$bronzeDb` CASCADE")
      spark.sql(s"DROP DATABASE IF EXISTS `$silverDb` CASCADE")
      spark.sql(s"CREATE DATABASE `$bronzeDb`")
      Seq((1, "a", testId, "sliceA"), (2, "b", testId, "sliceA"))
        .toDF("ID", "name", "test_id", filenameField)
        .write.format("delta").saveAsTable(bronzeTable)

      new Processing(entity, "sliceA").Process(Full)
      assert(output.silver.isInstanceOf[TableLocation])
      assert(readSilver(output).count() === 2)

      intercept[EmptyFullLoadException] {
        new Processing(entity, "sliceDoesNotExist").Process(Full)
      }
      assert(readSilver(output).count() === 2, "Silver must stay intact")
    } finally {
      spark.sql(s"DROP DATABASE IF EXISTS `$bronzeDb` CASCADE")
      spark.sql(s"DROP DATABASE IF EXISTS `$silverDb` CASCADE")
    }
  }

  private def withParquetSilverTable(testId: String)(body: (Entity, Paths, String) => Unit): Unit = {
    import spark.implicits._
    val (entity, _, paths) = createTestEntity(2108, testId, useCatalog = true)
    val silverDb = s"test_db_$testId"
    val table = s"$silverDb.test_table"
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `$silverDb` CASCADE")
      spark.sql(s"CREATE DATABASE `$silverDb`")
      Seq((1, "a", testId), (2, "b", testId)).toDF("ID", "name", "test_id")
        .write.format("parquet").saveAsTable(table)
      body(entity, paths, table)
    } finally {
      spark.sql(s"DROP DATABASE IF EXISTS `$silverDb` CASCADE")
    }
  }

  test("Full load with an empty slice onto an existing non-Delta table throws and leaves it") {
    val testId = uniqueId("empty_nondelta")
    withParquetSilverTable(testId) { (entity, paths, table) =>
      writeEmptySlice(paths, "empty.parquet")
      intercept[NotADeltaTableException] {
        new Processing(entity, "empty.parquet").Process(Full)
      }
      assert(spark.read.table(table).count() === 2)
    }
  }

  test("Merge onto an existing non-Delta table throws instead of diverting to Full") {
    val testId = uniqueId("merge_nondelta")
    withParquetSilverTable(testId) { (entity, paths, table) =>
      writeSlice(paths, "data.parquet", Seq(5, 6, 7), testId)
      intercept[NotADeltaTableException] {
        new Processing(entity, "data.parquet").Process(Merge)
      }
      assert(spark.read.table(table).count() === 2)
    }
  }
}
