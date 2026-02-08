package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import datalake.core.DatalakeException

class CalculatedColumnSpec extends AnyFunSuite with SparkSessionTest {

  test("Broken calculated column expression should throw DatalakeException") {
    import spark.implicits._

    val testId = s"calc_col_fail_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (CALC COL TEST)",
        "timezone": "Europe/Amsterdam",
        "root_folder": "${testBasePath.replace("\\", "/")}",
        "raw_path": "/$${connection}/$${entity}",
        "bronze_path": "/$${connection}/$${entity}",
        "silver_path": "/$${connection}/$${destination}",
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
          "id": 1,
          "name": "calc_test_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "merge",
          "watermark": [],
          "columns": [
            {
              "name": "id",
              "fieldroles": ["businesskey"]
            },
            {
              "name": "",
              "newname": "broken_col",
              "fieldroles": ["calculated"],
              "expression": "INVALID_FUNCTION_THAT_DOES_NOT_EXIST(id)"
            }
          ],
          "settings": {},
          "transformations": []
        }
      ]
    }
    """

    val settings = new StringMetadataSettings()
    settings.initialize(testMetadataJson)
    val metadata = new Metadata(settings)
    val testEntity = metadata.getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    val testData = Seq(
      (1, "Alice", testId),
      (2, "Bob", testId)
    ).toDF("id", "name", "test_id")

    val slice = s"calc_fail_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"$bronzePath/$slice")

    val proc = new Processing(testEntity, slice)

    val ex = intercept[DatalakeException] {
      proc.Process(Merge)
    }

    assert(ex.getMessage.contains("broken_col"), "Error should mention the column name")
    assert(ex.getMessage.contains("INVALID_FUNCTION_THAT_DOES_NOT_EXIST"), "Error should mention the failing expression")
  }

  test("Valid calculated column expression should succeed") {
    import spark.implicits._

    val testId = s"calc_col_ok_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val testMetadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (CALC COL TEST)",
        "timezone": "Europe/Amsterdam",
        "root_folder": "${testBasePath.replace("\\", "/")}",
        "raw_path": "/$${connection}/$${entity}",
        "bronze_path": "/$${connection}/$${entity}",
        "silver_path": "/$${connection}/$${destination}",
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
          "id": 1,
          "name": "calc_ok_entity",
          "enabled": true,
          "connection": "test_connection",
          "processtype": "merge",
          "watermark": [],
          "columns": [
            {
              "name": "id",
              "fieldroles": ["businesskey"]
            },
            {
              "name": "",
              "newname": "doubled_id",
              "datatype": "integer",
              "fieldroles": ["calculated"],
              "expression": "id * 2"
            }
          ],
          "settings": {},
          "transformations": []
        }
      ]
    }
    """

    val settings = new StringMetadataSettings()
    settings.initialize(testMetadataJson)
    val metadata = new Metadata(settings)
    val testEntity = metadata.getEntity(1)
    val ioLocations = testEntity.getOutput
    val bronzePath = ioLocations.bronze.asInstanceOf[PathLocation].path

    val testData = Seq(
      (1, "Alice", testId),
      (2, "Bob", testId)
    ).toDF("id", "name", "test_id")

    val slice = s"calc_ok_${testId}.parquet"
    testData.write.mode("overwrite").parquet(s"$bronzePath/$slice")

    val proc = new Processing(testEntity, slice)
    proc.Process(Merge)

    val result = ioLocations.silver match {
      case pathLoc: PathLocation => spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation => spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }

    assert(result.count() === 2, "Should have 2 records")
    val row1 = result.filter($"id" === 1).first()
    assert(row1.getAs[Int]("doubled_id") === 2, "Calculated column should be id * 2")
  }
}
