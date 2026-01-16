package datalake.metadata

import org.scalatest.funsuite.AnyFunSuite
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.FieldSerializer
import org.json4s.jackson.Serialization.{read, write}
class IOOutputSpec extends AnyFunSuite with SparkSessionTest {
  
  test("Entity with default settings should return Paths") {
    // Create test metadata with default paths setting
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
        "name": "test",
        "root_folder": "/data",
        "timezone": "UTC",
        "raw_path": "raw",
        "bronze_path": "bronze",
        "silver_path": "silver",
        "secure_container_suffix": "",
        "io_output": "paths"
      },
      "connections": [
        {
          "name": "test_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": []
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create a metadata instance with the test settings
    implicit val metadata = new Metadata(metadataSettings)

    // Create a basic entity with default settings (no io_output override)
    val entity = new Entity(
      metadata = metadata,
      id = 1,
      name = "test_entity",
      group = Some("test_group"),
      destination = None,
      enabled = true,
      secure = None,
      connection = "test_connection",
      processtype = "full",
      watermark = Array(),
      columns = Array(),
      settings = JObject(List()),
      transformations = Array()
    )

    // Assert that the entity returns Output with PathLocations
    entity.getOutput shouldBe a [Output]

    val output = entity.getOutput
    output.bronze shouldBe a [PathLocation]
    output.silver shouldBe a [PathLocation]
    
    // Verify that getPaths works correctly
    val paths = entity.getPaths
    paths.rawpath should startWith("/data/raw")
    paths.bronzepath should startWith("/data/bronze")
    paths.silverpath should startWith("/data/silver")
  }

  test("Entity with catalog settings should return CatalogTables") {
    // Create test metadata with catalog setting
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
        "name": "test",
        "root_folder": "/data",
        "timezone": "UTC",
        "raw_path": "raw",
        "bronze_path": "bronze",
        "silver_path": "silver",
        "secure_container_suffix": "",
        "output_method": "catalog"
      },
      "connections": [
        {
          "name": "test_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": []
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create a metadata instance with the test settings
    implicit val metadata = new Metadata(metadataSettings)

    // Create an entity with catalog settings
    val entity = new Entity(
      metadata = metadata,
      id = 2,
      name = "test_entity",
      group = Some("test_group"),
      destination = None,
      enabled = true,
      secure = None,
      connection = "test_connection",
      processtype = "full",
      watermark = Array(),
      columns = Array(),
      settings = JObject(List(
        JField("bronze_table", JString("custom_bronze")),
        JField("silver_table", JString("custom_silver"))
      )),
      transformations = Array()
    )

    // Assert that the entity returns TableLocation
    entity.getOutput shouldBe a[Output]
    val tables = entity.getOutput.asInstanceOf[Output]
    tables.bronze shouldBe TableLocation("custom_bronze")
    tables.silver shouldBe TableLocation("custom_silver")
  }

  test("Entity can override environment output_method setting") {
    // Create test metadata with paths as default
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
      "name": "test",
      "root_folder": "/data",
      "timezone": "UTC",
      "raw_path": "raw",
      "bronze_path": "bronze",
      "silver_path": "silver",
      "secure_container_suffix": "",
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
        "id": 3,
        "name": "test_entity",
        "group": "test_group",
        "enabled": true,
        "connection": "test_connection",
        "processtype": "full",
        "watermark": [],
        "columns": [],
        "settings": {
          "bronze_path": "test__${connection}.${entity}",
          "silver_table": "silver__${connection}.${destination}"
        },
        "transformations": []
      }
      ]
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create a metadata instance with the test settings
    implicit val metadata = new Metadata(metadataSettings)

    // Create an entity that overrides to use catalog
    val entity = metadata.getEntity(3)

    // Assert that the entity returns CatalogTables despite environment setting
    entity.getOutput shouldBe a[Output]
    val tables = entity.getOutput.asInstanceOf[Output]
    tables.bronze shouldBe PathLocation("/data/bronze/test__test_connection.test_entity")
    tables.silver shouldBe TableLocation("silver__test_connection.test_entity")
  }
}
