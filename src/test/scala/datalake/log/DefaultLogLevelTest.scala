package datalake.log

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._

class DefaultLogLevelTest extends AnyFunSuite with SparkSessionTest {
  test("Default log level should be WARN when not specified in metadata") {
    import spark.implicits._
    
    // Create metadata configuration without specifying log_level
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
        "name": "test_default",
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
      "entities": []
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create metadata instance - should default to WARN level
    implicit val metadata = new Metadata(metadataSettings)
    
    // Verify the default log level is WARN
    assert(metadata.getEnvironment.LogLevel == "WARN", "Default log level should be WARN")
  }
}
