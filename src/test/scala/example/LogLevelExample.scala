package example

import datalake.metadata._
import datalake.log.DatalakeLogManager
import org.apache.spark.sql.SparkSession

/**
 * Example demonstrating how to use configurable log levels in the Data Lake Foundation library.
 * 
 * This example shows how to:
 * 1. Configure log levels through metadata JSON
 * 2. Create environments with different log levels
 * 3. Use the DatalakeLogManager with environment-specific logging
 */
object LogLevelExample {
  
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Log Level Example")
      .master("local[*]")
      .getOrCreate()
    
    try {
      exampleWithInfoLevel()
      exampleWithWarnLevel()
      exampleWithDefaultLevel()
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Example: Configure log level through metadata JSON
   */
  def exampleWithInfoLevel()(implicit spark: SparkSession): Unit = {
    println("=== Example 1: INFO Log Level via Metadata ===")
    
    // Create metadata configuration with INFO log level
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
        "name": "example",
        "root_folder": "/data",
        "timezone": "UTC",
        "raw_path": "raw",
        "bronze_path": "bronze",
        "silver_path": "silver", 
        "secure_container_suffix": "",
        "output_method": "paths",
        "log_level": "INFO"
      },
      "connections": [
        {
          "name": "example_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": []
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create metadata instance - this will initialize logging with INFO level
    implicit val metadata = new Metadata(metadataSettings)
    
    // Get logger - will be configured with INFO level from environment
    val logger = DatalakeLogManager.getLogger(this.getClass, metadata.getEnvironment)
    
    println(s"Configured log level: ${metadata.getEnvironment.LogLevel}")
    
    // These log messages will be shown based on the configured level
    logger.info("This INFO message will be shown (log level is INFO)")
    logger.warn("This WARN message will be shown (log level is INFO)")
    logger.error("This ERROR message will be shown (log level is INFO)")
    
    // This DEBUG message will NOT be shown because log level is INFO
    logger.debug("This DEBUG message will NOT be shown (log level is INFO)")
    
    println()
  }
  
  /**
   * Example: Create environment with WARN log level directly
   */
  def exampleWithWarnLevel()(implicit spark: SparkSession): Unit = {
    println("=== Example 2: WARN Log Level via Environment ===")
    
    // Create environment with WARN log level directly
    val environment = new Environment(
      name = "example_warn",
      root_folder = "/data",
      timezone = "UTC", 
      raw_path = "raw",
      bronze_path = "bronze",
      silver_path = "silver",
      secure_container_suffix = Some(""),
      output_method = "paths",
      log_level = "WARN"
    )
    
    // Get logger configured with WARN level
    val logger = DatalakeLogManager.getLogger(this.getClass, environment)
    
    println(s"Configured log level: ${environment.LogLevel}")
    
    // Only WARN and higher level messages will be shown
    logger.warn("This WARN message will be shown")
    logger.error("This ERROR message will be shown")
    
    // These will NOT be shown because log level is WARN
    logger.info("This INFO message will NOT be shown")
    logger.debug("This DEBUG message will NOT be shown")
    
    println()
  }
  
  /**
   * Example: Default log level behavior
   */
  def exampleWithDefaultLevel()(implicit spark: SparkSession): Unit = {
    println("=== Example 3: Default Log Level (WARN) ===")
    
    // Create metadata configuration without specifying log_level
    val metadataSettings = new StringMetadataSettings()
    val configJson = """
    {
      "environment": {
        "name": "default_example",
        "root_folder": "/data",
        "timezone": "UTC",
        "raw_path": "raw",
        "bronze_path": "bronze",
        "silver_path": "silver", 
        "secure_container_suffix": "",
        "output_method": "paths"
      },
      "connections": [],
      "entities": []
    }
    """
    metadataSettings.initialize(configJson)
    
    // Create metadata instance - should default to WARN level
    implicit val metadata = new Metadata(metadataSettings)
    
    // Get logger with default configuration
    val logger = DatalakeLogManager.getLogger(this.getClass, metadata.getEnvironment)
    
    println(s"Default log level: ${metadata.getEnvironment.LogLevel}")
    
    // Only WARN and higher level messages will be shown (default behavior)
    logger.warn("This WARN message will be shown (default level)")
    logger.error("This ERROR message will be shown (default level)")
    
    // These will NOT be shown because default level is WARN
    logger.info("This INFO message will NOT be shown (default level)")
    logger.debug("This DEBUG message will NOT be shown (default level)")
    
    println()
  }
}
