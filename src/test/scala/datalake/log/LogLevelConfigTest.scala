package datalake.log

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext

class LogLevelConfigTest extends AnyFunSuite with SparkSessionTest {
  test("Log level should be configurable from environment") {
    import spark.implicits._
    
    // Test with INFO level
    val infoEnv = new Environment(
      "TEST",
      "/tmp/test",
      "UTC",
      "raw",
      "bronze", 
      "silver",
      "",
      output_method = "paths",
      log_level = "INFO"
    )
    
    // Reset Log4jConfigurator before test
    val initializedField = Log4jConfigurator.getClass.getDeclaredField("initialized")
    initializedField.setAccessible(true)
    Log4jConfigurator.reset()
    
    // Initialize log4j with INFO level
    Log4jConfigurator.init(spark, Some(infoEnv))
    
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig("datalake")
    
    assert(loggerConfig.getLevel == Level.INFO, "Log level should be set to INFO")
  }
  
  test("Log level should default to WARN when not specified") {
    import spark.implicits._
    
    val defaultEnv = new Environment(
      "TEST",
      "/tmp/test",
      "UTC", 
      "raw",
      "bronze",
      "silver",
      "",
      output_method = "paths"
      // log_level not specified, should default to WARN
    )
    
    // Reset Log4jConfigurator for new test  
    val initializedField2 = Log4jConfigurator.getClass.getDeclaredField("initialized")
    initializedField2.setAccessible(true)
    initializedField2.set(Log4jConfigurator, false)
    
    // Initialize log4j with default level
    Log4jConfigurator.init(spark, Some(defaultEnv))
    
    val ctx = LoggerContext.getContext(false)
    val config = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig("datalake")
    
    assert(loggerConfig.getLevel == Level.WARN, "Log level should default to WARN")
  }
}
