package datalake.log

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import datalake.metadata.Environment

object DatalakeLogManager {
  def getLogger(cls: Class[_])(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark)
    LogManager.getLogger(cls)
  }
  
  def getLogger(cls: Class[_], environment: Environment)(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark, Some(environment))
    LogManager.getLogger(cls)
  }
}