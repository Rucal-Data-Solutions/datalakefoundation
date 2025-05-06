package datalake.log

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object DatalakeLogManager {
  def getLogger(cls: Class[_])(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark)
    LogManager.getLogger(cls)
  }
}