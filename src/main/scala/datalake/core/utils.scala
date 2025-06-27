package datalake.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

// class Utils(spark: SparkSession){
  
//   private def createSparkTable(name: String, path: String): Unit={
//     spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
//     spark.sql(f"DROP TABLE IF EXISTS silver.${name}")
//     spark.sql(f"CREATE TABLE silver.${name} USING DELTA LOCATION '${path}'")
//   }

  
// }
object Utils {
  // def apply(implicit spark: SparkSession):Utils ={
  //   new Utils(spark)
  // }

  def hasColumn(df: DataFrame, path: String): Boolean = df.columns.contains(path)
}