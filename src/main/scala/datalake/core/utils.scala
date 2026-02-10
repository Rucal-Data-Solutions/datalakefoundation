package datalake.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

object Utils {
  def hasColumn(df: DataFrame, path: String): Boolean = df.columns.contains(path)
}
