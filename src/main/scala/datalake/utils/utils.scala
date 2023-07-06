package datalake.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Try
import scala.tools.reflect._
import scala.reflect.runtime._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

object Utils {
  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def EvaluateText(value: String, params: Map[String, String]): String = {
    val tb = currentMirror.mkToolBox()
    val today = params.getOrElse("today", "")
    val entity = params.getOrElse("entity", "")

    val code = s""" val today = s"${today}"
                    val entity = s"${entity}"
                    s"${value}" 
                """

    tb.eval(tb.parse(code)).asInstanceOf[String]
  }

}

object FileOperations {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  // Create Hadoop Configuration from Spark
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def remove(path: Path, force: Boolean) {
    // To Delete File
    if (fs.exists(path) && fs.isFile(path))
      fs.delete(path, force)

    // To Delete Directory
    if (fs.exists(path) && fs.isDirectory(path))
      fs.delete(path, force)

  }

  def remove(path: String, force: Boolean) {
    val pth = new Path(path)
    remove(pth, force)
  }

  def exists(path: String): Boolean={
    val _path = new Path(path)
    fs.exists(_path)
  }
}
