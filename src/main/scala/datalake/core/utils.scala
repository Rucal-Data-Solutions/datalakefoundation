package datalake.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Try
import scala.tools.reflect._
import scala.reflect.runtime._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

class Utils(spark: SparkSession){
  
  private def createSprakTable(name: String, path: String): Unit={
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
    spark.sql(f"DROP TABLE IF EXISTS silver.${name}")
    spark.sql(f"CREATE TABLE silver.${name} USING DELTA LOCATION '${path}'")
  }
}
object Utils {
  def apply(implicit spark: SparkSession):Utils ={
    new Utils(spark)
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def EvaluateText(text: String, params: Map[String, String]): String = {
    val tb = currentMirror.mkToolBox()
    val libs = ""
    val vals = params.map(p => s"val ${p._1} = ${p._2}").mkString(";\n")
    val code = s"""${libs}
                    ${vals}
                    s"${text}" 
                    """

    println(code)

    tb.eval(tb.parse(code)).asInstanceOf[String]
  }

}

object FileOperations {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  // Create Hadoop Configuration from Spark
  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def remove(path: Path, force: Boolean):Unit ={
    // To Delete File
    if (fs.exists(path) && fs.isFile(path))
      fs.delete(path, force)

    // To Delete Directory
    if (fs.exists(path) && fs.isDirectory(path))
      fs.delete(path, force)

  }

  def remove(path: String, force: Boolean):Unit = {
    val pth = new Path(path)
    remove(pth, force)
  }

  def exists(path: String): Boolean={
    val _path = new Path(path)
    fs.exists(_path)
  }

}
