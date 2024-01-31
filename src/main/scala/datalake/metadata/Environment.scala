package datalake.metadata

import datalake.core._
import datalake.processing._

import java.util.TimeZone
import scala.util.Try
import scala.reflect.runtime._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._

class Environment(
    name: String,
    root_folder: String,
    timezone: String,
    raw_path: String,
    bronze_path: String,
    silver_path: String,
    secure_container_suffix: String
) extends Serializable {

  override def toString(): String = s"Environment: ${this.name}"

  def Name: String =
    this.name

  def RootFolder: String = {
    val folder = this.root_folder
    if (folder.endsWith("/")) folder.dropRight(1) else folder
  }

  def Timezone: TimeZone =
    TimeZone.getTimeZone(timezone)

  /** Returns the default path used in raw processing.
    *
    * @return
    *   The default relative path, without a leading or trailing '/' if necessary.
    */
  def RawPath: String = {
    var rawPath = this.raw_path
    if (rawPath.startsWith("/")) rawPath = rawPath.drop(1)
    if (rawPath.endsWith("/")) rawPath = rawPath.dropRight(1)
    rawPath
  }

  /** Returns the default path used in bronze processing.
    *
    * @return
    *   The default relative path, without a leading or trailing '/' if necessary.
    */
  def BronzePath: String = {
    var bronzePath = this.bronze_path
    if (bronzePath.startsWith("/")) bronzePath = bronzePath.drop(1)
    if (bronzePath.endsWith("/")) bronzePath = bronzePath.dropRight(1)
    bronzePath
  }

  /** Returns the default path used in silver processing.
    *
    * @return
    *   The default relative path, without a leading or trailing '/' if necessary.
    */
  def SilverPath: String = {
    var silverPath = this.silver_path
    if (silverPath.startsWith("/")) silverPath = silverPath.drop(1)
    if (silverPath.endsWith("/")) silverPath = silverPath.dropRight(1)
    silverPath
  }
}
