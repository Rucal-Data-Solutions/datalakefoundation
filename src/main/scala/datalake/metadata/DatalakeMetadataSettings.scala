package datalake.metadata

import datalake.processing._
import datalake.utils._
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST
import scala.tools.cmd.Meta
import org.apache.arrow.flatbuf.Bool

trait DatalakeMetadataSettings {
  type initParam
  def initialize(initParameter: initParam)
  def isInitialized: Boolean
  def setMetadata(metadata: Metadata): Unit
  def getEntity(id: Int): Option[Entity]
  def getConnection(connectionCode: String): Option[Connection]
  def getConnectionByName(connectionName: String): Option[Connection]
  def getEnvironment: Environment
}