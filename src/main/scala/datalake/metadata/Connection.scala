package datalake.metadata

import datalake.processing._
import datalake.core._
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

class Connection(
    metadata: Metadata,
    code: String,
    name: String,
    enabled: Option[Boolean],
    settings: Map[String, Any],
    entities: List[Entity]
) {

  override def toString(): String =
    this.name
  
  def Code: String =
    this.code

  def Name: String =
    this.name.toLowerCase()

  def isEnabled: Boolean =
    this.enabled.getOrElse(true)

  def getSettings: Map[String, Any] =
    this.settings

  def getSettingAs[T](name: String): T = {
    val setting = this.settings.get(name)

    setting match {
      case Some(value) => value.asInstanceOf[T]
      case None        => None.asInstanceOf[T]
    }
  }
  def getEntities: List[Entity] = {
    this.entities
  } 
}