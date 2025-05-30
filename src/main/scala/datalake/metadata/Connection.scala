package datalake.metadata

import datalake.core._
import datalake.processing._

import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._

import scala.util.Try
import scala.reflect.runtime._

import org.json4s.JsonAST
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JField, JObject, JInt, JNull, JValue, JString}



class Connection(
    metadata: datalake.metadata.Metadata,
    code: String,
    name: String,
    enabled: Option[Boolean],
    val settings: JObject
) {

  override def toString(): String =
    this.name
  
  def Code: String =
    this.code

  def Name: String =
    this.name.toLowerCase()

  def isEnabled: Boolean =
    this.enabled.getOrElse(true)

  def Settings: Map[String, Any] =
    this.settings.values

  def getSettingAs[T](name: String): T = {
    val setting = this.settings.values.get(name)

    setting match {
      case Some(value) => value.asInstanceOf[T]
      case None        => None.asInstanceOf[T]
    }
  }

}

class ConnectionSerializer(metadata: datalake.metadata.Metadata)
    extends CustomSerializer[Connection](implicit formats =>
      (
        { case j: JObject =>
          new Connection(
            metadata = metadata,
            code = (j \ "name").extract[String],
            name = (j \ "name").extract[String].toLowerCase(),
            enabled = (j \ "enabled").extractOpt[Boolean],
            settings = (j \ "settings").extract[JObject]
          )
        },
        { case _: Connection =>
          JObject()
        }
      )
    )