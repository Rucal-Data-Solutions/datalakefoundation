package datalake.metadata

import scala.tools.reflect._
import scala.reflect.runtime._
import datalake.core._

class Watermark(
    environment: Environment,
    entity_id: Integer,
    column_name: String,
    operation: String,
    operation_group: Option[Integer],
    function: String
){

  override def toString(): String =
    s"${operation} ${column_name} > ${Function}"

  def Function: String = {
    val params = Watermark.GetWatermarkParams(entity_id, column_name, environment)
    Utils.EvaluateText(this.function, params)
  }
    

  def Column_Name: String =
    column_name
}

object Watermark {

  private def GetWatermarkParams(entity_id: Integer, column_name: String, environment:Environment): Map[String, String] = {
    implicit val env: Environment = environment
    val wmd = new WatermarkData
    val lastvalue = wmd.getLastValue(entity_id, column_name)

    lastvalue match {
      case Some(lv) => Map(("last_value", lv))
      case None => Map.empty[String, String]
    }
  }
}
