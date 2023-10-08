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

    params match {
      case Some(eval_pars) => Utils.EvaluateText(this.function, eval_pars)
      case None => this.function
    }
    
  }

  def Column_Name: String =
    column_name
}

object Watermark {

  private def GetWatermarkParams(entity_id: Integer, column_name: String, environment:Environment): Option[Map[String, String]] = {
    implicit val env: Environment = environment
    val wmd = new WatermarkData
    val lastvalue = wmd.getLastValue(entity_id, column_name)

    lastvalue match {
      case Some(value) => Some(Map(("last_value", value)))
      case None => None
    }
    
  }
}
