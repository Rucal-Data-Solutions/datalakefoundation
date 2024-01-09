package datalake.metadata

import scala.tools.reflect._
import scala.reflect.runtime._
import datalake.core._
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JField, JObject, JInt, JNull, JValue, JString}

class Watermark(
    environment: Environment,
    entity_id: Integer,
    column_name: String,
    operation: String,
    operation_group: Option[Integer],
    expression: String
){

  override def toString(): String =
    s"${operation} ${column_name} > ${Function}"

  final def Expression: String = expression
  
  final def Value: String = {
    val params = Watermark.GetWatermarkParams(entity_id, column_name, environment)
    Utils.EvaluateText(this.expression, params)
  }

  final def Column_Name: String =
    column_name

  final def Operation: String = 
    operation

  final def OperationGroup: Option[Integer] =
    operation_group

}

object Watermark {

  private def GetWatermarkParams(entity_id: Integer, column_name: String, environment:Environment): Map[String, String] = {
    implicit val env: Environment = environment
    val wmd = new WatermarkData

    val lastvalue = ("last_value", wmd.getLastValue(entity_id, column_name).getOrElse("None"))
    val date_obj = ("date", "\"new java.text.SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.S\")\"")
    
    Map(date_obj, lastvalue)
  }
}

class WatermarkSerializer(metadata: Metadata)
    extends CustomSerializer[Watermark](implicit formats =>
      (
        { case j: JObject =>
          new Watermark(
            metadata.getEnvironment,
            (j \ "entity_id").extract[Integer],
            (j \ "column_name").extract[String],
            (j \ "operation").extract[String],
            (j \ "operation_group").extract[Option[Integer]],
            (j \ "expression").extract[String]
          )
        },
        { case wm: Watermark =>
          JObject(
            JField("column_name", JString(wm.Column_Name)),
            JField("operation", JString(wm.Operation)),
            JField(
              "operation_group",
              wm.OperationGroup match {
                case Some(value) => JInt(BigInt(value))
                case None        => JNull
              }
            ),
            JField("expression", JString(wm.Expression)),
            JField("value", JString(wm.Value))
          )
        }
      )
    )
