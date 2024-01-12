package datalake.metadata

import scala.tools.reflect._
import scala.reflect.runtime._
import datalake.core._
import datalake.core.Utils._

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JField, JObject, JInt, JNull, JValue, JString}
import org.joda.time.DateTime

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
    val _params = Watermark.GetWatermarkParams(entity_id, column_name, environment)
    val _expressions = new Expressions(_params)

    _expressions.EvaluateExpression(this.expression)
  }

  final def Column_Name: String =
    column_name

  final def Operation: String = 
    operation

  final def OperationGroup: Option[Integer] =
    operation_group

}

object Watermark {
  private def GetWatermarkParams(entity_id: Integer, column_name: String, environment:Environment): Set[EvalParameter] = {
    implicit val env: Environment = environment
    val wmd = new WatermarkData
    val _libs = Set(LibraryEvalParameter("java.time.{LocalDate, LocalDateTime}"), LibraryEvalParameter("java.time.format.DateTimeFormatter"))
    val _objects = Set(ObjectEvalParameter("defaultFormat", "DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.S\")"))
    val _literals = Set(LiteralEvalParameter("last_value", wmd.getLastValue(entity_id, column_name).getOrElse("None")))


    _libs ++ _objects ++ _literals
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
