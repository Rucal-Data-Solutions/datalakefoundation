package datalake.metadata

import scala.tools.reflect._
import scala.reflect.runtime._
import datalake.core._
import datalake.core.Utils._

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{ JField, JObject, JInt, JNull, JValue, JString }
import org.joda.time.DateTime

class Watermark(
    environment: Environment,
    entity_id: Integer,
    column_name: String,
    operation: String,
    operation_group: Option[Integer],
    expression: String
) extends Serializable {
  implicit val env: Environment = environment
  val wmd = new WatermarkData(entity_id)

  override def toString(): String =
    s"${operation} ${column_name} > ${Function}"

  final def Expression: String = expression

  final def Value: Option[String] = {
    val _value = wmd.getLastValue(column_name) match {
      case Some(watermark_value) =>
        try {
          val _params = Watermark.GetWatermarkParams(wmd, watermark_value)
          val _expressions = new Expressions(_params)
          Some(_expressions.EvaluateExpression(this.expression))
        } catch {
          case e: Exception =>
            None
        }
      case None => None
    }
    return _value
  }

  final def Column_Name: String =
    column_name

  final def Operation: String =
    operation

  final def OperationGroup: Option[Integer] =
    operation_group

  final def Reset: Unit =
    wmd.Reset(this)
}

object Watermark {

  private def GetWatermarkParams(wmd: WatermarkData, value: WatermarkValue): Seq[EvalParameter] = {
    
    val _libs = Seq(
      LibraryEvalParameter("java.time.{LocalDate, LocalDateTime, LocalTime}"),
      LibraryEvalParameter("java.time.format.DateTimeFormatter")
    )
    val _objects = Seq(
      ObjectEvalParameter("defaultFormat", "DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.S\")")
    )
    val _literals =
      Seq(LiteralEvalParameter("watermark", s"${value.value}"))
    val _aliasses = Seq(ObjectEvalParameter("last_value", "watermark"))

    return _libs ++ _objects ++ _literals ++ _aliasses

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
            JField("value", JString(wm.Value.getOrElse("None")))
          )
        }
      )
    )
