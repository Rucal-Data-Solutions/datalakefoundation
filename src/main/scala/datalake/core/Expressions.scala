package datalake.core

import scala.tools.reflect._
import scala.reflect.runtime._
import java.beans.Expression
import scala.util.Try

case class InvalidEvalParameterException(message: String) extends Exception(message)

abstract class EvalParameter(name: String) {
  def validate: Boolean
  def AsParameterString: String
}

class LiteralEvalParameter(name: String, value: String) extends EvalParameter(name) {
  def validate: Boolean = true

  def AsParameterString: String ={
    val filteredValue = value.toString().replaceAll("[\\r\\n\\\\]", "")
    s"val ${name} = " + "\"" + s"${filteredValue}" + "\""
  }
    
}

object LiteralEvalParameter {

  def apply(name: String, value: String): LiteralEvalParameter = {
    val _param = new LiteralEvalParameter(name, value)
    if (!_param.validate)
      throw new InvalidEvalParameterException("Eval parameter failed validation")
    else _param
  }
}

class ObjectEvalParameter(name: String, obj: String) extends EvalParameter(name) {
  def validate: Boolean = true
  def AsParameterString: String = s"val ${name} = ${obj}"
}

object ObjectEvalParameter {

  def apply(name: String, value: String): ObjectEvalParameter = {
    val _param = new ObjectEvalParameter(name, value)
    if (!_param.validate)
      throw new InvalidEvalParameterException("Eval parameter failed validation")
    else _param
  }
}

class LibraryEvalParameter(library: String) extends EvalParameter(library) {
  def validate: Boolean = true
  def AsParameterString: String = s"import ${library}"
}

object LibraryEvalParameter {

  def apply(library: String): LibraryEvalParameter = {
    val _param = new LibraryEvalParameter(library)
    if (!_param.validate)
      throw new InvalidEvalParameterException("Eval parameter failed validation")
    else _param
  }
}

class Expressions(params: Seq[_ <: EvalParameter]) {

  private val expressionBase: String = {
    val _pars = params.map(_.AsParameterString).mkString(";\n")
    val code = s"""${_pars}\n"""
    code
  }

  def EvaluateExpression(text: String): String = {
    val tb = currentMirror.mkToolBox()
    val code = s"""${expressionBase}\ns"${text}" """

    val result =
      try {
        val parsed_code = tb.parse(code)
        tb.eval(parsed_code).asInstanceOf[String]
      } catch {
        case e: scala.tools.reflect.ToolBoxError =>
          println(s"tb Error: ${e.getMessage()}")
          println("-----------------")
          println(code)
          println("-----------------")
          ""
      }

    result

  }
}
