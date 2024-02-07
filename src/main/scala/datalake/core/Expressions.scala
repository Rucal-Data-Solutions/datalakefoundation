package datalake.core

import scala.tools.reflect._
import scala.reflect.runtime._
import java.beans.Expression

case class InvalidEvalParameterException(message: String) extends Exception(message)

abstract class EvalParameter(name: String){
    def validate: Boolean
    def AsParameterString: String
}
class LiteralEvalParameter(name: String, value: String) extends EvalParameter(name){
    def validate: Boolean = true
    def AsParameterString: String = {
        s"val ${name} = " + "\"" + s"${value.toString()}" + "\""
    }
}
object LiteralEvalParameter{
    def apply(name: String, value: String): LiteralEvalParameter={
        val _param = new LiteralEvalParameter(name, value)
        if (!_param.validate) throw new InvalidEvalParameterException("Eval parameter failed validation") 
        else _param
    }
}

class ObjectEvalParameter(name: String, obj: String) extends EvalParameter(name){
    def validate: Boolean = true
    def AsParameterString: String = s"val ${name} = ${obj}"
}
object ObjectEvalParameter{
    def apply(name: String, value: String): ObjectEvalParameter={
        val _param = new ObjectEvalParameter(name, value)
        if (!_param.validate) throw new InvalidEvalParameterException("Eval parameter failed validation") 
        else _param
    }
}

class LibraryEvalParameter(library: String) extends EvalParameter(library){
    def validate: Boolean = true
    def AsParameterString: String = s"import ${library}"
}
object LibraryEvalParameter{
    def apply(library: String): LibraryEvalParameter={
        val _param = new LibraryEvalParameter(library)
        if (!_param.validate) throw new InvalidEvalParameterException("Eval parameter failed validation") 
        else _param    
    }
}


class Expressions(params: Seq[_ <: EvalParameter]) {
    private val expressionBase: String={
        val _pars = params.map(_.AsParameterString).mkString(";\n")
        val code = s"""${_pars}\n"""
        code
    }

    def EvaluateExpression(text: String): String = {
    val tb = currentMirror.mkToolBox()
    val code = s"""${expressionBase}
                    s"${text}" 
                    """
    val parsed_code = tb.parse(code)
    // println(parsed_code)

    tb.eval(parsed_code).asInstanceOf[String]
  }
}

