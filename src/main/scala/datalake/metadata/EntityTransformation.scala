package datalake.metadata

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{ JField, JObject, JInt, JNull, JValue, JString }
import org.json4s.JsonAST.JArray


final class EntityTransformation(Expressions: List[String]) extends Serializable{
    private val _expressions: List[String] = Expressions

    override def toString(): String = {
      _expressions.toString()
    }

    def expressions: List[String] = {
        _expressions
    }

}

class EntityTransformationSerializer(metadata: Metadata)
    extends CustomSerializer[EntityTransformation](implicit formats =>
      (
        { case j: JArray => 
            new EntityTransformation( (j).extract[List[String]]  )
          case j: JString =>
            new EntityTransformation(List(j.extract[String]))
        },
        PartialFunction.empty
      )
    )