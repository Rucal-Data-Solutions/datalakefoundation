package datalake.metadata

case class EntityColumnFilter(fieldrole: Option[Seq[String]] = None, HasExpression: Option[Boolean] = None)

object EntityColumnFilter{
    def apply(HasExpression: Boolean) = new EntityColumnFilter(fieldrole = None, HasExpression = Some(HasExpression))
    def apply(fieldrole: String*) = new EntityColumnFilter(fieldrole = Some(fieldrole), None)
}