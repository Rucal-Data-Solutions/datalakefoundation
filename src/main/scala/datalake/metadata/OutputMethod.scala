package datalake.metadata


sealed trait OutputMethod extends Serializable


final case class Paths(rawpath: String, bronzepath: String, silverpath: String) extends OutputMethod


sealed trait OutputLocation extends Serializable {
  def value: String
}
final case class PathLocation(path: String) extends OutputLocation {
  override def value: String = path
}
final case class TableLocation(table: String) extends OutputLocation {
  override def value: String = table
}

final case class Output(
     rawpath: String,
     bronze: OutputLocation,
     silver: OutputLocation)
     extends OutputMethod
