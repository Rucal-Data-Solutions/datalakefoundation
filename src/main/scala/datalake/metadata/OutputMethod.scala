package datalake.metadata


sealed trait OutputMethod extends Serializable


final case class Paths(rawpath: String, bronzepath: String, silverpath: String) extends OutputMethod


sealed trait OutputLocation extends Serializable
final case class PathLocation(path: String) extends OutputLocation
final case class TableLocation(table: String) extends OutputLocation

final case class Output(
     rawpath: String,
     bronze: OutputLocation,
     silver: OutputLocation)
     extends OutputMethod
