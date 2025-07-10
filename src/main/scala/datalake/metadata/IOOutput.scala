package datalake.metadata

sealed trait IOOutput extends Serializable

case class Paths(rawpath: String, bronzepath: String, silverpath: String) extends IOOutput

case class CatalogTables(bronzeTable: String, silverTable: String) extends IOOutput
