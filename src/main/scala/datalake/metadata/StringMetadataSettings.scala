package datalake.metadata

/** A metadata settings implementation that takes a JSON string directly.
  * This implementation is useful for testing and for cases where the configuration
  * is already available as a string rather than from a file or database.
  */
class StringMetadataSettings extends DatalakeMetadataSettings {
  type ConfigString = String

  /** Initialize the metadata settings with a JSON configuration string.
    *
    * @param config A JSON string containing the complete configuration
    */
  override def initialize(config: String): Unit = {
    super.initialize(config)
  }
}

object StringMetadataSettings {
  def apply(config: String): StringMetadataSettings = {
    val settings = new StringMetadataSettings
    settings.initialize(config)
    settings
  }
}
