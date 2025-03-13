package datalake.metadata

import java.sql._
import java.util.Properties

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.functions.{ col, lit }
import org.apache.arrow.flatbuf.Bool
import org.apache.spark.sql.{ Encoder, Encoders }

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.microsoft.sqlserver.jdbc.{SQLServerException}

case class SqlServerSettings(
    server: String,
    port: Int,
    database: String,
    username: String,
    password: String
)

class SqlMetadataSettings extends DatalakeMetadataSettings {

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def initialize(settings: SqlServerSettings): Unit = {
    val connectionString =
      s"jdbc:sqlserver://${settings.server}:${settings.port};database=${settings.database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

    try {
      val _configJson = spark.read
        .format("jdbc")
        .option("url", connectionString)
        .option("user", settings.username)
        .option("password", settings.password)
        .option("query", "SELECT config FROM cfg.fnGetFoundationConfig()")
        .load()

      val _jsonString = _configJson.head.apply(0).toString()
      super.initialize(_jsonString.asInstanceOf[ConfigString])
    }
    catch {
      case e: Exception => {
        println(s"Failed to initialize metadata, Message: ${e.getMessage()}")
        logger.error(e)
        throw e
      }
    }

  }
}

object SqlMetadataSettings{
  def apply(settings: SqlServerSettings): SqlMetadataSettings ={
    val _metadataSettings = new SqlMetadataSettings
    _metadataSettings.initialize(settings)

    return _metadataSettings
  }
}