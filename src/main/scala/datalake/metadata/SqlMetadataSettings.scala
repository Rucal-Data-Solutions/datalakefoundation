package datalake.metadata

// import java.io.File
// import scala.reflect.runtime.universe._
// import java.sql.{Connection, DriverManager, ResultSet}
import java.sql._
import java.util.Properties
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.functions.{ col, lit }

case class SqlServerSettings(
    server: String,
    port: Int,
    database: String,
    username: String,
    password: String
)

class SqlMetadataSettings extends DatalakeMetadataSettings {
  private var _isInitialized: Boolean = false
  private var _connections: DataFrame = _
  private var _entities: DataFrame = _
  private var _environment: DataFrame = _
  private var _metadata: Metadata = _

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._

  def setMetadata(metadata: Metadata) {
    _metadata = metadata
  }

  type initParam = SqlServerSettings

  def initialize(initParameter: initParam): Unit = {
    val connectionString =
      s"jdbc:sqlserver://${initParameter.server}:${initParameter.port};database=${initParameter.database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;selectMethod=cursor;"

    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${initParameter.username}")
    connectionProperties.put("password", s"${initParameter.password}")

    _entities = spark.read.jdbc(connectionString, "cfg.Entity", connectionProperties)
    _connections = spark.read.jdbc(connectionString, "cfg.EntityConnection", connectionProperties)
    _environment = spark.read.jdbc(connectionString, "cfg.Environment", connectionProperties)

    _isInitialized = true
  }

  def isInitialized(): Boolean =
    _isInitialized

  def getEntity(id: Int): Option[Entity] = {
    throw new NotImplementedException()
    // val _entity = _entities
    //   .filter(col("EntityID") === id)
    //   .as[Entity]
    
    //   Some(_entity)
  }


  def getConnection(name: String): Option[Connection] =
    throw new NotImplementedException()

  def getEnvironment(): Environment =
    throw new NotImplementedException()

}
