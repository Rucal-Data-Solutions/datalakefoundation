package datalake.metadata

// import java.io.File
// import scala.reflect.runtime.universe._
// import java.sql.{Connection, DriverManager, ResultSet}
import java.sql._
import java.util.Properties
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.functions.{ col, lit }
import org.apache.arrow.flatbuf.Bool
import org.json4s.JsonAST
import org.apache.spark.sql.{ Encoder, Encoders }

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
  private var _entityColumns: DataFrame = _
  private var _entitySettings: DataFrame = _
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
    _entityColumns = spark.read.jdbc(connectionString, "cfg.EntityColumn", connectionProperties)
    _connections = spark.read.jdbc(connectionString, "cfg.EntityConnection", connectionProperties)
    _entitySettings = spark.read.jdbc(connectionString, "cfg.EntitySetting", connectionProperties)
    _environment = spark.read.jdbc(connectionString, "cfg.Environment", connectionProperties)

    _isInitialized = true
  }

  def isInitialized(): Boolean =
    _isInitialized

  def getEntity(id: Int): Option[Entity] = {
    val entityRow = _entities.filter(col("EntityID") === id).first()
    
    val entitySettings = _entitySettings
      .filter(col("EntityID") === id)
      .collect()
      .map(r => (r.getAs[String]("Name"), r.getAs[String]("Value")))
      .toMap

    val entityColumns = _entityColumns
      .filter(col("EntityID") === id)
      .collect()
      .map(r => 
        new EntityColumn(
          r.getAs[String]("ColumnName"),
          Some(r.getAs[String]("NewColumnName")),
          r.getAs[String]("DataType"),
          r.getAs[String]("FieldRole")
        )
      )
      .toList

    Some(new Entity(
      _metadata,
      entityRow.getAs[Int]("EntityID"),
      entityRow.getAs[String]("EntityName"),
      entityRow.getAs[Boolean]("EntityEnabled"),
      None,
      entityRow.getAs[Int]("EntityConnectionID").toString,
      entityRow.getAs[String]("EntityProcessType"),
      entityColumns,
      JsonAST.JArray(entitySettings.toList.map(t => JsonAST.JString(t._1 + ":" + t._2)))
    ))
  }

  def getConnection(connectionCode: String): Option[Connection] = {
    val connectionRow = _connections.filter(col("EntityConnectionID") === connectionCode).first()

    Some(new Connection(
      _metadata,
      connectionRow.getAs[Int]("EntityConnectionID").toString(),
      connectionRow.getAs[String]("EntityConnection"),
      Some(connectionRow.getAs[Boolean]("EntityConnectionEnabled")),
      Map.empty[String, Any], // Settings can be fetched similar to entity settings
      getEntities(connectionRow.getAs[Int]("EntityConnectionID").toString)
    ))
  }

  private def getEntities(connectionCode: String): List[Entity] = {
    _entities
      .filter(col("EntityConnectionID") === connectionCode)
      .collect()
      .flatMap(r => getEntity(r.getAs[Int]("EntityID")))
      .toList
  }

  def getEnvironment: Environment = {
    val environmentRow = _environment.first()
    new Environment(
      environmentRow.getAs[String]("name"),
      environmentRow.getAs[String]("root_folder"),
      environmentRow.getAs[String]("timezone")
    )
  }
}

