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
import org.apache.spark.sql.{ Encoder, Encoders }
import org.json4s.JsonAST.{JField, JObject, JInt, JNull, JValue, JString}
import org.stringtemplate.v4.compiler.STParser.namedArg_return


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
  private var _connectionSettings: DataFrame = _
  private var _environment: DataFrame = _
  private var _watermark: DataFrame = _
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
    _connectionSettings =
      spark.read.jdbc(connectionString, "cfg.EntityConnectionSetting", connectionProperties)
    _environment = spark.read.jdbc(connectionString, "cfg.Environment", connectionProperties)
    _watermark = spark.read.jdbc(connectionString, "cfg.Watermark", connectionProperties)

    _isInitialized = true
  }

  def isInitialized(): Boolean =
    _isInitialized

  def getEntity(id: Int): Option[Entity] = {
    val entityRow = _entities.filter(col("EntityID") === id).collect.headOption

    entityRow match {
      case Some(row) =>
        Some(createEntityFromRow(row))
      case None => None
    }

  }

  private def createEntityFromRow(row: Row): Entity = {
    implicit val environment: Environment = _metadata.getEnvironment
    val id = row.getAs[Int]("EntityID")

    val entitySettings = JObject(
      _entitySettings
        .filter(col("EntityID") === id)
        .collect()
        .map(r =>
          JField(r.getAs[String]("Name"), JString(r.getAs[String]("Value")))
        )
        .toList
    )
    val entityColumns = _entityColumns
      .filter(col("EntityID") === id)
      .collect()
      .map(r =>
        new EntityColumn(
          r.getAs[String]("ColumnName"),
          r.getAs[String]("NewColumnName") match {
            case value: String => Some(value)
            case _ => None
          },
          r.getAs[String]("DataType") match {
            case value: String => Some(value)
            case _ => None
          },
          r.getAs[String]("FieldRoles").toLowerCase().split(",").map(s => s.trim()),
          r.getAs[String]("Expression") match {
            case value: String => Some(value)
            case _ => None
          }
        )
      )
      .toList
    val watermark = _watermark
      .filter(col("EntityID") === id)
      .collect()
      .map(r =>
        new Watermark(
          environment,
          id,
          r.getAs[String]("ColumnName"),
          r.getAs[String]("Operation"),
          r.getAs[Integer]("OperationGroup") match {
            case value: Integer => Some(value)
            case _              => None
          },
          r.getAs[String]("Expression")
        )
      )
      .toList

    new Entity(
      _metadata,
      id,
      row.getAs[String]("EntityName").toLowerCase(),
      row.getAs[String]("EntityDestination") match {
        case null => None
        case "" => None
        case value: String => Some(value)
      },
      row.getAs[Boolean]("EntityEnabled"),
      None,
      row.getAs[Int]("EntityConnectionID").toString,
      row.getAs[String]("EntityProcessType").toLowerCase(),
      watermark,
      entityColumns,
      entitySettings
    )
  }

  def getConnectionEntities(connection: Connection): List[Entity] = {
    _entities
      .filter(col("EntityConnectionID") === connection.Code)
      .collect()
      .map(r => createEntityFromRow(r))
      .toList
  }

  def getConnection(connectionCode: String): Option[Connection] = {
    val connectionRow =
      _connections.filter(col("EntityConnectionID") === connectionCode).collect().headOption

    connectionRow match {
      case Some(row) =>
        Some(
          new Connection(
            _metadata,
            row.getAs[Int]("EntityConnectionID").toString(),
            row.getAs[String]("EntityConnection"),
            Some(row.getAs[Boolean]("EntityConnectionEnabled")),
            getConnectionSettings(row.getAs[Int]("EntityConnectionID"))
          )
        )
      case None => None
    }

  }

  def getConnectionByName(connectionName: String): Option[Connection] = {
    val connectionRow =
      _connections.filter(col("EntityConnection") === connectionName).collect().headOption

    connectionRow match {
      case Some(row) =>
        Some(
          new Connection(
            _metadata,
            row.getAs[Int]("EntityConnectionID").toString(),
            row.getAs[String]("EntityConnection"),
            Some(row.getAs[Boolean]("EntityConnectionEnabled")),
            getConnectionSettings(row.getAs[Int]("EntityConnectionID"))
          )
        )
      case None => None
    }
  }

  private def getConnectionSettings(connectionId: Int): JObject =
    return JObject(_connectionSettings
      .filter(col("EntityConnectionID") === connectionId.toString())
      .collect()
      .map(r => JField(r.getAs[String]("Name"), JString(r.getAs[String]("Value"))))
      .toList)

  private def getEntities(connectionCode: String): List[Entity] =
    _entities
      .filter(col("EntityConnectionID") === connectionCode)
      .collect()
      .flatMap(r => getEntity(r.getAs[Int]("EntityID")))
      .toList

  def getEnvironment: Environment = {
    val environmentRow = _environment.first()
    new Environment(
      name=environmentRow.getAs[String]("name"),
      root_folder=environmentRow.getAs[String]("root_folder"),
      timezone=environmentRow.getAs[String]("timezone"),
      raw_path=environmentRow.getAs[String]("raw_path"),
      bronze_path=environmentRow.getAs[String]("bronze_path"),
      silver_path=environmentRow.getAs[String]("silver_path"),
      secure_container_suffix=environmentRow.getAs[String]("secure_container_suffix")
    )
  }
}
