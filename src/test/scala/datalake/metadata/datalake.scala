package datalake.metadata

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SQLContext, SparkSession, DataFrame, Encoder }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import datalake.metadata._
import datalake.processing._
import datalake.outputs._
import datalake.core.WatermarkData

import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.fs.Path
import java.sql.Timestamp

trait SparkSessionTest extends Suite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Rucal Unit Tests")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


  lazy val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val testBasePath = java.nio.file.Files.createTempDirectory("dlf_tempdir").toString

  val randomPrefix = scala.util.Random.alphanumeric.filter(_.isLetter).take(3).mkString.toLowerCase + "_"
  val override_env = new Environment(
    "DEBUG (OVERRIDE)",
    testBasePath.replace("\\", "/"),
    "Europe/Amsterdam",
    "/${connection}/${entity}",
    "/${connection}/${entity}",
    "/${connection}/${destination}",
    secure_container_suffix = Some("-secure"),
    systemfield_prefix = Some(randomPrefix),
    output_method = "paths"
  )

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    // Clean up any existing test data before each test
    cleanupTestData()
    super.beforeEach()
  }

  protected def cleanupTestData(): Unit = {
    import org.apache.commons.io.FileUtils
    
    try {
      // Clean up file system paths
      val silverFolder = new java.io.File(s"$testBasePath/silver")
      if (silverFolder.exists()) {
        FileUtils.cleanDirectory(silverFolder)
      }
      
      // Clean up any test tables in the metastore
      // Get list of databases that might contain test tables
      val testDatabases = spark.sql("SHOW DATABASES").collect()
        .map(_.getString(0))
        .filter(db => db.startsWith("test_") || db.startsWith("silver_"))
      
      testDatabases.foreach { dbName =>
        try {
          val tables = spark.sql(s"SHOW TABLES IN `$dbName`").collect()
          tables.foreach { tableRow =>
            val tableName = tableRow.getString(1)
            try {
              spark.sql(s"DROP TABLE IF EXISTS `$dbName`.`$tableName`")
            } catch {
              case _: Exception => // Ignore individual table cleanup errors
            }
          }
        } catch {
          case _: Exception => // Ignore if database doesn't exist or can't be accessed
        }
      }
    } catch {
      case _: Exception => // Ignore cleanup errors - tests should still run
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.streams.active.foreach(_.stop())  // Stop any active streams
        spark.stop()
        spark.sparkContext.stop()
      }
      SparkSession.clearActiveSession()  // Clear the active session
      SparkSession.clearDefaultSession() // Clear the default session
      org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testBasePath))
    } finally {
      super.afterAll()
    }
  }
}

class DatalakeJsonMetadataTest extends AnyFunSuite with SparkSessionTest {

  test("Generate ADF Json") {
    try {
      val metadatasettings = new JsonMetadataSettings()
      val user_dir = System.getProperty("user.dir")

      metadatasettings.initialize(f"${user_dir}/src/test/scala/example/metadata.json")

      implicit val metadata = new Metadata(metadatasettings)
      println(metadata.getEnvironment.Name)

      val connection = metadata.getConnectionByName("AdventureWorksSql")
      val adf_config = DataFactory.getConfigItems(EntityGroup("avw"))

      println(pretty(parse(adf_config)))

    } catch {
      case e: Exception =>
        fail(s"Test failed with exception: ${e.getMessage}")
    }
  }

}

class SparkEnvironmentTests extends AnyFunSuite with SparkSessionTest {

  test("Spark Version") {
    import spark.implicits._ // Now spark is a stable var, so we import implicits inside the test

    info(s"spark version: ${spark.version}")
    assert(spark.version === "3.5.2", s"spark version: ${spark.version}")

    // spark.conf.getAll.foreach(println)

  }

  test("simple range test") {
    import spark.implicits._

    try {
      val df = spark.range(0, 10, 1)
      assert(df.count() === 10)
    } catch {
      case e: Exception =>
        fail(s"Test failed with exception: ${e.getMessage}")
    }
  }
}

