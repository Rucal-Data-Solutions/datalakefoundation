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
  // Use unique app name and warehouse per test class instance to avoid session sharing
  private val uniqueId = s"${this.getClass.getSimpleName}-${System.nanoTime()}"

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(s"Rucal Unit Tests - $uniqueId")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.ui.enabled", "false") // Disable Spark UI to prevent port conflicts
    .set("spark.sql.shuffle.partitions", "4") // Reduce shuffle partitions for faster tests
    .set("spark.sql.warehouse.dir", s"${System.getProperty("java.io.tmpdir")}/spark-warehouse-$uniqueId")
    .set("spark.driver.host", "localhost") // Explicitly set driver host
    // Derby/Hive metastore settings to avoid locks and conflicts
    .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:${uniqueId};create=true")
    .set("spark.sql.catalogImplementation", "hive")
    .set("spark.sql.streaming.stopTimeout", "5000") // 5 second timeout for streaming query shutdown


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
    try {
      // Ensure we have a valid SparkSession
      // If session is stopped from a previous run, this will create a new one
      if (spark.sparkContext.isStopped) {
        // Clear the stopped session reference and force reinitialization
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        // This is a lazy val, so we can't reinitialize it directly
        // The test will fail here, but at least we'll know why
        throw new IllegalStateException("SparkSession was already stopped. Please restart the test runner.")
      }
      spark.sparkContext.setLogLevel("ERROR")
    } catch {
      case e: IllegalStateException => throw e // Re-throw our custom exception
      case _: Exception =>
        // If we can't access the spark context, try to initialize it
        spark.sparkContext.setLogLevel("ERROR")
    }
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
      // Clean up file system paths - force delete and recreate to ensure clean state
      val silverFolder = new java.io.File(s"$testBasePath/silver")
      if (silverFolder.exists()) {
        FileUtils.deleteDirectory(silverFolder)
      }
      silverFolder.mkdirs()

      val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
      if (bronzeFolder.exists()) {
        FileUtils.deleteDirectory(bronzeFolder)
      }
      bronzeFolder.mkdirs()

      // Clean up any test tables in the metastore
      // Get list of ALL databases except default and system databases
      val testDatabases = spark.sql("SHOW DATABASES").collect()
        .map(_.getString(0))
        .filter(db => db != "default" && !db.startsWith("sys_") && !db.startsWith("information_schema"))

      testDatabases.foreach { dbName =>
        try {
          // Drop the entire database cascade to remove all tables and the database itself
          spark.sql(s"DROP DATABASE IF EXISTS `$dbName` CASCADE")
        } catch {
          case _: Exception => // Ignore if database doesn't exist or can't be accessed
        }
      }

      // Also clean up spark-warehouse directory for ALL non-default databases
      try {
        val warehouseDir = new java.io.File("spark-warehouse")
        if (warehouseDir.exists() && warehouseDir.isDirectory) {
          val warehouseFiles = warehouseDir.listFiles()
          if (warehouseFiles != null) {
            warehouseFiles.filter(_.isDirectory).foreach { dbDir =>
              val dbName = dbDir.getName.replace(".db", "")
              if (dbName != "default") {
                try {
                  FileUtils.deleteDirectory(dbDir)
                } catch {
                  case _: Exception => // Ignore cleanup errors
                }
              }
            }
          }
        }
      } catch {
        case _: Exception => // Ignore warehouse cleanup errors
      }
    } catch {
      case _: Exception => // Ignore cleanup errors - tests should still run
    }
  }

  override def afterAll(): Unit = {
    try {
      // Shutdown log appenders before stopping Spark to avoid "non-started appender" errors
      datalake.log.DatalakeLogManager.shutdown()

      if (spark != null) {
        try {
          // Stop all active streaming queries with timeout
          spark.streams.active.foreach { query =>
            try {
              query.stop()
              query.awaitTermination(5000) // Wait max 5 seconds for graceful shutdown
            } catch {
              case _: Exception => // Ignore if already stopped or timeout
            }
          }
        } catch {
          case _: Exception => // Ignore if streams is not accessible
        }

        try {
          // Stop spark session (this also stops the context)
          spark.stop()
        } catch {
          case _: Exception => // Ignore if already stopped
        }
      }

      // Clear session references
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      // Shutdown Derby to release its threads
      try {
        java.sql.DriverManager.getConnection(s"jdbc:derby:memory:${uniqueId};drop=true")
      } catch {
        case _: java.sql.SQLException => // Derby throws SQLException on successful shutdown
        case _: Exception => // Ignore other exceptions
      }

      // Clean up test directory
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(testBasePath))
      } catch {
        case _: Exception => // Ignore if directory doesn't exist or can't be deleted
      }

      // Clean up temporary warehouse directory
      try {
        val warehouseDir = new java.io.File(s"${System.getProperty("java.io.tmpdir")}/spark-warehouse-$uniqueId")
        if (warehouseDir.exists()) {
          org.apache.commons.io.FileUtils.deleteDirectory(warehouseDir)
        }
      } catch {
        case _: Exception => // Ignore if directory doesn't exist or can't be deleted
      }

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
    assert(spark.version === "4.0.0", s"spark version: ${spark.version}")

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

