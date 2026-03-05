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

object SparkSessionTest {
  private[metadata] val isClusterMode = sys.env.get("DLF_TEST_MODE").contains("cluster")
  private val sessionId = s"dlf-tests-${System.nanoTime()}"

  lazy val sharedBasePath: String = {
    val path = if (isClusterMode) {
      val dir = sys.env.getOrElse("DLF_TEST_DIR", "/tmp/datalake-tests")
      new java.io.File(dir).mkdirs()
      dir
    } else {
      java.nio.file.Files.createTempDirectory("dlf_testdata").toString
    }
    new java.io.File(s"$path/bronze").mkdirs()
    new java.io.File(s"$path/silver").mkdirs()
    new java.io.File(s"$path/system").mkdirs()
    path
  }

  private[metadata] lazy val sharedConf: SparkConf = {
    val sparkMaster =
      if (isClusterMode) sys.env.getOrElse("DLF_SPARK_MASTER", "spark://localhost:7077")
      else "local[*]"

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName(s"Rucal Unit Tests - $sessionId")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.ui.enabled", "true")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.warehouse.dir", {
        val baseDir = if (isClusterMode)
          sys.env.getOrElse("DLF_TEST_DIR", System.getProperty("java.io.tmpdir"))
        else
          System.getProperty("java.io.tmpdir")
        s"$baseDir/spark-warehouse-$sessionId"
      })
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:$sessionId;create=true")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.streaming.stopTimeout", "5000")

    if (isClusterMode) {
      conf.set("spark.driver.host",
        sys.env.getOrElse("DLF_DRIVER_HOST", "host.docker.internal"))
      conf.set("spark.driver.bindAddress", "0.0.0.0")

      val targetDir = new java.io.File("target/scala-2.13")
      val jar = targetDir
        .listFiles()
        .find(f =>
          f.getName.endsWith(".jar") && !f.getName.contains("javadoc") && !f.getName.contains(
            "sources"
          )
        )
        .getOrElse(throw new RuntimeException("Project JAR not found. Run 'sbt package' first."))
      conf.set("spark.jars", jar.getAbsolutePath)
      conf.set("spark.hadoop.fs.file.impl", classOf[org.apache.hadoop.fs.RawLocalFileSystem].getName)
      conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    } else {
      conf.set("spark.driver.host", "localhost")
    }

    conf
  }

  lazy val sharedSpark: SparkSession = SparkSession
    .builder()
    .config(sharedConf)
    .enableHiveSupport()
    .getOrCreate()

  private[metadata] def detectExistingPrefix(
      spark: SparkSession,
      basePath: String
  ): Option[String] = {
    try {
      val silverDir = new java.io.File(s"$basePath/silver")
      if (!silverDir.exists()) return None

      import scala.collection.JavaConverters._
      val deltaTableDir = java.nio.file.Files
        .walk(silverDir.toPath)
        .iterator()
        .asScala
        .find(p =>
          p.getFileName.toString == "_delta_log" &&
            java.nio.file.Files.isDirectory(p)
        )
        .map(_.getParent.toString)

      deltaTableDir.flatMap { tablePath =>
        val schema = io.delta.tables.DeltaTable.forPath(spark, tablePath).toDF.schema
        schema.fieldNames
          .find(_.endsWith("SourceHash"))
          .map(_.stripSuffix("SourceHash"))
      }
    } catch {
      case _: Exception => None
    }
  }
}

trait SparkSessionTest extends Suite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  val conf: SparkConf = SparkSessionTest.sharedConf

  lazy val spark: SparkSession = SparkSessionTest.sharedSpark

  val testBasePath: String = SparkSessionTest.sharedBasePath

  lazy val randomPrefix: String = SparkSessionTest
    .detectExistingPrefix(spark, testBasePath)
    .getOrElse(
      scala.util.Random.alphanumeric.filter(_.isLetter).take(3).mkString.toLowerCase + "_"
    )
  lazy val override_env = new Environment(
    "DEBUG (OVERRIDE)",
    testBasePath.replace("\\", "/"),
    "Europe/Amsterdam",
    "/${connection}/${entity}",
    "/${connection}/${entity}",
    "/${connection}/${destination}",
    secure_container_suffix = Some("-secure"),
    systemfield_prefix = Some(randomPrefix),
    log_output = Some(s"${testBasePath.replace("\\", "/")}/dlf_log.parquet"),
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
    // No-op: test isolation is achieved via unique entity names per test.
    // In-memory Derby metastore is cleaned up on JVM exit.
    // Blanket database dropping caused race conditions with parallel test suites.
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
      }

      // Session, Derby, and warehouse cleanup happen at JVM shutdown
      // since they are shared across all test classes.
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

