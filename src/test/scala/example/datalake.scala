
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SparkSession, DataFrame }

import datalake.metadata._
import datalake.processing._
import org.apache.parquet.format.IntType

// import org.apache.spark.sql._
// import org.apache.spark.sql.functions.{col}

object DatalakeApp {

  val spark = SparkSession
    .builder()
    .appName("Rucal Test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val metadatasettings = new SqlMetadataSettings()
    metadatasettings.initialize(new SqlServerSettings("<SERVER>", 1433, "<DATABASE>", "<USER>", "<PASSWORD>"))
    val metadata = new Metadata(metadatasettings)

    val connection = metadata.getConnection("1")
    // val env = metadata.getEnvironment

    val entity = metadata.getEntity(39)

    val proc = new Processing(entity, "test.parquet")
    
    proc.process()
 }

  def getTestParquet(): DataFrame = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import spark.implicits._

// Define the schema
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("dec_value", DecimalType(38, 1), true),
        StructField("description", StringType, true),
        StructField("new_field", BooleanType, true),
      )
    )

// Create a sequence of rows
    val data = Seq(
      Row(1, "Alice", Decimal(100.5), "Desc1", false),
      Row(2, "Bob", Decimal(200.5), "Desc2", false),
      Row(3, "Charlie", Decimal(300.5), "Test_123", false),
      Row(4, "Gerald", Decimal(300.5), "Test_345", false),
      Row(5, "Ruben", Decimal(10000.45), "asdsfdgs", true)
    )

// Create a DataFrame with the given schema and data
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    df
  }
}
