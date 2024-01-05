package datalake.core

import datalake.metadata._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.{ StructType, IntegerType, StringType, TimestampType }
import org.apache.spark.sql.functions.{ col, lit, max }

final class WatermarkData(implicit environment: Environment)
    extends SystemDataObject(
      new SystemDataTable_Definition(
        "watermark",
        List(
          SystemDataColumn("entity_id", IntegerType, false, true),
          SystemDataColumn("column_name", StringType, false, false),
          SystemDataColumn("timestamp", TimestampType, false, false),
          SystemDataColumn("source_type", StringType, false, false),
          SystemDataColumn("value", StringType, false, false)
        )
      )
    ) {

  def getLastValue(entity_id: Integer, column_name: String): Option[String] = {
    val df: Option[DataFrame] = getDataFrame

    val lastValue: Option[String] =
      try {
        val firstRow = df.get
          .filter((col("entity_id") === entity_id) && (col("column_name") === column_name))
          .sort(col("timestamp").desc)
          .agg(max(col("value")).alias("last_value"))
          .first()
        Some(firstRow.getAs[String]("last_value"))
      }
      catch{
        case e: NoSuchElementException => None
      }

    return lastValue
  }
}

