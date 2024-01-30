package datalake.core

import datalake.metadata._
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types.{ StructType, IntegerType, StringType, TimestampType }
import org.apache.spark.sql.functions.{ col, lit, max }
import java.time.LocalDateTime
import scala.tools.nsc.Reporting

final class WatermarkData(entity_id: Integer)(implicit environment: Environment)
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

  def getLastValue(column_name: String): Option[String] = {
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

  final def WriteWatermark(watermark_values: Seq[(Watermark, Any)]): Unit = {
    // Write the watermark values to system table
    val timezoneId = environment.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))
    
    watermark_values.foreach(wm => println(s"input for watermark: ${wm._1}: ${wm._2.toString()}(${wm._2.getClass().getTypeName()})"))

    if(watermark_values.size > 0) {
      val data = watermark_values.map(
        wm => {
          val new_row = Row(this.entity_id, wm._1, timestamp_now, wm._2.getClass().getTypeName(), wm._2.toString())
          new_row
        }
      )
      this.Append(data.toSeq)
    }

  }

  final def Reset(column: Watermark): Unit = {
    WriteWatermark(Seq((column, String.valueOf(None))))
  }
  
  final def Reset(columns: Seq[Watermark]): Unit = {
    WriteWatermark( columns.map(wm => (wm, String.valueOf(None))) )
  }
}

