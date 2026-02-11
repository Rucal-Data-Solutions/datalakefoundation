package datalake.core

import datalake.metadata._
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types.{ StructType, IntegerType, StringType, TimestampType }
import org.apache.spark.sql.functions.{ col, lit, max }
import java.time.LocalDateTime
import scala.tools.nsc.Reporting

case class WatermarkValue(value: String, datatype: String)

final class WatermarkData(entity_id: Integer)(implicit environment: Environment)
    extends SystemDataObject(
      new SystemDataTableDefinition(
        "watermark",
        List(
          DatalakeColumn("entity_id", IntegerType, false, true),
          DatalakeColumn("column_name", StringType, false, true),
          DatalakeColumn("timestamp", TimestampType, false, false),
          DatalakeColumn("source_type", StringType, false, false),
          DatalakeColumn("value", StringType, false, false)
        )
      )
    ) {

  def getLastValue(column_name: String): Option[WatermarkValue] = {
    val df: Option[DataFrame] = getDataFrame

    val lastValue: Option[WatermarkValue] =
      try {
        val lastRow = df.get
          .filter((col("entity_id") === entity_id) && (col("column_name") === column_name))
          .sort(col("timestamp").desc)
          .head()
        val _value = lastRow.getAs[String]("value")
        val _datatype = lastRow.getAs[String]("source_type")
        if (_datatype == None.getClass().getTypeName())
          None
        else
          Some(WatermarkValue(_value, _datatype))
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
    
    watermark_values.foreach(wm => println(s"Write watermark: <${wm._1.Column_Name}> => ${wm._2.toString()}(${wm._2.getClass().getTypeName()})"))

    if(watermark_values.size > 0) {
      val data = watermark_values.map(
        wm => {
          val new_row = Row(this.entity_id, wm._1.Column_Name, timestamp_now, wm._2.getClass().getTypeName(), wm._2.toString())
          new_row
        }
      )
      this.Append(data.toSeq)
    }

  }

  final def Reset(column: Watermark): Unit = {
    WriteWatermark(Seq((column, None)))
  }

  final def Reset(column: Watermark, toValue: String): Unit = {
    WriteWatermark(Seq((column, toValue)) )
  }

  final def Reset(columnName: String): Unit = {
    // Write watermark reset directly using column name
    val timezoneId = environment.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

    println(s"Write watermark: <${columnName}> => None(${None.getClass().getTypeName()})")

    val new_row = Row(this.entity_id, columnName, timestamp_now, None.getClass().getTypeName(), "None")
    this.Append(Seq(new_row))
  }

  final def Reset(columnName: String, toValue: String): Unit = {
    // Write watermark reset directly using column name and value
    val timezoneId = environment.Timezone.toZoneId
    val timestamp_now = java.sql.Timestamp.valueOf(LocalDateTime.now(timezoneId))

    println(s"Write watermark: <${columnName}> => ${toValue}(${toValue.getClass().getTypeName()})")

    val new_row = Row(this.entity_id, columnName, timestamp_now, toValue.getClass().getTypeName(), toValue)
    this.Append(Seq(new_row))
  }
}

