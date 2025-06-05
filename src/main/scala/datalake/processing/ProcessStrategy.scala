package datalake.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode, Row, SparkSession, Dataset }
import org.apache.logging.log4j.LogManager

import scala.util.{ Try, Success, Failure }
import java.util.TimeZone
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import io.delta.tables._

import datalake.core._
import datalake.metadata._
import datalake.core.implicits._
import datalake.log._

abstract class ProcessStrategy {
  implicit val spark: SparkSession =
    SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  final val logger = LogManager.getLogger(this.getClass())

  final val Name: String = {
    val cls = this.getClass()
    cls.getSimpleName().dropRight(1).toLowerCase()
  }
  
  def Process(processing: Processing): Unit
}