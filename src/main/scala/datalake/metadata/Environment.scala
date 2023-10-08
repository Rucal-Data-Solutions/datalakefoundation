package datalake.metadata

import datalake.core._
import datalake.processing._
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Column, Row, Dataset }
import org.apache.spark.sql.types._
import scala.util.Try
import scala.reflect.runtime._
import org.json4s.JsonAST

class Environment(
  name: String,
  root_folder: String,
  timezone: String
){
    def Name: String =
    this.name

    def RootFolder: String =
      this.root_folder

    def Timezone: TimeZone = 
      TimeZone.getTimeZone(timezone)
}