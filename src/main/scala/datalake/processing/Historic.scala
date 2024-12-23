package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Column, SaveMode }

import io.delta.tables._

import datalake.metadata._
import datalake.core.implicits._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import datalake.core.FileOperations


final object Historic extends ProcessStrategy {

  throw new org.apache.commons.lang.NotImplementedException ("Historic strategy not implemented.")

  private val spark: SparkSession =
    SparkSession.builder.enableHiveSupport().getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

  def Process(processing: Processing) = {
    val source: DataFrame = processing.getSource.source

    // firstly Merge load
    Merge.Process(processing)

    // Get the last version of the destination table for comparison
    val latestVersion: Long = DeltaTable
      .forPath(processing.destination)
      .history(1)
      .select("version")
      .limit(1).collect()(0).getAs[Long](0)

    val deltaTable = DeltaTable.forPath(spark, processing.destination, Map("versionAsOf" -> latestVersion.toString()) )


    //TODO: OUTDATE changed records

    //TODO: CREATE New records from changes.
  }
}