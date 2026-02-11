// Databricks notebook source
package datalake.core

import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.StructType

object implicits {

  // Extension helpers for DataFrame normalization, schema comparison, and path utilities

  implicit class DatalakeDataFrame(df: DataFrame) {

    def datalakeNormalize(): DataFrame = {
      val columns = df.columns

      // drop sys columns
      val dfWOsys = df.drop(columns.filter(col => col.startsWith("sys_")): _*)
      
      val regex = """[ +-.,;{}()\n\t=]+"""
      val replacingColumns = columns.map(regex.r.replaceAllIn(_, ""))
      val resultingDf = replacingColumns.zip(columns).foldLeft(dfWOsys) { (tempdf, name) =>
        tempdf.withColumnRenamed(name._2, name._1)
      }

      resultingDf
    }

    def datalakeSchemaCompare(targetSchema: StructType): Array[(DatalakeColumn, String)] = {

      val targetColumns = targetSchema.fields.map(fld => DatalakeColumn(fld.name.toLowerCase(), fld.dataType, fld.nullable ) )
      val sourceColumns = df.schema.fields.map(fld => DatalakeColumn(fld.name.toLowerCase(), fld.dataType, fld.nullable ) )

      // Find columns that exist in source but not in target (added)
      val addedColumns = sourceColumns.filter(sourceCol => 
        !targetColumns.exists(_.name == sourceCol.name)
      )

      // Find columns that exist in target but not in source (removed)
      val removedColumns = targetColumns.filter(targetCol => 
        !sourceColumns.exists(_.name == targetCol.name)
      )

      val schemaDifferences = scala.collection.mutable.ArrayBuffer.empty[(DatalakeColumn, String)]

      if (addedColumns.nonEmpty) {
        addedColumns.foreach { col =>
          schemaDifferences += (col -> "New")
        }
      }

      if (removedColumns.nonEmpty) {
        removedColumns.foreach { col =>
          schemaDifferences += (col -> "Missing")
        }
      }


      schemaDifferences.toArray
    }

  }

  implicit class StringFunctions(str: String) {

    def normalizedPath: String =
      s"${if (str.toString.startsWith("/")) str else "/" + str}"

  }

}
