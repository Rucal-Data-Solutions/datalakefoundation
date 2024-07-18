// Databricks notebook source
package datalake.core

// import org.apache.spark.sql.SparkSession
// private val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
import org.apache.spark.sql.{ DataFrame, Dataset, Row, DataFrameWriter, DataFrameReader }
import org.apache.spark.sql.types.{StructType}
import java.time.LocalDateTime
import org.apache.derby.iapi.types.DataType

object implicits {

  implicit class DatalakeDataFrame(df: DataFrame) {

    def datalake_normalize(): DataFrame = {
      val columns = df.columns

      // dorp sys columns
      val dfWOsys = df.drop(columns.filter(col => col.startsWith("sys_")): _*)
      
      val regex = """[ +-.,;{}()\n\t=]+"""
      val replacingColumns = columns.map(regex.r.replaceAllIn(_, ""))
      val resultingDf = replacingColumns.zip(columns).foldLeft(dfWOsys) { (tempdf, name) =>
        tempdf.withColumnRenamed(name._2, name._1)
      }

      return resultingDf
    }

    def datalake_schemacompare(targetSchema: StructType): String = {

      val targetColumns = targetSchema.fields.map(fld => DatalakeColumn(fld.name.toLowerCase(), fld.dataType, fld.nullable ) ).toSet[DatalakeColumn]
      val sourceColumns = df.schema.fields.map(fld => DatalakeColumn(fld.name.toLowerCase(), fld.dataType, fld.nullable ) ).toSet[DatalakeColumn]

      val addedColumns = sourceColumns -- targetColumns
      val deletedColumns = targetColumns -- sourceColumns

      val schemaDifferences = scala.collection.mutable.ArrayBuffer.empty[(DatalakeColumn, String)]

      if (addedColumns.nonEmpty) {
        addedColumns.foreach { col =>
          schemaDifferences += (col -> "Added")
        }
      }

      if (deletedColumns.nonEmpty) {
        deletedColumns.foreach { col =>
          schemaDifferences += (col -> "Deleted")
        }
      }




      // if (schemaDifferences.nonEmpty) {
      //   println("Schema differences found:")
      //   schemaDifferences.foreach { diff =>
      //     println(s"Column: ${diff.columnName}, Difference: ${diff.difference}")
      //   }
      // } else {
      //   println("No schema differences found.")
      // }

      return "TEST"
    }

  }

  implicit class StringFunctions(str: String) {

    def normalized_path: String ={
      return s"${if (str.toString.startsWith("/")) str else "/" + str}"
    }

  }

  // implicit class DatalakeDataframeWriter(dfw: DataFrameWriter[Row] ){
  //   def parquetWithLock(path: String, lockfile: String): Unit = {
  //     if ( datalake.utils.FileOperations.check_lock(lockfile) )
  //       throw new datalake.utils.FileOperations.FileLockedException("destination is locked.")
  //     else {
  //       datalake.utils.FileOperations.set_lock(lockfile)
  //       try {
  //         dfw.parquet(path)
  //       }
  //       finally {
  //         datalake.utils.FileOperations.drop_lock(lockfile)
  //       }
  //     }
  //   }

  //   def parquetWithLock(path: String): Unit = {
  //     val lockfile = s"$path.lck"
  //     this.parquetWithLock(path, lockfile)
  //   }
  // }

  // implicit class DatalakeDataframeReader(dfr: DataFrameReader){
  //   def parquetWithLock(path: String, lockfile: String, timeout: Int): DataFrame = {
  //     val startOfLoop = LocalDateTime.now()

  //     do{
  //       Thread.sleep(1000)
  //       val diff = startOfLoop.plusSeconds(timeout).compareTo(LocalDateTime.now())
  //       if ( diff <= 0 ){
  //         throw new LockTimeout(f"Timeout ($timeout\fS) expired waiting for lock.")
  //       }
  //     }
  //     while(datalake.utils.FileOperations.check_lock(lockfile) )
  //     return dfr.parquet(path)
  //   }

  //   def parquetWithLock(path: String, timeout: Int = 120): DataFrame = {
  //     val lockfile = s"$path.lck"
  //     return this.parquetWithLock(path, lockfile, timeout)
  //   }
  // }

}
