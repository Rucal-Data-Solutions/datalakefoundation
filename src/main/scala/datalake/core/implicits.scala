// Databricks notebook source
package datalake.core

// import org.apache.spark.sql.SparkSession
// private val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
import org.apache.spark.sql.{ DataFrame, Dataset, Row, DataFrameWriter, DataFrameReader }
import org.apache.spark.sql.types.{StructType}
import java.time.LocalDateTime
// import org.apache.derby.iapi.types.DataType

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

    def datalake_schemacompare(targetSchema: StructType): Array[(DatalakeColumn, String)] = {

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


      if (schemaDifferences.nonEmpty) {
        println("Schema differences found:")
        schemaDifferences.foreach { diff =>
          println(s"Column: ${diff._1}, Difference: ${diff._2}")
        }
      } else {
        println("No schema differences found.")
      }

      return schemaDifferences.toArray
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
