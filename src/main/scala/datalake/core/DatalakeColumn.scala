package datalake.core

import datalake.metadata._
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import io.delta.tables._
import java.sql.Timestamp
import com.fasterxml.jackson.module.scala.deser.overrides

class DatalakeColumn(
    val name: String,
    val dataType: DataType,
    val nullable: Boolean,
    val partOfPartition: Boolean
) extends Serializable {

  override def toString(): String = this.name

  override def equals(obj: Any): Boolean = {
    obj match {
      case col: DatalakeColumn =>
        this.name == col.name
      case _ => false
    }
  }
}

object DatalakeColumn {

  def apply(
      name: String,
      data_type: DataType,
      nullable: Boolean,
      part_of_partition: Boolean
  ): DatalakeColumn =
    new DatalakeColumn(name, data_type, nullable, part_of_partition)

  def apply(name: String, data_type: DataType,nullable: Boolean): DatalakeColumn = new DatalakeColumn(name, data_type, nullable, false)
}