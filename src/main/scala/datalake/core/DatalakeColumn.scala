package datalake.core

import datalake.metadata._
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import io.delta.tables._
import java.sql.Timestamp
import com.fasterxml.jackson.module.scala.deser.overrides

class DatalakeColumn(
    Name: String,
    Data_type: DataType,
    Nullable: Boolean,
    Part_of_partition: Boolean
) extends Serializable {
  final def name:String = this.Name
  final def dataType:DataType = this.Data_type
  final def nullable:Boolean = this.Nullable
  final def partOfPartition:Boolean = this.Part_of_partition

  override def toString(): String = this.name

  override def equals(obj: Any): Boolean = {
    obj match {
      case c: DatalakeColumn => (c.name == obj.asInstanceOf[DatalakeColumn].name)
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