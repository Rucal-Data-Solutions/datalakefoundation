package datalake.core

import org.apache.spark.sql.types.DataType

case class DatalakeColumn(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    partOfPartition: Boolean = false
) {

    override def toString: String = this.name

    override def equals(obj: Any): Boolean = {
        obj match {
            case col: DatalakeColumn => this.name == col.name
            case _ => false
        }
    }

    override def hashCode(): Int = name.hashCode
}
