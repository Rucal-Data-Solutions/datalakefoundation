package datalake.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

class TestStrategy extends ProcessStrategy {
  override val Name: String = "teststrategy"

  def Process(
      processing: Processing
  )(implicit spark: SparkSession): Option[StreamingQuery] = None
}

class TestStrategyProvider extends ProcessStrategyProvider {
  def name: String = "teststrategy"
  override def aliases: Seq[String] = Seq("test-alias")
  def create(): ProcessStrategy = new TestStrategy
}
