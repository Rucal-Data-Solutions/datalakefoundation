package datalake.processing

trait ProcessStrategyProvider {
  def name: String
  def aliases: Seq[String] = Seq.empty
  def create(): ProcessStrategy
}
