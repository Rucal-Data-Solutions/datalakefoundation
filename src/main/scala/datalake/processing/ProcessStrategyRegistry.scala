package datalake.processing

import java.util.ServiceLoader
import scala.collection.JavaConverters._
import datalake.metadata.ProcessStrategyNotSupportedException
import datalake.log.DatalakeLogManager
import org.apache.spark.sql.SparkSession

object ProcessStrategyRegistry {
  @transient private lazy val logger = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    DatalakeLogManager.getLogger(this.getClass)
  }

  private val builtIn: Map[String, ProcessStrategy] = Map(
    Full.Name -> Full,
    Merge.Name -> Merge,
    Historic.Name -> Historic,
    "delta" -> Merge
  )

  private lazy val spiStrategies: Map[String, ProcessStrategy] = {
    val loader = ServiceLoader.load(
      classOf[ProcessStrategyProvider],
      Thread.currentThread.getContextClassLoader
    )
    val strategies = scala.collection.mutable.Map.empty[String, ProcessStrategy]
    for (provider <- loader.asScala) {
      val strategy = provider.create()
      val names = provider.name.toLowerCase +: provider.aliases.map(_.toLowerCase)
      for (n <- names) {
        if (builtIn.contains(n)) {
          logger.warn(
            s"SPI provider '${provider.getClass.getName}' overrides " +
              s"built-in strategy '$n'"
          )
        }
        strategies(n) = strategy
      }
    }
    strategies.toMap
  }

  private lazy val all: Map[String, ProcessStrategy] = builtIn ++ spiStrategies

  def get(name: String): Option[ProcessStrategy] =
    all.get(name.toLowerCase)

  def getOrThrow(name: String): ProcessStrategy =
    get(name).getOrElse(
      throw ProcessStrategyNotSupportedException(
        s"Process Type $name not supported"
      )
    )
}
