package datalake.outputs

import java.util.ServiceLoader
import scala.collection.JavaConverters._

object OutputProviderRegistry {

  private lazy val providers: Map[String, OutputProvider] = {
    val loader = ServiceLoader.load(
      classOf[OutputProvider],
      Thread.currentThread.getContextClassLoader
    )
    loader.asScala.map(p => p.name.toLowerCase -> p).toMap
  }

  def get(name: String): Option[OutputProvider] =
    providers.get(name.toLowerCase)

  def all: Iterable[OutputProvider] = providers.values
}
