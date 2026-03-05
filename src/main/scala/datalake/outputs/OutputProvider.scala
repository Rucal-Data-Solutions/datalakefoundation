package datalake.outputs

import datalake.metadata.Metadata

trait OutputProvider {
  def name: String
  def getConfigItems(arg: Any)(implicit metadata: Metadata): String
}
