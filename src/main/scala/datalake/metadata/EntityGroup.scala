package datalake.metadata


class EntityGroup(name: String) extends Serializable {
  private val lowerCaseName: String = name.toLowerCase

  override def toString(): String = lowerCaseName
  def Name: String = this.toString()
}

object EntityGroup{
  def apply(name: String): EntityGroup = {
    if (name.isEmpty) {
      throw new IllegalArgumentException("Name cannot be empty")
    }
    new EntityGroup(name)
  }
}