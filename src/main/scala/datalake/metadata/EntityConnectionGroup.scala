package datalake.metadata

class EntityConnectionGroup(name: String) extends Serializable {
  private val lowerCaseName: String = name.toLowerCase

  override def toString(): String = lowerCaseName
  def Name: String = this.toString()
}

object EntityConnectionGroup{
  def apply(name: String): EntityConnectionGroup = {
    if (name.isEmpty) {
      throw new IllegalArgumentException("Name cannot be empty")
    }
    new EntityConnectionGroup(name)
  }
}