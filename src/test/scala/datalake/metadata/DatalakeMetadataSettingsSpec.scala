package datalake.metadata

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DatalakeMetadataSettingsSpec extends AnyFunSuite with SparkSessionTest {

  test("getEntitiesById(id: Int) should return Some(Entity) when entity exists") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(1)

    result shouldBe defined
    result.get.Id shouldBe 1
  }

  test("getEntitiesById(id: Int) should return None when entity does not exist") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(9999)

    result shouldBe None
  }

  test("getEntitiesById(ids: Array[Int]) should return Some(List[Entity]) when all entities exist") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(Array(1, 2, 3))

    result shouldBe defined
    result.get should have size 3
    result.get.map(_.Id) should contain allOf (1, 2, 3)
  }

  test("getEntitiesById(ids: Array[Int]) should return Some(List[Entity]) when some entities exist") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(Array(1, 2, 9999))

    result shouldBe defined
    result.get should have size 2
    result.get.map(_.Id) should contain allOf (1, 2)
  }

  test("getEntitiesById(ids: Array[Int]) should return None when no entities exist") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(Array(9998, 9999))

    result shouldBe None
  }

  test("getEntitiesById(ids: Array[Int]) should return None when empty array is provided") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(Array.empty[Int])

    result shouldBe None
  }

  test("getEntitiesById(ids: Array[Int]) should preserve order based on input array") {
    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings)
    settings.setMetadata(metadata)

    val result = settings.getEntitiesById(Array(3, 1, 2))

    result shouldBe defined
    result.get.map(_.Id) shouldBe List(3, 1, 2)
  }
}