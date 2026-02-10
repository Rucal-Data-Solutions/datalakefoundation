package datalake.metadata

import org.scalatest.funsuite.AnyFunSuite
import datalake.core.WatermarkData

class WatermarkSpec extends AnyFunSuite with SparkSessionTest {

  test("Watermark with no stored value returns None gracefully") {
    // Create a watermark for an entity that has never been processed
    // (no watermark values in the system table)
    val wm = new Watermark(
      override_env,
      entity_id = 99999, // Non-existent entity, no watermark data stored
      column_name = "SeqNr",
      operation = "or",
      operation_group = Some(0),
      expression = "'${last_value}'"
    )

    val result = wm.Value
    assert(result.isEmpty, "Watermark.Value should return None when no stored value exists")
  }

  test("Watermark with invalid expression logs error and returns None") {
    // First, write a watermark value that contains a backslash,
    // which triggers InvalidEvalParameterException during expression building.
    // This exercises the catch blocks in Watermark.Value that use logger.warn.
    val entityId = 99998
    implicit val env = override_env
    val wmd = new WatermarkData(entityId)

    val wm = new Watermark(
      override_env,
      entity_id = entityId,
      column_name = "TestCol",
      operation = "or",
      operation_group = Some(0),
      expression = "'${last_value}'"
    )

    // Write a watermark value containing a backslash — this will cause
    // LiteralEvalParameter validation to fail when building expression params
    wmd.WriteWatermark(Seq((wm, "bad\\value")))

    // Should return None instead of throwing, and log the error via logger.warn
    val result = wm.Value
    assert(result.isEmpty,
      "Watermark.Value should return None when expression evaluation fails")
  }

  test("Watermark with valid expression evaluates correctly") {
    val entityId = 99997
    implicit val env = override_env
    val wmd = new WatermarkData(entityId)

    val wm = new Watermark(
      override_env,
      entity_id = entityId,
      column_name = "TestCol2",
      operation = "or",
      operation_group = Some(0),
      expression = "'${last_value}'"
    )

    // Write a watermark value
    wmd.WriteWatermark(Seq((wm, "42")))

    // Now evaluate — should return the last_value wrapped in quotes
    val result = wm.Value
    assert(result.isDefined, "Watermark.Value should return Some for valid expression with stored value")
    assert(result.get === "'42'", s"Watermark.Value should evaluate to the quoted last value, got: ${result.get}")
  }

  test("WatermarkData.Reset with column name string resets to None") {
    val entityId = 99996
    implicit val env = override_env
    val wmd = new WatermarkData(entityId)

    // First write a watermark value
    val wm = new Watermark(
      override_env,
      entity_id = entityId,
      column_name = "TestCol3",
      operation = "or",
      operation_group = Some(0),
      expression = "'${last_value}'"
    )
    wmd.WriteWatermark(Seq((wm, "100")))

    // Verify it was written
    val initialValue = wmd.getLastValue("TestCol3")
    assert(initialValue.isDefined, "Initial watermark value should be written")
    assert(initialValue.get.value === "100", "Initial watermark value should be 100")

    // Reset using the new string-based method
    wmd.Reset("TestCol3")

    // Verify it was reset to None
    val resetValue = wmd.getLastValue("TestCol3")
    assert(resetValue.isEmpty, "Watermark should be reset to None")
  }

  test("WatermarkData.Reset with column name string and value resets to specified value") {
    val entityId = 99995
    implicit val env = override_env
    val wmd = new WatermarkData(entityId)

    // First write a watermark value
    val wm = new Watermark(
      override_env,
      entity_id = entityId,
      column_name = "TestCol4",
      operation = "or",
      operation_group = Some(0),
      expression = "'${last_value}'"
    )
    wmd.WriteWatermark(Seq((wm, "500")))

    // Verify it was written
    val initialValue = wmd.getLastValue("TestCol4")
    assert(initialValue.isDefined, "Initial watermark value should be written")
    assert(initialValue.get.value === "500", "Initial watermark value should be 500")

    // Reset to a new value using the new string-based method
    wmd.Reset("TestCol4", "2024-01-01")

    // Verify it was reset to the new value
    val resetValue = wmd.getLastValue("TestCol4")
    assert(resetValue.isDefined, "Watermark should be reset to new value")
    assert(resetValue.get.value === "2024-01-01", "Watermark should be reset to 2024-01-01")
  }

  test("Entity.ResetWatermark with column name resets to None") {
    val entityId = 99994
    implicit val env = override_env

    // Create a minimal metadata string for an entity
    val metadataJson = s"""{
      "environment": {
        "name": "test",
        "rootfolder": "$testBasePath",
        "timezone": "Europe/Amsterdam"
      },
      "connections": [
        {
          "name": "test_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": [
        {
          "id": $entityId,
          "name": "test_entity",
          "connection": "test_connection",
          "enabled": true,
          "processtype": "full",
          "settings": {},
          "columns": [
            {
              "name": "id",
              "datatype": "integer",
              "fieldrole": ["businesskey"]
            }
          ],
          "watermark": [
            {
              "column_name": "TestCol5",
              "operation": "or",
              "expression": "'$${last_value}'"
            }
          ]
        }
      ]
    }"""

    val settings = new StringMetadataSettings()
    settings.initialize(metadataJson)
    val metadata = new Metadata(settings, override_env)
    val entity = metadata.getEntities(entityId).head

    // Write a watermark value first
    val wmd = new WatermarkData(entityId)(env)
    val wm = entity.Watermark.head
    wmd.WriteWatermark(Seq((wm, "999")))

    // Verify it was written
    val initialValue = wmd.getLastValue("TestCol5")
    assert(initialValue.isDefined, "Initial watermark value should be written")

    // Reset using Entity convenience method
    entity.ResetWatermark("TestCol5")

    // Verify it was reset to None
    val resetValue = wmd.getLastValue("TestCol5")
    assert(resetValue.isEmpty, "Watermark should be reset to None via Entity.ResetWatermark")
  }

  test("Entity.ResetWatermark with column name and value resets to specified value") {
    val entityId = 99993
    implicit val env = override_env

    // Create a minimal metadata string for an entity
    val metadataJson = s"""{
      "environment": {
        "name": "test",
        "rootfolder": "$testBasePath",
        "timezone": "Europe/Amsterdam"
      },
      "connections": [
        {
          "name": "test_connection",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": [
        {
          "id": $entityId,
          "name": "test_entity",
          "connection": "test_connection",
          "enabled": true,
          "processtype": "full",
          "settings": {},
          "columns": [
            {
              "name": "id",
              "datatype": "integer",
              "fieldrole": ["businesskey"]
            }
          ],
          "watermark": [
            {
              "column_name": "TestCol6",
              "operation": "or",
              "expression": "'$${last_value}'"
            }
          ]
        }
      ]
    }"""

    val settings = new StringMetadataSettings()
    settings.initialize(metadataJson)
    val metadata = new Metadata(settings, override_env)
    val entity = metadata.getEntities(entityId).head

    // Write a watermark value first
    val wmd = new WatermarkData(entityId)(env)
    val wm = entity.Watermark.head
    wmd.WriteWatermark(Seq((wm, "777")))

    // Verify it was written
    val initialValue = wmd.getLastValue("TestCol6")
    assert(initialValue.isDefined, "Initial watermark value should be written")

    // Reset to a new value using Entity convenience method
    entity.ResetWatermark("TestCol6", "2025-12-31")

    // Verify it was reset to the new value
    val resetValue = wmd.getLastValue("TestCol6")
    assert(resetValue.isDefined, "Watermark should be reset to new value")
    assert(resetValue.get.value === "2025-12-31", "Watermark should be reset to 2025-12-31 via Entity.ResetWatermark")
  }
}
