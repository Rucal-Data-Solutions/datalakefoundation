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
}
