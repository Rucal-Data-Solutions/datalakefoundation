package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata.ProcessStrategyNotSupportedException

class ProcessStrategyRegistrySpec extends AnyFunSuite {

  test("resolves Full by name") {
    assert(ProcessStrategyRegistry.get("full").contains(Full))
  }

  test("resolves Merge by name") {
    assert(ProcessStrategyRegistry.get("merge").contains(Merge))
  }

  test("resolves Historic by name") {
    assert(ProcessStrategyRegistry.get("historic").contains(Historic))
  }

  test("resolves delta alias to Merge") {
    assert(ProcessStrategyRegistry.get("delta").contains(Merge))
  }

  test("case insensitivity works") {
    assert(ProcessStrategyRegistry.get("FULL").contains(Full))
    assert(ProcessStrategyRegistry.get("Merge").contains(Merge))
    assert(ProcessStrategyRegistry.get("HISTORIC").contains(Historic))
  }

  test("unknown name returns None from get") {
    assert(ProcessStrategyRegistry.get("nonexistent").isEmpty)
  }

  test("unknown name throws from getOrThrow") {
    assertThrows[ProcessStrategyNotSupportedException] {
      ProcessStrategyRegistry.getOrThrow("nonexistent")
    }
  }

  test("built-in strategies have correct Name property") {
    assert(Full.Name == "full")
    assert(Merge.Name == "merge")
    assert(Historic.Name == "historic")
  }

  test("SPI discovers TestStrategyProvider") {
    val strategy = ProcessStrategyRegistry.get("teststrategy")
    assert(strategy.isDefined)
    assert(strategy.get.Name == "teststrategy")
  }

  test("SPI alias resolves") {
    val strategy = ProcessStrategyRegistry.get("test-alias")
    assert(strategy.isDefined)
    assert(strategy.get.Name == "teststrategy")
  }
}
