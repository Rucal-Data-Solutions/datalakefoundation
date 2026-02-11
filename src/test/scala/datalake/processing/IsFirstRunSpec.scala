package datalake.processing

import org.scalatest.funsuite.AnyFunSuite
import datalake.metadata._
import io.delta.tables._
import org.apache.spark.sql.AnalysisException

/**
  * Tests for the isFirstRun method in ProcessStrategy.
  *
  * Verifies that:
  * - Missing paths/tables are correctly detected as first run
  * - Existing Delta tables are correctly detected as not first run
  * - Only AnalysisException is caught (non-AnalysisException errors propagate)
  */
class IsFirstRunSpec extends AnyFunSuite with SparkSessionTest {

  /** Test helper that exposes the protected isFirstRun method */
  private object TestStrategy extends ProcessStrategy {
    def Process(processing: Processing)(implicit spark: org.apache.spark.sql.SparkSession): Unit = {}
    def testIsFirstRun(destination: OutputLocation): Boolean = isFirstRun(destination)
  }

  test("isFirstRun should return true for non-existent path") {
    val nonExistentPath = s"$testBasePath/silver/does_not_exist"
    assert(TestStrategy.testIsFirstRun(PathLocation(nonExistentPath)) === true)
  }

  test("isFirstRun should return false for existing Delta table at path") {
    import spark.implicits._

    val deltaPath = s"$testBasePath/silver/existing_delta"
    Seq((1, "a"), (2, "b")).toDF("id", "value")
      .write.format("delta").save(deltaPath)

    assert(TestStrategy.testIsFirstRun(PathLocation(deltaPath)) === false)
  }

  test("isFirstRun should return true for non-existent table") {
    assert(TestStrategy.testIsFirstRun(TableLocation("nonexistent_db_xyz.nonexistent_table")) === true)
  }

  test("isFirstRun should return false for existing Delta table in catalog") {
    import spark.implicits._

    val testDb = s"isfirstrun_test_db_${System.currentTimeMillis()}"
    val testTable = s"$testDb.test_table"

    spark.sql(s"CREATE DATABASE IF NOT EXISTS `$testDb`")
    Seq((1, "a"), (2, "b")).toDF("id", "value")
      .write.format("delta").saveAsTable(testTable)

    try {
      assert(TestStrategy.testIsFirstRun(TableLocation(testTable)) === false)
    } finally {
      spark.sql(s"DROP DATABASE IF EXISTS `$testDb` CASCADE")
    }
  }

  test("isFirstRun should propagate non-AnalysisException errors for table locations") {
    // Verify that a non-AnalysisException (e.g. caused by passing null) is NOT
    // silently swallowed as was the case before the fix. Before the fix,
    // `case _: Exception => true` would catch everything and default to overwrite.
    // After the fix, only AnalysisException is caught.
    val caught = intercept[Exception] {
      TestStrategy.testIsFirstRun(TableLocation(null))
    }
    // The exception should NOT be an AnalysisException (those are caught and return true)
    assert(!caught.isInstanceOf[AnalysisException],
      "Non-AnalysisException should propagate, not be silently caught")
  }
}
