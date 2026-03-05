package datalake.processing

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.funsuite.AnyFunSuite

import datalake.metadata._

class ExpressionsDocSpec extends AnyFunSuite with SparkSessionTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Builds a minimal metadata JSON string for a single entity.
    *
    * @param entityId
    *   numeric id used in the JSON; must be unique across concurrent tests
    * @param entityName
    *   logical source name
    * @param columnsJson
    *   JSON fragment for the "columns" array
    * @param transformationsJson
    *   JSON fragment for the "transformations" array (default: empty)
    */
  private def buildMetadataJson(
    entityId: Int,
    entityName: String,
    columnsJson: String,
    transformationsJson: String = "[]"
  ): String =
    s"""
    {
      "environment": {
        "name": "DEBUG (EXPRESSIONS DOC TEST)",
        "timezone": "Europe/Amsterdam",
        "root_folder": "${testBasePath.replace("\\", "/")}",
        "raw_path": "/$${connection}/$${entity}",
        "bronze_path": "/$${connection}/$${entity}",
        "silver_path": "/$${connection}/$${destination}",
        "systemfield_prefix": "${randomPrefix}",
        "output_method": "paths"
      },
      "connections": [
        {
          "name": "test_conn",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": [
        {
          "id": $entityId,
          "name": "$entityName",
          "enabled": true,
          "connection": "test_conn",
          "processtype": "merge",
          "watermark": [],
          "columns": $columnsJson,
          "settings": {},
          "transformations": $transformationsJson
        }
      ]
    }
    """

  /** Runs a merge processing cycle and returns the silver DataFrame filtered by
    * test_id. Abstracts over PathLocation / TableLocation.
    */
  private def runAndRead(
    testEntity: Entity,
    slice: String,
    testId: String
  ) = {
    import spark.implicits._
    val proc = new Processing(testEntity, slice)
    proc.Process(Merge)

    testEntity.getOutput.silver match {
      case pathLoc: PathLocation =>
        spark.read.format("delta").load(pathLoc.path).filter($"test_id" === testId)
      case tableLoc: TableLocation =>
        spark.read.table(tableLoc.table).filter($"test_id" === testId)
      case _ => throw new IllegalStateException("Unexpected output method type")
    }
  }

  // ---------------------------------------------------------------------------
  // Calculated column tests
  // ---------------------------------------------------------------------------

  test("Calculated column: string literal 'EMEA' should produce value EMEA") {
    import spark.implicits._
    val testId = s"calc_str_lit_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "Region", "datatype": "string",
        "fieldroles": ["calculated"], "expression": "'EMEA'" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_str_lit", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, testId), (2, testId))
      .toDF("id", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/str_lit_$testId.parquet")

    val result = runAndRead(testEntity, s"str_lit_$testId.parquet", testId)

    assert(result.count() === 2)
    result.collect().foreach { row =>
      assert(row.getAs[String]("Region") === "EMEA", "Region should be string literal EMEA")
    }
  }

  test("Calculated column: concat(first_name, ' ', last_name) should produce full name") {
    import spark.implicits._
    val testId = s"calc_concat_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "FullName", "datatype": "string",
        "fieldroles": ["calculated"], "expression": "concat(first_name, ' ', last_name)" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_concat", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, "Alice", "Smith", testId), (2, "Bob", "Jones", testId))
      .toDF("id", "first_name", "last_name", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/concat_$testId.parquet")

    val result = runAndRead(testEntity, s"concat_$testId.parquet", testId)

    assert(result.count() === 2)
    val row1 = result.filter($"id" === 1).first()
    assert(row1.getAs[String]("FullName") === "Alice Smith", "FullName should be concatenated")
    val row2 = result.filter($"id" === 2).first()
    assert(row2.getAs[String]("FullName") === "Bob Jones", "FullName should be concatenated")
  }

  test("Calculated column: current_date() should produce a non-null date") {
    import spark.implicits._
    val testId = s"calc_curdate_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "TodayDate", "datatype": "date",
        "fieldroles": ["calculated"], "expression": "current_date()" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_curdate", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, testId))
      .toDF("id", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/curdate_$testId.parquet")

    val result = runAndRead(testEntity, s"curdate_$testId.parquet", testId)

    assert(result.count() === 1)
    val row = result.first()
    assert(row.getAs[java.sql.Date]("TodayDate") != null, "current_date() should be non-null")
  }

  test("Calculated column: current_timestamp() should produce a non-null timestamp") {
    import spark.implicits._
    val testId = s"calc_ts_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "NowTs", "datatype": "timestamp",
        "fieldroles": ["calculated"], "expression": "current_timestamp()" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_ts", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, testId))
      .toDF("id", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/ts_$testId.parquet")

    val result = runAndRead(testEntity, s"ts_$testId.parquet", testId)

    assert(result.count() === 1)
    val row = result.first()
    assert(row.getAs[java.sql.Timestamp]("NowTs") != null, "current_timestamp() should be non-null")
  }

  test("Calculated column: upper(name) should produce uppercase string") {
    import spark.implicits._
    val testId = s"calc_upper_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "UpperName", "datatype": "string",
        "fieldroles": ["calculated"], "expression": "upper(name)" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_upper", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, "Alice", testId), (2, "bob", testId))
      .toDF("id", "name", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/upper_$testId.parquet")

    val result = runAndRead(testEntity, s"upper_$testId.parquet", testId)

    assert(result.count() === 2)
    val row1 = result.filter($"id" === 1).first()
    assert(row1.getAs[String]("UpperName") === "ALICE", "upper() should uppercase the value")
    val row2 = result.filter($"id" === 2).first()
    assert(row2.getAs[String]("UpperName") === "BOB", "upper() should uppercase the value")
  }

  test("Calculated column: year(order_date) should extract year as integer") {
    import spark.implicits._
    val testId = s"calc_year_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "OrderYear", "datatype": "integer",
        "fieldroles": ["calculated"], "expression": "year(order_date)" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_year", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, java.sql.Date.valueOf("2023-07-15"), testId))
      .toDF("id", "order_date", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/year_$testId.parquet")

    val result = runAndRead(testEntity, s"year_$testId.parquet", testId)

    assert(result.count() === 1)
    val row = result.first()
    assert(row.getAs[Int]("OrderYear") === 2023, "year() should extract 2023 from the date")
  }

  test("Calculated column: coalesce(phone, 'N/A') should fall back for null phone") {
    import spark.implicits._
    val testId = s"calc_coalesce_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "PhoneOrNA", "datatype": "string",
        "fieldroles": ["calculated"], "expression": "coalesce(phone, 'N/A')" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_coalesce", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq(
      (1, Some("555-1234"), testId),
      (2, None: Option[String], testId)
    ).toDF("id", "phone", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/coalesce_$testId.parquet")

    val result = runAndRead(testEntity, s"coalesce_$testId.parquet", testId)

    assert(result.count() === 2)
    val row1 = result.filter($"id" === 1).first()
    assert(row1.getAs[String]("PhoneOrNA") === "555-1234", "Non-null phone should pass through")
    val row2 = result.filter($"id" === 2).first()
    assert(row2.getAs[String]("PhoneOrNA") === "N/A", "Null phone should fall back to 'N/A'")
  }

  test("Calculated column: integer literal 950 should produce integer value 950") {
    import spark.implicits._
    val testId = s"calc_int_lit_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] },
      { "name": "", "newname": "FixedVal", "datatype": "integer",
        "fieldroles": ["calculated"], "expression": "950" }
    ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_int_lit", columnsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, testId), (2, testId))
      .toDF("id", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/int_lit_$testId.parquet")

    val result = runAndRead(testEntity, s"int_lit_$testId.parquet", testId)

    assert(result.count() === 2)
    result.collect().foreach { row =>
      assert(row.getAs[Int]("FixedVal") === 950, "Integer literal expression should produce 950")
    }
  }

  // ---------------------------------------------------------------------------
  // Transformation tests
  // ---------------------------------------------------------------------------

  test("Transformation: multi-column select with upper() and concat()") {
    import spark.implicits._
    val testId = s"trans_multi_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    // Transformation selects only 3 columns; test_id is dropped intentionally.
    // We do NOT filter by test_id afterwards — instead we keep the full result small.
    val columnsJson = """[
      { "name": "customer_id", "fieldroles": ["businesskey"] }
    ]"""

    val transformationsJson =
      """[["customer_id", "upper(name) as Name", "concat(city, ', ', country) as Location"]]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_trans_multi", columnsJson, transformationsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, "alice", "amsterdam", "netherlands", testId))
      .toDF("customer_id", "name", "city", "country", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/trans_multi_$testId.parquet")

    val proc = new Processing(testEntity, s"trans_multi_$testId.parquet")
    proc.Process(Merge)

    val silverPath = testEntity.getOutput.silver.asInstanceOf[PathLocation].path
    val result = spark.read.format("delta").load(silverPath)

    assert(result.count() >= 1, "At least one record should be written")
    val row = result.filter($"customer_id" === 1).first()
    assert(row.getAs[String]("Name") === "ALICE", "upper() should produce uppercase name")
    assert(
      row.getAs[String]("Location") === "amsterdam, netherlands",
      "concat() should produce location string"
    )
  }

  test("Transformation: wildcard with new year column using year(order_date)") {
    import spark.implicits._
    val testId = s"trans_wild_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] }
    ]"""

    val transformationsJson = """[["*", "year(order_date) as OrderYear"]]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_trans_wild", columnsJson, transformationsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, java.sql.Date.valueOf("2024-03-10"), testId))
      .toDF("id", "order_date", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/trans_wild_$testId.parquet")

    val result = runAndRead(testEntity, s"trans_wild_$testId.parquet", testId)

    assert(result.count() === 1)
    val row = result.first()
    assert(row.getAs[Int]("OrderYear") === 2024, "year() should extract 2024")
    assert(result.columns.contains("order_date"), "Wildcard should preserve original columns")
  }

  test("Transformation: two sequential transformation steps are applied in order") {
    import spark.implicits._
    val testId = s"trans_seq_${System.currentTimeMillis()}_${scala.util.Random.nextInt(10000)}"

    val columnsJson = """[
      { "name": "id", "fieldroles": ["businesskey"] }
    ]"""

    // Step 1: keep id, test_id and add UpperName from name
    // Step 2: keep everything and add NameLen from length of UpperName
    val transformationsJson =
      """[
        ["id", "test_id", "upper(name) as UpperName"],
        ["*", "length(UpperName) as NameLen"]
      ]"""

    val settings = new StringMetadataSettings()
    settings.initialize(buildMetadataJson(1, "expr_trans_seq", columnsJson, transformationsJson))
    val testEntity = new Metadata(settings).getEntity(1)
    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path

    Seq((1, "hello", testId))
      .toDF("id", "name", "test_id")
      .write.mode("overwrite").parquet(s"$bronzePath/trans_seq_$testId.parquet")

    val result = runAndRead(testEntity, s"trans_seq_$testId.parquet", testId)

    assert(result.count() === 1)
    val row = result.first()
    assert(row.getAs[String]("UpperName") === "HELLO", "Step 1 upper() should be applied")
    assert(row.getAs[Int]("NameLen") === 5, "Step 2 length() should consume step-1 output")
  }

  // ---------------------------------------------------------------------------
  // Path expression test
  // ---------------------------------------------------------------------------

  test("Path expression: $${today} resolves to current date in yyyyMMdd format") {
    val expectedDate =
      LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val metadataJson = s"""
    {
      "environment": {
        "name": "DEBUG (PATH EXPR TEST)",
        "timezone": "Europe/Amsterdam",
        "root_folder": "${testBasePath.replace("\\", "/")}",
        "raw_path": "/$${connection}/$${entity}",
        "bronze_path": "/$${connection}/$${entity}/$${today}",
        "silver_path": "/$${connection}/$${destination}/$${today}",
        "systemfield_prefix": "${randomPrefix}",
        "output_method": "paths"
      },
      "connections": [
        {
          "name": "test_conn",
          "enabled": true,
          "settings": {}
        }
      ],
      "entities": [
        {
          "id": 1,
          "name": "path_expr_entity",
          "enabled": true,
          "connection": "test_conn",
          "processtype": "merge",
          "watermark": [],
          "columns": [
            { "name": "id", "fieldroles": ["businesskey"] }
          ],
          "settings": {},
          "transformations": []
        }
      ]
    }
    """

    val settings = new StringMetadataSettings()
    settings.initialize(metadataJson)
    val testEntity = new Metadata(settings).getEntity(1)

    val bronzePath = testEntity.getOutput.bronze.asInstanceOf[PathLocation].path
    val silverPath = testEntity.getOutput.silver.asInstanceOf[PathLocation].path

    assert(
      bronzePath.contains(expectedDate),
      s"Bronze path '$bronzePath' should contain today's date '$expectedDate'"
    )
    assert(
      silverPath.contains(expectedDate),
      s"Silver path '$silverPath' should contain today's date '$expectedDate'"
    )
  }
}
