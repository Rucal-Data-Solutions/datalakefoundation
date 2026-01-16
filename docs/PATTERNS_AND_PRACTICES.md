# Datalake Foundation - Patterns and Practices

This document describes the architectural patterns, design patterns, and coding practices used throughout the Datalake Foundation codebase.

## Table of Contents

- [Project Structure](#project-structure)
- [Design Patterns](#design-patterns)
- [Scala-Specific Patterns](#scala-specific-patterns)
- [Configuration Management](#configuration-management)
- [Error Handling](#error-handling)
- [Data Processing Patterns](#data-processing-patterns)
- [Logging and Monitoring](#logging-and-monitoring)
- [Testing Patterns](#testing-patterns)

---

## Project Structure

The project follows a **layered architecture** pattern:

```
datalake/
├── core/          # Core utilities and base patterns
├── log/           # Logging infrastructure
├── metadata/      # Configuration and metadata management
├── outputs/       # Output factories
└── processing/    # Data processing strategies
```

**Architecture Layers:**
- **Metadata Layer**: Configuration management via JSON, SQL Server, or strings
- **Processing Layer**: Strategy-based data transformation
- **Core Layer**: Utility functions and base abstractions
- **Logging Layer**: Centralized log management with Log4j integration

---

## Design Patterns

### Strategy Pattern

The primary pattern for processing logic, enabling runtime algorithm selection.

**Location:** [ProcessStrategy.scala](../src/main/scala/datalake/processing/ProcessStrategy.scala)

```scala
abstract class ProcessStrategy {
  def Process(processing: Processing)(implicit spark: SparkSession): Unit
}

final object Full extends ProcessStrategy { ... }
final object Merge extends ProcessStrategy { ... }
final object Historic extends ProcessStrategy { ... }
```

**Strategy Selection:** [Entity.scala:104-113](../src/main/scala/datalake/metadata/Entity.scala#L104-L113)

```scala
final def ProcessType: ProcessStrategy =
  this.processtype.toLowerCase match {
    case Full.Name     => Full
    case Merge.Name    => Merge
    case Historic.Name => Historic
    case "delta"       => Merge // backward compatibility
  }
```

### Factory Pattern

Used for polymorphic entity creation based on input type.

**Location:** [datafactory.scala](../src/main/scala/datalake/outputs/datafactory.scala)

```scala
object DataFactory {
  def getConfigItems(arg: Any)(implicit metadata: Metadata): String = {
    val entities: List[Entity] = arg match {
      case group: EntityGroup => metadata.getEntities(group)
      case connection: Connection => metadata.getEntities(connection)
      case entityId: Int => metadata.getEntities(entityId)
      case entities: Array[Int] => metadata.getEntities(entities)
    }
    write(entities)
  }
}
```

### Builder Pattern

Applied with Delta Lake merge operations for fluent API construction.

**Location:** [Merge.scala](../src/main/scala/datalake/processing/Merge.scala), [Historic.scala](../src/main/scala/datalake/processing/Historic.scala)

```scala
val mergeBuilder = deltaTable
  .as("target")
  .merge(source.as("source"), ...)
  .whenMatched(...)
  .update(...)
  .whenNotMatched(...)
  .insertAll()
```

### Template Method Pattern

Composable DataFrame transformations through chained transforms.

**Location:** [Processing.scala:96-108](../src/main/scala/datalake/processing/Processing.scala#L96-L108)

```scala
val transformedDF = dfSlice
  .transform(injectTransformations)
  .transform(addCalculatedColumns)
  .transform(calculateSourceHash)
  .transform(addTemporalTrackingColumns)
  .transform(addFilenameColumn(_, sliceFile))
  .transform(addPrimaryKey)
  .transform(castColumns)
  .transform(renameColumns)
  .transform(addDeletedColumn)
  .transform(addLastSeen)
  .datalakeNormalize()
  .cache()
```

### Singleton Pattern

Scala objects provide natural singleton implementations for strategies.

```scala
final object Full extends ProcessStrategy { ... }
final object Merge extends ProcessStrategy { ... }
final object Historic extends ProcessStrategy { ... }
```

### Custom Serializer Pattern

JSON4S custom serializers for complex domain object serialization.

**Location:** [Entity.scala:285-361](../src/main/scala/datalake/metadata/Entity.scala#L285-L361)

```scala
class EntitySerializer(metadata: datalake.metadata.Metadata)
    extends CustomSerializer[Entity](implicit formats =>
      (
        { case j: JObject => new Entity(...) },
        { case entity: Entity => JObject(...) }
      )
    )
```

---

## Scala-Specific Patterns

### Implicit Parameters

Context threading without explicit parameter passing.

```scala
private def calculateSourceHash(input: Dataset[Row])(implicit env: Environment): Dataset[Row]
private def addTemporalTrackingColumns(input: Dataset[Row])(implicit env: Environment): Dataset[Row]
```

### Implicit Classes (Extension Methods)

Adding methods to existing types via "Pimp My Library" pattern.

**Location:** [implicits.scala](../src/main/scala/datalake/core/implicits.scala)

```scala
object implicits {
  implicit class DatalakeDataFrame(df: DataFrame) {
    def datalakeNormalize(): DataFrame = { ... }
    def datalakeSchemaCompare(targetSchema: StructType): Array[(DatalakeColumn, String)] = { ... }
  }

  implicit class StringFunctions(str: String) {
    def normalizedPath: String = { ... }
  }
}
```

### Sealed Traits with Case Classes

Enabling exhaustive pattern matching with compiler checks.

**Location:** [OutputMethod.scala](../src/main/scala/datalake/metadata/OutputMethod.scala)

```scala
sealed trait OutputLocation extends Serializable { def value: String }
final case class PathLocation(path: String) extends OutputLocation
final case class TableLocation(table: String) extends OutputLocation
```

### Option/Try Handling

Functional error handling throughout the codebase.

```scala
Try {
  tempdf.withColumn(column.Name, expr(column.Expression))
} match {
  case Success(newDf) => newDf
  case Failure(e) =>
    logger.error(s"Failed to add calculated column ${column.Name}...", e)
    tempdf
}
```

### Higher-Order Functions

Functional collection operations with `foldLeft`, `map`, `filter`.

```scala
private def addCalculatedColumns(input: Dataset[Row]): Dataset[Row] =
  entity.Columns(EntityColumnFilter(HasExpression=true)).foldLeft(input) { (tempdf, column) =>
    // transformation logic
  }
```

---

## Configuration Management

### Metadata Settings Hierarchy

Abstract base class with multiple implementations for different configuration sources.

**Location:** [DatalakeMetadataSettings.scala](../src/main/scala/datalake/metadata/DatalakeMetadataSettings.scala)

| Implementation | Source |
|----------------|--------|
| `JsonMetadataSettings` | JSON file |
| `SqlMetadataSettings` | SQL Server database |
| `StringMetadataSettings` | Direct string input |

### Environment Configuration

Immutable case class with sensible defaults.

**Location:** [Environment.scala](../src/main/scala/datalake/metadata/Environment.scala)

```scala
case class Environment(
  private val name: String,
  private val root_folder: String,
  private val timezone: String,
  private val raw_path: String,
  private val bronze_path: String,
  private val silver_path: String,
  private val secure_container_suffix: Option[String] = None,
  private val systemfield_prefix: Option[String] = None,
  private val output_method: String = "paths",
  private val log_level: String = "WARN"
) extends Serializable
```

### Settings Merging

Connection-level settings merged with Entity-level settings (Entity overrides).

```scala
final def Settings: Map[String, Any] = {
  val mergedSettings = this.Connection.settings merge this.settings
  mergedSettings.values
}
```

---

## Error Handling

### Custom Exception Hierarchy

Base exception with automatic logging on creation.

**Location:** [DatalakeException.scala](../src/main/scala/datalake/core/DatalakeException.scala)

```scala
class DatalakeException(msg: String, lvl: Level) extends Exception(msg) {
  @transient private lazy val logger = LogManager.getLogger(this.getClass)
  logger.log(lvl, msg, this)
}
```

### Specific Exception Types

**Location:** [metadata.scala](../src/main/scala/datalake/metadata/metadata.scala)

| Exception | Purpose |
|-----------|---------|
| `MetadataNotInitializedException` | Metadata not loaded |
| `EntityNotFoundException` | Entity lookup failed |
| `ConnectionNotFoundException` | Connection lookup failed |
| `ProcessStrategyNotSupportedException` | Invalid process type |

### Resource Cleanup

Try-catch-finally pattern with proper resource cleanup.

```scala
try {
  strategy.Process(this)
  WriteWatermark(getSource.watermark_values)
}
catch {
  case e:Throwable => {
    logger.error("Unhandled exception during processing", e)
    throw e
  }
}
finally {
  _cachedSource.foreach { source =>
    source.source_df.unpersist()
  }
  _cachedSource = None
}
```

---

## Data Processing Patterns

### Transform Chain Pattern

Composable DataFrame transformations using Spark's `.transform()` method.

### DataFrame Caching with Memoization

Preventing duplicate DataFrame materialization.

**Location:** [Processing.scala](../src/main/scala/datalake/processing/Processing.scala)

```scala
private var _cachedSource: Option[DatalakeSource] = None

def getSource: DatalakeSource = {
  _cachedSource.getOrElse {
    val source = // ... build source ...
    _cachedSource = Some(source)
    source
  }
}
```

### Watermark Window Conditions

Building range conditions for incremental data scope definition.

**Location:** [ProcessStrategy.scala:52-119](../src/main/scala/datalake/processing/ProcessStrategy.scala#L52-L119)

### Dual Output Support

Polymorphic output handling supporting both file paths and Databricks Unity Catalog.

```scala
destination match {
  case PathLocation(path) => spark.read.parquet(s"$path/$sliceFile")
  case TableLocation(table) => spark.read.table(table)
}
```

### SCD Type 2 Implementation

Slowly Changing Dimension tracking with ValidTo and IsCurrent fields.

**Location:** [Historic.scala:100-142](../src/main/scala/datalake/processing/Historic.scala#L100-L142)

### Delta Lake Merge with Conditional Logic

Multi-condition merge for upserts with soft-delete and source hash tracking.

**Location:** [Merge.scala:111-139](../src/main/scala/datalake/processing/Merge.scala#L111-L139)

---

## Logging and Monitoring

### Centralized Log Manager

Factory for logger creation with environment configuration.

**Location:** [DatalakeLogManager.scala](../src/main/scala/datalake/log/DatalakeLogManager.scala)

```scala
object DatalakeLogManager {
  def getLogger(cls: Class[_])(implicit spark: SparkSession): Logger = {
    Log4jConfigurator.init(spark)
    LogManager.getLogger(cls)
  }
}
```

### Lazy Logger Initialization

Loggers are `@transient` and `lazy` for proper serialization.

```scala
@transient
private lazy val logger: Logger = DatalakeLogManager.getLogger(this.getClass, environment)
```

### Dynamic Log Level Configuration

Thread-safe initialization with environment-driven log levels.

**Location:** [Log4jConfigurator.scala](../src/main/scala/datalake/log/Log4jConfigurator.scala)

---

## Testing Patterns

### ScalaTest with FunSuite

Using ScalaTest's fluent assertion syntax.

**Location:** [DatalakeMetadataSettingsSpec.scala](../src/test/scala/datalake/metadata/DatalakeMetadataSettingsSpec.scala)

```scala
class DatalakeMetadataSettingsSpec extends AnyFunSuite with SparkSessionTest {
  test("getEntitiesById(id: Int) should return Some(Entity)") {
    val result = settings.getEntitiesById(1)
    result shouldBe defined
    result.get.Id shouldBe 1
  }
}
```

### Fixture-Based Tests

Shared Spark fixtures via `SparkSessionTest` trait.

### Comprehensive Data Processing Tests

Full pipeline verification with data assertions.

**Location:** [MergeProcessingSpec.scala](../src/test/scala/datalake/processing/MergeProcessingSpec.scala)

---

## Pattern Summary

| Pattern | Primary Location | Purpose |
|---------|------------------|---------|
| Strategy | ProcessStrategy.scala | Runtime algorithm selection |
| Factory | datafactory.scala | Polymorphic entity creation |
| Builder | Merge.scala, Historic.scala | Delta Lake merge construction |
| Template Method | Processing.scala | Chained transformations |
| Singleton | Full/Merge/Historic.scala | Shared strategy instances |
| Custom Serializer | Entity.scala | JSON domain serialization |
| Implicit Parameters | Multiple files | Lightweight DI |
| Implicit Classes | implicits.scala | Extension methods |
| Sealed Traits | OutputMethod.scala | Exhaustive matching |
| Memoization | Processing.scala | DataFrame caching |
| SCD Type 2 | Historic.scala | Dimension tracking |
