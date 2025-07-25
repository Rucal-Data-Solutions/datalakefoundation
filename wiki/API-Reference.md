# API Reference

This page provides detailed API documentation for Datalake Foundation classes and methods.

## Core Classes

### Processing

Main class for executing data processing operations.

```scala
class Processing(entity: Entity, sliceFile: String, options: Map[String, String] = Map.empty)
```

#### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `entity` | Entity | Entity metadata object |
| `sliceFile` | String | Name of the slice file to process |
| `options` | Map[String, String] | Optional processing parameters |

#### Methods

**Process() : Unit**
```scala
def Process(): Unit
```
Executes processing using the entity's configured strategy.

**Process(strategy: ProcessStrategy) : Unit**
```scala
def Process(strategy: ProcessStrategy): Unit
```
Executes processing using the specified strategy, overriding entity configuration.

Parameters:
- `strategy`: Processing strategy (Full, Merge, or Historic)

#### Usage Examples

```scala
// Basic processing with entity-configured strategy
val processing = new Processing(entity, "2025-01-01-slice.parquet")
processing.Process()

// Processing with explicit strategy
processing.Process(Full)

// Processing with options
val options = Map("processing.time" -> "2025-01-01T12:00:00")
val processing = new Processing(entity, "slice.parquet", options)
processing.Process()
```

### Entity

Represents a data entity with its metadata and configuration.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | Int | Unique entity identifier |
| `name` | String | Entity name |
| `destination` | String | Target table/path name |
| `sourceConnection` | Connection | Source connection object |
| `columns` | Array[EntityColumn] | Column definitions |
| `processingType` | ProcessStrategy | Configured processing strategy |
| `watermark` | Option[Watermark] | Watermark configuration |

#### Methods

**getPaths : EntityPaths**
```scala
def getPaths: EntityPaths
```
Returns the bronze and silver paths for this entity.

**getBusinessKeyColumns : Array[EntityColumn]**
```scala
def getBusinessKeyColumns: Array[EntityColumn]
```
Returns columns marked with business_key role.

**getPartitionColumns : Array[EntityColumn]**
```scala
def getPartitionColumns: Array[EntityColumn]
```
Returns columns marked with partition role.

**isEnabled : Boolean**
```scala
def isEnabled: Boolean
```
Returns whether the entity is enabled for processing.

#### Usage Examples

```scala
val entity = metadata.getEntity(42)
val paths = entity.getPaths
val businessKeys = entity.getBusinessKeyColumns
val isEnabled = entity.isEnabled
```

### EntityColumn

Represents a column definition within an entity.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Column name |
| `dataType` | String | Data type (integer, string, date, etc.) |
| `sourceName` | Option[String] | Source column name if different |
| `role` | Option[String] | Column role (business_key, partition, etc.) |
| `required` | Boolean | Whether column is required |
| `defaultValue` | Option[Any] | Default value for null handling |

#### Methods

**isBusinessKey : Boolean**
```scala
def isBusinessKey: Boolean
```
Returns true if column has business_key role.

**isPartition : Boolean**
```scala
def isPartition: Boolean
```
Returns true if column has partition role.

**getSparkDataType : DataType**
```scala
def getSparkDataType: DataType
```
Returns the Spark SQL DataType for this column.

#### Usage Examples

```scala
val column = entity.columns.find(_.name == "customer_id").get
val isBusinessKey = column.isBusinessKey
val sparkType = column.getSparkDataType
```

## Metadata Classes

### JsonMetadataSettings

Loads metadata configuration from JSON files.

```scala
class JsonMetadataSettings(jsonPath: String)
```

#### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `jsonPath` | String | Path to JSON metadata file |

#### Methods

**getEntity(id: Int) : Entity**
```scala
def getEntity(id: Int): Entity
```
Retrieves entity by ID. Throws exception if not found.

**getConnectionEntities(connectionId: Int) : Array[Entity]**
```scala
def getConnectionEntities(connectionId: Int): Array[Entity]
```
Returns all entities for a specific connection.

**getGroupEntities(group: String) : Array[Entity]**
```scala
def getGroupEntities(group: String): Array[Entity]
```
Returns all entities in a logical group.

**getConnection(id: Int) : Connection**
```scala
def getConnection(id: Int): Connection
```
Retrieves connection by ID.

**getConnectionByName(name: String) : Connection**
```scala
def getConnectionByName(name: String): Connection
```
Retrieves connection by name.

**getEnvironment() : Environment**
```scala
def getEnvironment(): Environment
```
Returns environment configuration.

#### Usage Examples

```scala
val metadata = new JsonMetadataSettings("/path/to/metadata.json")
val entity = metadata.getEntity(42)
val entities = metadata.getConnectionEntities(1)
val connection = metadata.getConnectionByName("production_db")
```

### SqlMetadataSettings

Loads metadata configuration from SQL Server.

```scala
class SqlMetadataSettings(sqlSettings: SqlServerSettings, functionName: String)
```

#### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `sqlSettings` | SqlServerSettings | SQL Server connection settings |
| `functionName` | String | Function name to retrieve JSON config |

#### Methods

Same methods as JsonMetadataSettings, but loads configuration from SQL Server.

#### Usage Examples

```scala
val sqlSettings = SqlServerSettings(
  server = "myserver.database.windows.net",
  port = 1433,
  database = "config_db", 
  username = "user",
  password = "password"
)

val metadata = new SqlMetadataSettings(sqlSettings, "cfg.fnGetFoundationConfig()")
val entity = metadata.getEntity(42)
```

## Processing Strategies

### ProcessStrategy

Base trait for all processing strategies.

```scala
trait ProcessStrategy
```

### Full

Full load processing strategy.

```scala
object Full extends ProcessStrategy
```

Performs complete overwrite of target table.

### Merge

Incremental merge processing strategy.

```scala
object Merge extends ProcessStrategy
```

Performs Delta Lake MERGE operation with upsert logic.

### Historic

SCD Type 2 historic processing strategy.

```scala
object Historic extends ProcessStrategy
```

Maintains full history with validity tracking.

## Data Factory Integration

### DataFactoryItemGenerator

Generates processing items for orchestration tools.

```scala
class DataFactoryItemGenerator(metadata: MetadataSettings)
```

#### Methods

**generateItems(date: String) : Array[ProcessingItem]**
```scala
def generateItems(date: String): Array[ProcessingItem]
```
Generates items for a specific date.

**generateItems(startDate: String, endDate: String) : Array[ProcessingItem]**
```scala
def generateItems(startDate: String, endDate: String): Array[ProcessingItem]
```
Generates items for a date range.

**generateItemsForConnection(connectionId: Int, date: String) : Array[ProcessingItem]**
```scala
def generateItemsForConnection(connectionId: Int, date: String): Array[ProcessingItem]
```
Generates items for a specific connection.

#### Usage Examples

```scala
val generator = new DataFactoryItemGenerator(metadata)
val items = generator.generateItems("2025-01-01")
val connectionItems = generator.generateItemsForConnection(1, "2025-01-01")
```

## Helper Classes

### EntityPaths

Container for entity storage paths.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `bronzepath` | String | Bronze layer path |
| `silverpath` | String | Silver layer path |

### Watermark

Watermark configuration for incremental processing.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `column` | String | Watermark column name |
| `dataType` | String | Column data type |
| `format` | Option[String] | Date/timestamp format |
| `initialValue` | Option[String] | Initial watermark value |

### Connection

Connection configuration object.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | Int | Connection identifier |
| `name` | String | Connection name |
| `connectionType` | Option[String] | Connection type |
| `settings` | Map[String, Any] | Connection settings |

### Environment

Environment configuration object.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `timezone` | String | Default timezone |
| `outputMethod` | String | Output method (paths/catalog) |
| `systemPrefix` | String | System column prefix |
| `settings` | Map[String, Any] | Additional environment settings |

## Exception Classes

### DatalakeException

Base exception class for library-specific errors.

```scala
class DatalakeException(message: String, cause: Throwable = null) extends Exception(message, cause)
```

### MetadataException

Exception for metadata-related errors.

```scala
class MetadataException(message: String, cause: Throwable = null) extends DatalakeException(message, cause)
```

### ProcessingException

Exception for processing-related errors.

```scala
class ProcessingException(message: String, cause: Throwable = null) extends DatalakeException(message, cause)
```

### ValidationException

Exception for validation errors.

```scala
class ValidationException(message: String, cause: Throwable = null) extends DatalakeException(message, cause)
```

## Logging

### LogManager

Provides logging functionality throughout the library.

```scala
object LogManager {
  def getLogger(name: String): Logger
}
```

#### Usage Examples

```scala
import datalake.logging.LogManager

val logger = LogManager.getLogger("MyProcessor")
logger.info("Processing started")
logger.error("Processing failed", exception)
logger.warn("Schema drift detected")
```

## Configuration Classes

### SqlServerSettings

Configuration for SQL Server connections.

```scala
case class SqlServerSettings(
  server: String,
  port: Int, 
  database: String,
  username: String,
  password: String
)
```

## Utility Functions

### Expression Evaluation

**evaluateExpression(expression: String, variables: Map[String, String]) : String**

Evaluates expressions with variable substitution.

```scala
val result = evaluateExpression(
  "/bronze/${connection}/${entity}", 
  Map("connection" -> "prod", "entity" -> "customer")
)
// Result: "/bronze/prod/customer"
```

### Hash Generation

**generateSourceHash(row: Row, columns: Array[String]) : String**

Generates SHA-256 hash for change detection.

```scala
val hash = generateSourceHash(row, Array("id", "name", "email"))
```

## Type Definitions

### Data Types

Supported data types for column definitions:

- `integer` - 32-bit signed integer
- `long` - 64-bit signed integer  
- `string` - Variable-length string
- `date` - Date (year, month, day)
- `timestamp` - Timestamp with timezone
- `decimal` - Decimal number with precision/scale
- `boolean` - True/false value
- `binary` - Binary data

### Processing Options

Available processing options:

| Option | Type | Description |
|--------|------|-------------|
| `processing.time` | String | ISO-8601 timestamp for processing time |
| `validation.enabled` | Boolean | Enable/disable validation |
| `schema.evolution` | String | Schema evolution strategy |
| `partition.pruning` | Boolean | Enable partition pruning |

## Best Practices

### Error Handling

Always wrap processing calls in try-catch blocks:

```scala
try {
  val processing = new Processing(entity, sliceFile)
  processing.Process()
} catch {
  case e: ProcessingException =>
    logger.error(s"Processing failed: ${e.getMessage}", e)
    // Handle processing-specific error
  case e: ValidationException =>
    logger.error(s"Validation failed: ${e.getMessage}", e)
    // Handle validation error
  case e: Exception =>
    logger.error(s"Unexpected error: ${e.getMessage}", e)
    // Handle unexpected error
}
```

### Resource Management

Ensure proper resource cleanup:

```scala
// Use try-with-resources pattern for metadata connections
val metadata = new SqlMetadataSettings(sqlSettings, functionName)
try {
  // Use metadata
} finally {
  // Cleanup if needed
}
```

### Performance Optimization

- **Reuse metadata objects** when processing multiple entities
- **Batch process related entities** to improve efficiency  
- **Monitor memory usage** for large datasets
- **Use appropriate Spark configurations** for your workload

## Related Topics

- [Processing Strategies](Processing-Strategies.md) - Detailed strategy documentation
- [Metadata Management](Metadata-Management.md) - Metadata configuration guide
- [Configuration Reference](Configuration-Reference.md) - Complete configuration options