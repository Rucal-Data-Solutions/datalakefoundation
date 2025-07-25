# Quick Start Guide

Get up and running with Datalake Foundation quickly using this step-by-step guide.

## Prerequisites

Before starting, ensure you have:
- Databricks Runtime 16.4 LTS or later
- Access to Bronze and Silver storage layers
- Basic familiarity with Scala and Spark

## Step 1: Basic Setup

### Import Required Classes
```scala
import datalake.processing.Processing
import datalake.metadata.JsonMetadataSettings
import datalake.processing.strategies.{Full, Merge, Historic}
```

### Create Metadata Configuration

Create a simple JSON metadata file (`metadata.json`):

```json
{
  "environment": {
    "timezone": "UTC",
    "output_method": "paths"
  },
  "connections": [
    {
      "id": 1,
      "name": "quickstart_demo",
      "settings": {
        "bronze_path": "/mnt/datalake/bronze/quickstart_demo",
        "silver_path": "/mnt/datalake/silver/quickstart_demo"
      }
    }
  ],
  "entities": [
    {
      "id": 1,
      "name": "customer",
      "source_connection": 1,
      "destination": "dim_customer",
      "processing_type": "Merge",
      "columns": [
        {
          "name": "customer_id",
          "type": "integer",
          "role": "business_key"
        },
        {
          "name": "customer_name",
          "type": "string"
        },
        {
          "name": "email",
          "type": "string"
        },
        {
          "name": "created_date",
          "type": "date"
        }
      ]
    }
  ]
}
```

## Step 2: Prepare Sample Data

### Create Bronze Layer Data

Create a sample Parquet file in your Bronze layer:

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// Define schema
val schema = StructType(Array(
  StructField("customer_id", IntegerType, false),
  StructField("customer_name", StringType, true),
  StructField("email", StringType, true),
  StructField("created_date", DateType, true)
))

// Create sample data
val data = Seq(
  Row(1, "John Doe", "john@example.com", java.sql.Date.valueOf("2025-01-01")),
  Row(2, "Jane Smith", "jane@example.com", java.sql.Date.valueOf("2025-01-02")),
  Row(3, "Bob Johnson", "bob@example.com", java.sql.Date.valueOf("2025-01-03"))
)

// Create DataFrame and save as Parquet
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
df.write.mode("overwrite").parquet("/mnt/datalake/bronze/quickstart_demo/customer/2025-01-01-slice.parquet")
```

## Step 3: Process Your First Data Slice

### Load Metadata and Process
```scala
// Initialize metadata
val metadata = new JsonMetadataSettings("/path/to/metadata.json")

// Get the entity
val entity = metadata.getEntity(1)

// Create processing instance
val processing = new Processing(entity, "2025-01-01-slice.parquet")

// Process the data (uses entity's configured strategy)
processing.Process()
```

### Verify Results
```scala
// Check the Silver layer table
val silverPath = entity.getPaths.silverpath
val result = spark.read.format("delta").load(silverPath)
result.show()

// Expected output:
// +------------+-------------+-------------------+------------+----------+----------+-----------+
// |customer_id |customer_name|email              |created_date|ValidFrom |ValidTo   |IsCurrent  |
// +------------+-------------+-------------------+------------+----------+----------+-----------+
// |1           |John Doe     |john@example.com   |2025-01-01  |2025-01-01|null      |true       |
// |2           |Jane Smith   |jane@example.com   |2025-01-02  |2025-01-01|null      |true       |
// |3           |Bob Johnson  |bob@example.com    |2025-01-03  |2025-01-01|null      |true       |
// +------------+-------------+-------------------+------------+----------+----------+-----------+
```

## Step 4: Process Incremental Updates

### Create Updated Data
```scala
// Create updated sample data with changes
val updatedData = Seq(
  Row(1, "John Doe Updated", "john.doe@example.com", java.sql.Date.valueOf("2025-01-01")),
  Row(2, "Jane Smith", "jane@example.com", java.sql.Date.valueOf("2025-01-02")), // No change
  Row(4, "Alice Brown", "alice@example.com", java.sql.Date.valueOf("2025-01-04")) // New record
)

val updatedDf = spark.createDataFrame(spark.sparkContext.parallelize(updatedData), schema)
updatedDf.write.mode("overwrite").parquet("/mnt/datalake/bronze/quickstart_demo/customer/2025-01-02-slice.parquet")
```

### Process the Update
```scala
// Process the incremental slice
val incrementalProcessing = new Processing(entity, "2025-01-02-slice.parquet")
incrementalProcessing.Process()

// Verify the merge results
val updatedResult = spark.read.format("delta").load(silverPath)
updatedResult.show()

// You should see:
// - John Doe's record updated with new name and email
// - Jane Smith's record unchanged but with updated lastSeen
// - Alice Brown as a new record
```

## Step 5: Explore Different Processing Strategies

### Full Load Processing
```scala
// Override to use Full processing strategy
val fullProcessing = new Processing(entity, "2025-01-01-slice.parquet")
fullProcessing.Process(Full) // Completely replaces the Silver table
```

### Historic (SCD Type 2) Processing

First, update your entity configuration to use Historic processing:

```json
{
  "id": 1,
  "name": "customer",
  "processing_type": "Historic",
  // ... other configuration
}
```

Then process with history tracking:
```scala
val historicProcessing = new Processing(entity, "2025-01-01-slice.parquet")
historicProcessing.Process(Historic)

// Process updates - this will maintain history
val historicUpdate = new Processing(entity, "2025-01-02-slice.parquet")
historicUpdate.Process(Historic)

// View historic data
val historicResult = spark.read.format("delta").load(silverPath)
historicResult.orderBy("customer_id", "ValidFrom").show()
```

## Step 6: Working with Processing Time

### Override Processing Time
```scala
val options = Map("processing.time" -> "2025-01-15T10:30:00")
val timedProcessing = new Processing(entity, "2025-01-01-slice.parquet", options)
timedProcessing.Process()
```

## Common Patterns

### Batch Processing Multiple Slices
```scala
val sliceFiles = Array("2025-01-01-slice.parquet", "2025-01-02-slice.parquet", "2025-01-03-slice.parquet")

sliceFiles.foreach { sliceFile =>
  val processing = new Processing(entity, sliceFile)
  processing.Process()
  println(s"Processed: $sliceFile")
}
```

### Error Handling
```scala
try {
  val processing = new Processing(entity, "2025-01-01-slice.parquet")
  processing.Process()
  println("Processing completed successfully")
} catch {
  case e: Exception =>
    println(s"Processing failed: ${e.getMessage}")
    // Handle error appropriately
}
```

### Dynamic Entity Processing
```scala
// Process all entities for a connection
val connectionEntities = metadata.getConnectionEntities(1)

connectionEntities.foreach { entity =>
  try {
    val processing = new Processing(entity, "2025-01-01-slice.parquet")
    processing.Process()
    println(s"Processed entity: ${entity.name}")
  } catch {
    case e: Exception =>
      println(s"Failed to process ${entity.name}: ${e.getMessage}")
  }
}
```

## Troubleshooting Quick Start Issues

### File Not Found
- Verify Bronze layer paths are correct
- Check that Parquet files exist in the expected location
- Ensure proper permissions for reading Bronze and writing Silver

### Schema Mismatch
- Verify column definitions in metadata match your Parquet files
- Check data types are correctly specified
- Ensure business key columns are properly defined

### Processing Errors
- Check Databricks cluster has Delta Lake support
- Verify sufficient resources for processing
- Enable detailed logging for debugging

## Next Steps

Now that you've completed the quick start:

1. **Explore Advanced Features**:
   - [Processing Strategies](Processing-Strategies.md) - Detailed strategy documentation
   - [Metadata Management](Metadata-Management.md) - Advanced metadata configuration
   - [Transformations](Transformations.md) - Custom data transformations

2. **Production Considerations**:
   - [Configuration Reference](Configuration-Reference.md) - Production configuration options
   - [Migration Guide](Migration-Guide.md) - Upgrading existing implementations

3. **Integration**:
   - [Data Factory Integration](Data-Factory-Integration.md) - Orchestration with Azure Data Factory

## Sample Code Repository

For complete working examples, check the test files in the repository:
- `src/test/scala/example/` - Contains sample metadata and processing examples
- `src/test/scala/BenchmarkSpec.scala` - Integration test examples

---

*You're now ready to use Datalake Foundation in your data processing pipelines!*