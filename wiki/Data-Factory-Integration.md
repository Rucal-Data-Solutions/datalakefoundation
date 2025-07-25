# Data Factory Integration

Datalake Foundation provides structured data for integration with orchestration tools like Azure Data Factory (ADF). The Data Factory item generator creates organized lists of entities and slices for use in dynamic pipeline execution.

## Overview

The Data Factory integration enables:
- **Dynamic pipeline generation** based on metadata
- **Structured entity processing** through ADF For Each activities
- **Date-based slice processing** for incremental loads
- **Error handling and retry logic** in orchestration
- **Parallel processing** of multiple entities

## Item Generator

### Basic Usage

Generate a list of processing items for ADF consumption:

```scala
import datalake.datafactory.DataFactoryItemGenerator
import datalake.metadata.JsonMetadataSettings

// Initialize metadata and generator
val metadata = new JsonMetadataSettings("metadata.json")
val generator = new DataFactoryItemGenerator(metadata)

// Generate items for a specific date
val items = generator.generateItems("2025-01-01")

// Generate items for a date range
val rangeItems = generator.generateItems("2025-01-01", "2025-01-31")
```

### Generated Item Structure

Each item contains the information needed for processing:

```json
{
  "connection_id": 1,
  "connection_name": "production_db", 
  "entity_id": 42,
  "entity_name": "customer",
  "destination": "dim_customer",
  "slice_file": "2025-01-01-slice.parquet",
  "processing_type": "Merge",
  "bronze_path": "/bronze/production_db/customer",
  "silver_path": "/silver/production_db/dim_customer",
  "metadata_json": "{...}"  // Serialized entity metadata
}
```

## Azure Data Factory Pipeline Integration

### Pipeline Structure

Create an ADF pipeline with the following structure:

1. **Lookup Activity** - Call the item generator
2. **For Each Activity** - Process each item
3. **Databricks Notebook Activity** - Execute processing
4. **Error Handling** - Manage failures and retries

### Sample ADF Pipeline

```json
{
  "name": "DatalakeFoundation_Processing",
  "properties": {
    "activities": [
      {
        "name": "GetProcessingItems",
        "type": "Lookup",
        "linkedServiceName": {
          "referenceName": "DatabricksLinkedService",
          "type": "LinkedServiceReference"
        },
        "typeProperties": {
          "source": {
            "type": "DatabricksSource",
            "query": "SELECT * FROM get_processing_items('2025-01-01')"
          },
          "firstRowOnly": false
        }
      },
      {
        "name": "ProcessEntities",
        "type": "ForEach",
        "dependsOn": [
          {
            "activity": "GetProcessingItems",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "isSequential": false,
          "batchCount": 10,
          "items": {
            "value": "@activity('GetProcessingItems').output.value",
            "type": "Expression"
          },
          "activities": [
            {
              "name": "ProcessEntity",
              "type": "DatabricksNotebook",
              "typeProperties": {
                "notebookPath": "/notebooks/ProcessEntity",
                "baseParameters": {
                  "entity_json": "@item().metadata_json",
                  "slice_file": "@item().slice_file"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
```

### Databricks Notebook Processing

Create a Databricks notebook to handle individual entity processing:

```scala
// ProcessEntity notebook
// Get parameters from ADF
val entityJson = dbutils.widgets.get("entity_json")
val sliceFile = dbutils.widgets.get("slice_file")

// Parse entity metadata
import datalake.metadata.JsonMetadataSettings
import datalake.processing.Processing
import com.fasterxml.jackson.databind.ObjectMapper

val mapper = new ObjectMapper()
val entityData = mapper.readTree(entityJson)

// Create entity and process
val metadata = new JsonMetadataSettings(entityData)
val entity = metadata.getEntity(entityData.get("id").asInt())
val processing = new Processing(entity, sliceFile)

try {
  processing.Process()
  println(s"Successfully processed ${entity.name} - ${sliceFile}")
} catch {
  case e: Exception =>
    println(s"Failed to process ${entity.name} - ${sliceFile}: ${e.getMessage}")
    throw e
}
```

## Advanced Configuration

### Filtered Item Generation

Generate items for specific entities or connections:

```scala
// Filter by connection
val connectionItems = generator.generateItemsForConnection(1, "2025-01-01")

// Filter by entity group
val groupItems = generator.generateItemsForGroup("customer_data", "2025-01-01")

// Filter by entity list
val entityIds = Array(1, 2, 5, 10)
val selectedItems = generator.generateItemsForEntities(entityIds, "2025-01-01")
```

### Custom Date Ranges

Handle different date range scenarios:

```scala
// Single date
val singleDate = generator.generateItems("2025-01-01")

// Date range
val dateRange = generator.generateItems("2025-01-01", "2025-01-07")

// Month processing
val monthItems = generator.generateItemsForMonth("2025-01")

// Dynamic date (yesterday)
val yesterday = LocalDate.now().minusDays(1).toString
val yesterdayItems = generator.generateItems(yesterday)
```

### Conditional Processing

Include conditions for selective processing:

```scala
// Generate items with conditions
val conditionalItems = generator.generateItemsWithConditions(
  date = "2025-01-01",
  conditions = Map(
    "min_file_size" -> "1MB",
    "max_age_hours" -> "24",
    "required_dependencies" -> "dim_date,dim_time"
  )
)
```

## Error Handling and Retry Logic

### ADF Error Handling

Configure retry policies in ADF:

```json
{
  "name": "ProcessEntity",
  "type": "DatabricksNotebook",
  "retryPolicy": {
    "count": 3,
    "intervalInSeconds": 300
  },
  "typeProperties": {
    "notebookPath": "/notebooks/ProcessEntity",
    "baseParameters": {
      "entity_json": "@item().metadata_json",
      "slice_file": "@item().slice_file"
    }
  },
  "onInactiveMarkAs": "Failed",
  "onFailure": [
    {
      "activity": "LogFailure",
      "dependencyConditions": ["Failed"]
    }
  ]
}
```

### Custom Error Handling

Implement detailed error handling in notebooks:

```scala
// Enhanced error handling
def processEntityWithRetry(entity: Entity, sliceFile: String, maxRetries: Int = 3): Boolean = {
  var attempt = 0
  var success = false
  
  while (attempt < maxRetries && !success) {
    try {
      attempt += 1
      val processing = new Processing(entity, sliceFile)
      processing.Process()
      success = true
      println(s"Success on attempt $attempt: ${entity.name}")
    } catch {
      case e: Exception =>
        println(s"Attempt $attempt failed for ${entity.name}: ${e.getMessage}")
        if (attempt >= maxRetries) {
          throw e
        } else {
          Thread.sleep(30000 * attempt) // Exponential backoff
        }
    }
  }
  success
}
```

## Monitoring and Logging

### ADF Monitoring

Monitor pipeline execution through ADF:

```json
{
  "name": "LogProcessingStart",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://your-logging-endpoint.com/log",
    "method": "POST",
    "body": {
      "pipeline": "@pipeline().RunId",
      "entity": "@item().entity_name",
      "slice": "@item().slice_file",
      "status": "started",
      "timestamp": "@utcnow()"
    }
  }
}
```

### Custom Logging

Implement detailed logging in processing notebooks:

```scala
import datalake.logging.LogManager

val logger = LogManager.getLogger("DataFactoryProcessing")

// Log processing start
logger.info(s"Starting processing for ${entity.name}, slice: ${sliceFile}")

// Log processing completion
logger.info(s"Completed processing for ${entity.name}, records processed: ${recordCount}")

// Log errors
logger.error(s"Processing failed for ${entity.name}: ${exception.getMessage}", exception)
```

## Performance Optimization

### Parallel Processing

Configure optimal parallelism in ADF:

```json
{
  "name": "ProcessEntities",
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,
    "batchCount": 20,  // Process up to 20 entities in parallel
    "items": "@activity('GetProcessingItems').output.value"
  }
}
```

### Resource Management

Optimize Databricks cluster usage:

```scala
// Configure cluster auto-scaling in ADF linked service
{
  "name": "DatabricksLinkedService",
  "properties": {
    "type": "AzureDatabricks",
    "typeProperties": {
      "domain": "https://your-workspace.azuredatabricks.net",
      "newClusterNodeType": "Standard_DS3_v2",
      "newClusterNumOfWorker": "2:8",  // Auto-scale between 2-8 workers
      "newClusterSparkEnvVars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      }
    }
  }
}
```

### Batch Processing

Process multiple slices in batches:

```scala
// Batch processing approach
val batchSize = 10
val slices = generator.generateItems("2025-01-01", "2025-01-31")
val batches = slices.grouped(batchSize)

batches.foreach { batch =>
  batch.par.foreach { item =>
    processEntity(item.entity, item.sliceFile)
  }
}
```

## Integration Patterns

### Event-Driven Processing

Trigger processing based on file arrival:

```json
{
  "name": "BlobTrigger",
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/bronze/",
    "blobPathEndsWith": "-slice.parquet",
    "ignoreEmptyBlobs": true,
    "events": ["Microsoft.Storage.BlobCreated"]
  }
}
```

### Scheduled Processing

Regular scheduled processing:

```json
{
  "name": "DailyProcessingTrigger",
  "type": "ScheduleTrigger",
  "typeProperties": {
    "recurrence": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2025-01-01T02:00:00Z",
      "timeZone": "UTC"
    }
  }
}
```

### Dependency Management

Handle processing dependencies:

```scala
// Check dependencies before processing
def checkDependencies(entity: Entity, date: String): Boolean = {
  val requiredTables = entity.dependencies
  
  requiredTables.forall { table =>
    val exists = spark.catalog.tableExists(table)
    val hasData = if (exists) {
      spark.table(table).filter(s"partition_date = '$date'").count() > 0
    } else false
    
    if (!hasData) {
      logger.warn(s"Dependency not met: $table for date $date")
    }
    hasData
  }
}
```

## Best Practices

### Pipeline Design

1. **Modular notebooks** - Keep processing logic separate from orchestration
2. **Parameter validation** - Validate all input parameters
3. **Comprehensive logging** - Log all significant events and errors
4. **Resource optimization** - Use appropriate cluster sizes and auto-scaling

### Error Management

1. **Graceful degradation** - Continue processing other entities on individual failures
2. **Detailed error messages** - Include context for troubleshooting
3. **Retry strategies** - Implement exponential backoff for transient failures
4. **Dead letter handling** - Route failed items for manual investigation

### Monitoring

1. **Pipeline metrics** - Track success/failure rates and processing times
2. **Data quality metrics** - Monitor record counts and validation results
3. **Resource utilization** - Track cluster usage and costs
4. **Alert configuration** - Set up alerts for critical failures

## Related Topics

- [Processing Strategies](Processing-Strategies.md) - Understanding how entities are processed
- [Metadata Management](Metadata-Management.md) - Configuring entities for ADF integration
- [Configuration Reference](Configuration-Reference.md) - Advanced configuration options