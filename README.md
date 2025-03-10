# Datalake Foundation

[![Scala Version](https://img.shields.io/badge/scala-2.12.19-red.svg)](https://www.scala-lang.org/)
[![Spark Version](https://img.shields.io/badge/spark-3.5.1-orange.svg)](https://spark.apache.org/)
[![Delta Lake Version](https://img.shields.io/badge/delta--lake-3.2.0-blue.svg)](https://delta.io/)

Datalake Foundation is a powerful Scala library designed to process data according to the DataLakehouse principles. It streamlines the transformation of data from bronze to silver layers in a data lakehouse architecture, making it ready for business logic transformations.

## Features

- **Processing Strategies**
  - Full Load - Complete replacement of target data
  - Delta/Merge Processing - Incremental updates with change tracking

- **Metadata Management**
  - JSON Metadata Configuration
  - SQL Server Database Configuration

- **Data Factory Integration**
  - Item generator for batch processing

- **Data Quality**
  - Schema validation and evolution
  - Business key validation
  - Automatic data type casting

- **Tracking**
  - Watermark tracking for incremental loads
  - Change detection with source hash calculation

## Installation

Add the following dependency to your `build.sbt` file:

```scala
libraryDependencies += "nl.rucal" %% "datalakefoundation" % "1.0.0-SNAPSHOT"
```

### Prerequisites

- Scala 2.12.x
- Apache Spark 3.5.x
- Delta Lake 3.2.x

## Configuration

Datalake Foundation requires metadata configuration to define entities, connections, and processing rules. You can configure it using either JSON files or a SQL Server database.

### JSON Configuration

Create a JSON configuration file with the following structure:

```json
{
  "environment": {
    "root_folder": "/path/to/datalake",
    "raw_path": "/raw",
    "bronze_path": "/bronze",
    "silver_path": "/silver",
    "timezone": "Europe/Amsterdam"
  },
  "connections": [
    {
      "name": "source_system",
      "code": "src",
      "enabled": true,
      "settings": {}
    }
  ],
  "entities": [
    {
      "id": 1,
      "name": "customers",
      "group": "sales",
      "destination": "customers",
      "enabled": true,
      "secure": false,
      "connection": "src",
      "processtype": "merge",
      "watermark": [
        {
          "column_name": "last_update_date",
          "data_type": "timestamp"
        }
      ],
      "columns": [
        {
          "name": "customer_id",
          "data_type": "string",
          "field_roles": ["businesskey"]
        },
        {
          "name": "customer_name",
          "data_type": "string"
        },
        {
          "name": "last_update_date",
          "data_type": "timestamp"
        }
      ],
      "settings": {},
      "transformations": []
    }
  ]
}
```

### SQL Server Configuration

For SQL Server configuration, you need to provide connection details:

```scala
import datalake.metadata._

val sqlSettings = SqlServerSettings(
  server = "your-server.database.windows.net",
  port = 1433,
  database = "metadata_db",
  username = "username",
  password = "password"
)

val metadataSettings = SqlMetadataSettings(sqlSettings)
val metadata = new Metadata(metadataSettings)
```

## Usage

### Basic Usage

```scala
import datalake.metadata._
import datalake.processing._

// Initialize metadata from JSON file
val jsonSettings = new JsonMetadataSettings
jsonSettings.initialize("/path/to/config.json")
val metadata = new Metadata(jsonSettings)

// Get entity by ID
val entity = metadata.getEntity(1)

// Process a data slice
val sliceFile = "customers_20230101.parquet"
val processing = new Processing(entity, sliceFile)
processing.Process()
```

### Processing Strategies

#### Full Load

```scala
// Process with full load strategy (overwrites target)
processing.Process(Full)
```

#### Delta/Merge Processing

```scala
// Process with merge strategy (updates existing records, inserts new ones)
processing.Process(Merge)
```

### Data Factory Integration

For Azure Data Factory or other orchestration tools:

```scala
import datalake.outputs.DataFactory
import datalake.metadata._

// Initialize metadata
val jsonSettings = new JsonMetadataSettings
jsonSettings.initialize("/path/to/config.json")
val metadata = new Metadata(jsonSettings)

// Generate configuration items for a connection
implicit val md = metadata
val connectionItems = DataFactory.getConfigItems(metadata.getConnection("src"))

// Generate configuration items for a group
val groupItems = DataFactory.getConfigItems(EntityGroup("sales"))

// Generate configuration items for a specific entity
val entityItems = DataFactory.getConfigItems(1)
```

## Data Paths

Datalake Foundation follows a structured path convention:

- **Bronze (Source) Location**: `<root_folder>/bronze/<connection>/<entity_name>/<slice_file>`
- **Silver (Target) Location**: `<root_folder>/silver/<connection>/<entity_name>/<slice_file>`

## Advanced Features

### Column Transformations

You can define SQL expressions for column transformations:

```json
{
  "columns": [
    {
      "name": "full_name",
      "data_type": "string",
      "expression": "concat(first_name, ' ', last_name)"
    }
  ]
}
```

### Partitioning

Define partition columns for better performance:

```json
{
  "columns": [
    {
      "name": "year",
      "data_type": "integer",
      "field_roles": ["partition"]
    }
  ]
}
```

### Watermark Tracking

Watermarks help with incremental processing:

```json
{
  "watermark": [
    {
      "column_name": "last_update_date",
      "data_type": "timestamp"
    }
  ]
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed as GNU General Public License v3.0

---

*For more detailed information, please contact the maintainers.*
