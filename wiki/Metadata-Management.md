# Metadata Management

Datalake Foundation uses metadata-driven configuration to define entities, connections, and processing behavior. This section covers the available metadata sources and configuration options.

## Metadata Sources

### JSON Metadata

Use `JsonMetadataSettings` to load metadata from JSON files. This is the most common approach for configuration management.

#### Basic Usage
```scala
import datalake.metadata.JsonMetadataSettings

val metadata = new JsonMetadataSettings("path/to/metadata.json")
val entity = metadata.getEntity(42)
```

#### JSON Structure
The JSON metadata file defines three main sections:

**Connections** - Storage accounts, database instances, or other connection endpoints
```json
{
  "connections": [
    {
      "id": 1,
      "name": "production_storage",
      "type": "azure_storage",
      "settings": {
        "account_name": "mystorageaccount",
        "container": "datalake"
      }
    }
  ]
}
```

**Entities** - Data entities with their source connections, destination tables, and column definitions
```json
{
  "entities": [
    {
      "id": 42,
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
        }
      ],
      "watermark": {
        "column": "modified_date",
        "type": "timestamp"
      }
    }
  ]
}
```

**Environment** - Global settings such as timezone and system field prefixes
```json
{
  "environment": {
    "timezone": "UTC",
    "system_prefix": "sys_",
    "output_method": "paths"
  }
}
```

### SQL Server Metadata

Use `SqlMetadataSettings` to read JSON configuration from SQL Server via JDBC. This approach is useful for centralized configuration management.

#### Basic Usage
```scala
import datalake.metadata.{SqlMetadataSettings, SqlServerSettings}

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

#### Requirements
- JDBC connectivity to SQL Server
- A function that returns JSON configuration (e.g., `cfg.fnGetFoundationConfig()`)
- The function should return the same JSON structure as the file-based approach

## Configuration Hierarchy

Settings follow a cascading hierarchy where more specific configurations override general ones:

1. **Environment settings** - Global defaults
2. **Connection settings** - Per-connection overrides
3. **Entity settings** - Most specific, overrides all others

### Example Configuration Cascade
```json
{
  "environment": {
    "output_method": "paths",
    "timezone": "UTC"
  },
  "connections": [
    {
      "id": 1,
      "settings": {
        "output_method": "catalog"  // Overrides environment
      }
    }
  ],
  "entities": [
    {
      "id": 42,
      "source_connection": 1,
      "settings": {
        "output_method": "paths"  // Overrides connection and environment
      }
    }
  ]
}
```

## Entity Configuration

### Core Entity Properties

| Property | Description | Required |
|----------|-------------|----------|
| `id` | Unique entity identifier | Yes |
| `name` | Entity name | Yes |
| `source_connection` | Source connection ID | Yes |
| `destination` | Target table/path name | Yes |
| `processing_type` | Full, Merge, or Historic | No (defaults to Full) |

### Column Definitions

Each entity must define its columns with types and optional roles:

```json
{
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "role": "business_key"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "partition_date",
      "type": "date",
      "role": "partition"
    }
  ]
}
```

#### Column Roles

| Role | Purpose |
|------|---------|
| `business_key` | Used for primary key generation and merge operations |
| `partition` | Defines partitioning columns |
| `watermark` | Incremental processing control (deprecated - use watermark object) |

### Watermark Configuration

For incremental processing, define watermark columns:

```json
{
  "watermark": {
    "column": "modified_timestamp",
    "type": "timestamp"
  }
}
```

### Custom Settings

Entities can include custom settings for specialized behavior:

```json
{
  "settings": {
    "bronze_table": "${connection}_${entity}_bronze",
    "silver_table": "${connection}_${destination}_silver",
    "secure_suffix": "_secure",
    "custom_transformation": "uppercase_names"
  }
}
```

## Helper Methods

The metadata classes provide several helper methods for dynamic pipeline generation:

### Entity Retrieval
```scala
// Get specific entity
val entity = metadata.getEntity(42)

// Get all entities for a connection
val entities = metadata.getConnectionEntities(connectionId)

// Get entities by group
val groupEntities = metadata.getGroupEntities("customer_data")
```

### Connection Management
```scala
// Get connection by ID
val connection = metadata.getConnection(1)

// Get connection by name
val connection = metadata.getConnectionByName("production_storage")
```

### Environment Settings
```scala
// Get environment configuration
val environment = metadata.getEnvironment()
```

## Expression Evaluation

Datalake Foundation supports dynamic string evaluation in configuration values using expression placeholders:

### Available Variables
- `${today}` - Current date
- `${entity}` - Entity name
- `${destination}` - Destination table name
- `${connection}` - Connection name
- `${settings_*}` - Any setting value (e.g., `${settings_bronze_table}`)

### Example Usage
```json
{
  "settings": {
    "bronze_path": "/bronze/${connection}/${entity}",
    "silver_path": "/silver/${connection}/${destination}",
    "archive_path": "/archive/${today}/${entity}"
  }
}
```

## Validation Rules

### Entity Validation
- **Unique IDs**: Entity identifiers must be unique across the metadata
- **Valid connections**: Source connections must exist
- **Required columns**: At least one column must be defined
- **Business keys**: At least one business key column is recommended for merge/historic processing

### Connection Validation
- **Unique names**: Connection names must be unique
- **Required settings**: Each connection type has specific required settings

### Environment Validation
- **Valid timezone**: Must be a recognized timezone identifier
- **Valid output method**: Must be either "paths" or "catalog"

## Error Handling

Metadata loading includes comprehensive error handling:

- **Duplicate entity IDs**: Rejected with detailed error message
- **Missing connections**: Referenced connections must exist
- **JSON parsing errors**: Detailed parsing error information
- **SQL connectivity issues**: JDBC connection problems are logged and raised

## Best Practices

### Organization
- **Separate files**: Consider splitting large configurations into multiple files
- **Version control**: Keep metadata under version control
- **Environment separation**: Use different metadata for dev/test/prod environments

### Maintenance
- **Regular validation**: Validate metadata against actual data sources
- **Documentation**: Document custom settings and their purposes
- **Backup**: Maintain backups of metadata configurations

### Performance
- **Connection pooling**: Use connection pooling for SQL Server metadata
- **Caching**: Consider caching metadata for frequently accessed entities
- **Minimal metadata**: Only include necessary configuration to reduce parsing overhead

## Related Topics
- [Processing Strategies](Processing-Strategies.md) - How processing types affect entity behavior
- [Configuration Reference](Configuration-Reference.md) - Complete configuration options
- [Folder Structure](Folder-Structure.md) - Understanding path conventions