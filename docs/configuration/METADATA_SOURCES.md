# Metadata Sources

Datalake Foundation supports multiple sources for loading entity configuration metadata. This document describes each source type and how to configure them.

## Overview

All metadata sources extend `DatalakeMetadataSettings` and provide the same configuration structure. The difference is where the JSON configuration is loaded from:

| Source | Class | Use Case |
|--------|-------|----------|
| JSON File | `JsonMetadataSettings` | Single configuration file |
| JSON Folder | `JsonFolderMetadataSettings` | Split configuration across files |
| SQL Server | `SqlMetadataSettings` | Central database-driven configuration |
| String | `StringMetadataSettings` | Programmatic/embedded configuration |

## JSON File Source

The most common approach - load configuration from a single JSON file.

### Usage

```scala
import datalake.metadata._

val settings = new JsonMetadataSettings()
settings.initialize("/path/to/metadata.json")

val metadata = new Metadata(settings)
```

### File Structure

The JSON file must contain three top-level sections:

```json
{
  "environment": {
    "name": "production",
    "root_folder": "/mnt/datalake",
    "timezone": "Europe/Amsterdam",
    "raw_path": "/${connection}/${entity}",
    "bronze_path": "/${connection}/${entity}",
    "silver_path": "/${connection}/${destination}"
  },
  "connections": [
    {
      "name": "SalesDB",
      "enabled": true,
      "settings": {}
    }
  ],
  "entities": [
    {
      "id": 1,
      "name": "customer",
      "connection": "SalesDB",
      "processtype": "merge",
      "columns": [],
      "watermark": [],
      "settings": {},
      "transformations": []
    }
  ]
}
```

## JSON Folder Source

Split configuration across multiple JSON files in a directory. Files are merged together, allowing modular organization.

### Usage

```scala
import datalake.metadata._

val settings = new JsonFolderMetadataSettings()
settings.initialize("/path/to/config/directory")

val metadata = new Metadata(settings)
```

### Directory Structure

```
config/
├── environment.json
├── connections.json
├── entities_sales.json
├── entities_finance.json
└── entities_hr.json
```

Each file contributes to the merged configuration:

**environment.json**
```json
{
  "environment": {
    "name": "production",
    "root_folder": "/mnt/datalake",
    "timezone": "UTC",
    "raw_path": "/${connection}/${entity}",
    "bronze_path": "/${connection}/${entity}",
    "silver_path": "/${connection}/${destination}"
  }
}
```

**connections.json**
```json
{
  "connections": [
    {"name": "SalesDB", "enabled": true, "settings": {}},
    {"name": "FinanceDB", "enabled": true, "settings": {}}
  ]
}
```

**entities_sales.json**
```json
{
  "entities": [
    {"id": 1, "name": "customer", "connection": "SalesDB", "processtype": "merge", "columns": [], "watermark": [], "settings": {}, "transformations": []},
    {"id": 2, "name": "orders", "connection": "SalesDB", "processtype": "merge", "columns": [], "watermark": [], "settings": {}, "transformations": []}
  ]
}
```

Files are merged in filesystem order. Arrays with the same key are combined.

## SQL Server Source

Load configuration from a SQL Server function. Useful for centralized configuration management.

### Usage

```scala
import datalake.metadata._

val sqlSettings = SqlServerSettings(
  server = "sql-server.database.windows.net",
  port = 1433,
  database = "ConfigDB",
  username = "config_reader",
  password = "your_password"
)

val settings = SqlMetadataSettings(sqlSettings)
val metadata = new Metadata(settings)
```

### SQL Server Setup

The library expects a function `cfg.fnGetFoundationConfig()` that returns a single row with a `config` column containing the full JSON configuration:

```sql
CREATE FUNCTION cfg.fnGetFoundationConfig()
RETURNS TABLE
AS
RETURN
(
    SELECT CAST(
        (SELECT
            (SELECT name, root_folder, timezone, raw_path, bronze_path, silver_path
             FROM cfg.Environment FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS environment,
            (SELECT name, enabled, settings
             FROM cfg.Connections FOR JSON PATH) AS connections,
            (SELECT id, name, [group], destination, enabled, connection, processtype,
                    (SELECT column_name, operation, operation_group, expression
                     FROM cfg.Watermarks w WHERE w.entity_id = e.id FOR JSON PATH) AS watermark,
                    (SELECT name, newname, datatype, fieldroles, expression
                     FROM cfg.Columns c WHERE c.entity_id = e.id FOR JSON PATH) AS [columns],
                    (SELECT * FROM cfg.EntitySettings s WHERE s.entity_id = e.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS settings,
                    (SELECT column, expression
                     FROM cfg.Transformations t WHERE t.entity_id = e.id FOR JSON PATH) AS transformations
             FROM cfg.Entities e FOR JSON PATH) AS entities
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    AS NVARCHAR(MAX)) AS config
)
```

### Connection String

The library builds a connection string with these defaults:
- Encryption enabled
- Trust server certificate disabled
- Azure SQL compatible hostname certificate
- 30 second login timeout

## String Source

For programmatic configuration or testing. Pass the JSON configuration directly as a string.

### Usage

```scala
import datalake.metadata._

val configJson = """
{
  "environment": {
    "name": "test",
    "root_folder": "/data",
    "timezone": "UTC",
    "raw_path": "raw",
    "bronze_path": "bronze",
    "silver_path": "silver"
  },
  "connections": [],
  "entities": []
}
"""

val settings = new StringMetadataSettings()
settings.initialize(configJson)

val metadata = new Metadata(settings)
```

### Factory Method

A convenience factory method is available:

```scala
val settings = StringMetadataSettings.fromString(configJson)
```

## Common Operations

Once metadata is loaded, the same operations are available regardless of source:

### Get Entity by ID

```scala
val entity = metadata.getEntity(42)
// Returns Entity or throws EntityNotFoundException
```

### Get Multiple Entities by ID

```scala
val entities = settings.getEntitiesById(Array(1, 2, 3))
// Returns Option[List[Entity]]
```

### Get Entities by Connection

```scala
val connection = metadata.getConnection("SalesDB")
val entities = settings.getConnectionEntities(connection)
// Returns List[Entity]
```

### Get Entities by Group

```scala
val group = metadata.getEntityGroup("sales")
val entities = settings.getGroupEntities(group)
// Returns List[Entity]
```

### Get Entities by Connection Group

```scala
val connGroup = metadata.getEntityConnectionGroup("erp_systems")
val entities = settings.getConnectionGroupEntities(connGroup)
// Returns List[Entity]
```

### Get Environment Settings

```scala
val env = metadata.getEnvironment
println(env.RootFolder)
println(env.Timezone)
```

## Error Handling

The metadata system throws specific exceptions:

| Exception | Cause |
|-----------|-------|
| `MetadataNotInitializedException` | Accessing metadata before `initialize()` |
| `EntityNotFoundException` | Entity ID not found |
| `ConnectionNotFoundException` | Connection name not found |
| `DatalakeException` | Duplicate entity IDs in configuration |

## See Also

- [Entity Configuration](ENTITY_CONFIGURATION.md)
- [Processing Strategies](../processing/PROCESSING_STRATEGIES.md)
