# IO Output Modes

Datalake Foundation supports two output modes: file paths and Unity Catalog tables. This document explains how to configure each mode and how they affect processing behavior.

## Overview

| Mode | Setting | Output Type | Use Case |
|------|---------|-------------|----------|
| **Paths** | `output_method: "paths"` | File system paths | Traditional data lakes, ADLS/S3 |
| **Catalog** | `output_method: "catalog"` | Unity Catalog tables | Databricks Unity Catalog |

## Configuration Hierarchy

Output mode can be set at multiple levels:

```
Environment (default for all entities)
    └── Connection (override for connection)
        └── Entity (override for specific entity)
```

## Paths Mode (Default)

Paths mode writes data to file system locations as Delta tables.

### Environment Configuration

```json
{
  "environment": {
    "name": "production",
    "root_folder": "/mnt/datalake",
    "raw_path": "/${connection}/${entity}",
    "bronze_path": "/${connection}/${entity}",
    "silver_path": "/${connection}/${destination}",
    "output_method": "paths"
  }
}
```

### Resulting Paths

For an entity with:
- Connection: `AdventureWorksSql`
- Name: `customer`
- Destination: `dim_customer`

Generated paths:
```
Raw:    /mnt/datalake/raw/AdventureWorksSql/customer
Bronze: /mnt/datalake/bronze/AdventureWorksSql/customer
Silver: /mnt/datalake/silver/AdventureWorksSql/dim_customer
```

### Entity Override

Override paths at entity level:

```json
{
  "id": 1,
  "name": "customer",
  "settings": {
    "bronze_path": "/custom/bronze/path/${entity}",
    "silver_path": "/custom/silver/path/${destination}"
  }
}
```

## Catalog Mode

Catalog mode writes data to Unity Catalog managed tables.

### Environment Configuration

```json
{
  "environment": {
    "name": "production",
    "root_folder": "/mnt/datalake",
    "raw_path": "/${connection}/${entity}",
    "bronze_path": "/${connection}/${entity}",
    "silver_path": "/${connection}/${destination}",
    "output_method": "catalog"
  }
}
```

### Default Table Names

When using catalog mode, default table names follow this pattern:

```
Bronze: bronze_<connection>.<entity>
Silver: silver_<connection>.<destination>
```

For an entity with:
- Connection: `AdventureWorksSql`
- Name: `customer`
- Destination: `dim_customer`

Generated tables:
```
Bronze: bronze_AdventureWorksSql.customer
Silver: silver_AdventureWorksSql.dim_customer
```

### Entity Override

Specify custom table names:

```json
{
  "id": 1,
  "name": "customer",
  "settings": {
    "bronze_table": "raw_data.bronze_customers",
    "silver_table": "curated_data.dim_customer"
  }
}
```

## Mixed Mode

You can mix paths and catalog at the entity level. This is useful for transition scenarios or when different data zones use different storage patterns.

### Example: Paths Bronze, Catalog Silver

```json
{
  "environment": {
    "output_method": "paths"
  },
  "entities": [
    {
      "id": 1,
      "name": "customer",
      "settings": {
        "silver_table": "silver_sales.dim_customer"
      }
    }
  ]
}
```

Result:
- Bronze: `/mnt/datalake/bronze/AdventureWorksSql/customer` (path)
- Silver: `silver_sales.dim_customer` (catalog table)

### Example: Environment Catalog with Path Override

```json
{
  "environment": {
    "output_method": "catalog"
  },
  "entities": [
    {
      "id": 1,
      "name": "large_dataset",
      "settings": {
        "bronze_path": "/archive/bronze/${entity}",
        "silver_path": "/archive/silver/${destination}"
      }
    }
  ]
}
```

Result:
- Bronze: `/archive/bronze/large_dataset` (path)
- Silver: `/archive/silver/large_dataset` (path)

## Per-Layer Configuration

Fine-grained control is available with `bronze_output` and `silver_output`:

```json
{
  "environment": {
    "output_method": "paths",
    "bronze_output": "paths",
    "silver_output": "catalog"
  }
}
```

This configures:
- Bronze layer: File paths
- Silver layer: Unity Catalog tables

## Expression Variables

Table and path settings support expression variables:

| Variable | Description |
|----------|-------------|
| `${connection}` | Connection name |
| `${entity}` | Entity source name |
| `${destination}` | Entity destination name |
| `${today}` | Current date (yyyyMMdd) |
| `${settings_<key>}` | Any setting value |

Examples:

```json
{
  "silver_table": "silver_${connection}.${destination}",
  "bronze_path": "/${connection}/${entity}/${today}"
}
```

## Accessing Output in Code

### Get Output Configuration

```scala
val entity = metadata.getEntity(42)
val output = entity.getOutput

output match {
  case Output(rawPath, bronze, silver) =>
    bronze match {
      case PathLocation(path) => println(s"Bronze path: $path")
      case TableLocation(table) => println(s"Bronze table: $table")
    }
    silver match {
      case PathLocation(path) => println(s"Silver path: $path")
      case TableLocation(table) => println(s"Silver table: $table")
    }
}
```

### Get Paths (Legacy Compatibility)

```scala
val paths = entity.getPaths
println(paths.rawpath)
println(paths.bronzepath)
println(paths.silverpath)
```

Note: `getPaths` returns path strings even when catalog mode is configured. For catalog tables, it returns the underlying path if available.

## Processing Behavior

### Path Mode Processing

```scala
// Reads from path
val source = spark.read.parquet(s"$bronzePath/$sliceFile")

// Writes to path
source.write.delta(silverPath)
```

### Catalog Mode Processing

```scala
// Reads from table
val source = spark.read.table(bronzeTable)

// Writes to table (creates database if needed)
spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
source.write.format("delta").saveAsTable(silverTable)
```

### Database Creation

When writing to catalog tables, the library automatically creates the database if it doesn't exist:

```scala
val (databaseName, tableName) = "silver_sales.dim_customer".split("\\.") match {
  case Array(db, tbl) => (db, tbl)
  case Array(tbl) => ("default", tbl)
}

spark.sql(s"CREATE DATABASE IF NOT EXISTS `$databaseName`")
```

## Migration Considerations

When migrating from paths to catalog:

1. **Schema Registration**: Tables will be registered in the catalog
2. **Permissions**: Ensure proper Unity Catalog permissions
3. **Existing Data**: Data in existing paths won't be automatically migrated
4. **Dependencies**: Update downstream systems to reference catalog tables

### Migration Strategy

1. Start with new entities using catalog mode
2. Gradually migrate existing entities
3. Use mixed mode during transition
4. Update documentation and access patterns

## JSON Serialization

Entity JSON output reflects the configured output mode:

**Paths mode:**
```json
{
  "paths": {
    "rawpath": "/data/raw/...",
    "bronzepath": "/data/bronze/...",
    "silverpath": "/data/silver/..."
  }
}
```

**Catalog mode:**
```json
{
  "output": {
    "raw_path": "/data/raw/...",
    "bronze_table": "bronze_conn.entity",
    "silver_table": "silver_conn.entity"
  }
}
```

## See Also

- [Entity Configuration](../configuration/ENTITY_CONFIGURATION.md)
- [Metadata Sources](../configuration/METADATA_SOURCES.md)
- [Processing Strategies](../processing/PROCESSING_STRATEGIES.md)
