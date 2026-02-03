# Entity Configuration

This document describes how to configure entities in Datalake Foundation, including column definitions, field roles, and settings hierarchy.

## Entity Structure

An entity represents a data table or dataset that flows through the bronze-to-silver pipeline. Each entity is defined in the metadata JSON with the following properties:

```json
{
  "id": 1,
  "name": "customer",
  "group": "sales",
  "destination": "dim_customer",
  "enabled": true,
  "secure": false,
  "connection": "AdventureWorksSql",
  "connectiongroup": "erp_group",
  "processtype": "merge",
  "watermark": [],
  "columns": [],
  "settings": {},
  "transformations": []
}
```

### Entity Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `id` | Integer | Yes | Unique identifier for the entity |
| `name` | String | Yes | Source entity name (used in bronze path) |
| `group` | String | No | Logical grouping for batch processing |
| `destination` | String | No | Target name in silver layer (defaults to `name`) |
| `enabled` | Boolean | No | Whether processing is enabled (default: `true`) |
| `secure` | Boolean | No | Use secure container suffix paths (default: `false`) |
| `connection` | String | Yes | Reference to a connection definition |
| `connectiongroup` | String | No | Connection-level grouping |
| `processtype` | String | Yes | Processing strategy: `full`, `merge`, or `historic` |
| `watermark` | Array | No | Watermark column definitions for incremental processing |
| `columns` | Array | No | Column definitions with types and roles |
| `settings` | Object | No | Entity-specific settings (override connection/environment) |
| `transformations` | Array | No | Custom transformation expressions |

## Column Definitions

Columns define the schema and behavior of entity fields. Each column can have multiple field roles that determine how it's used during processing.

```json
{
  "columns": [
    {
      "name": "customer_id",
      "newname": "CustomerID",
      "datatype": "integer",
      "fieldroles": ["businesskey"],
      "expression": ""
    },
    {
      "name": "",
      "newname": "Region",
      "datatype": "string",
      "fieldroles": ["calculated", "partition"],
      "expression": "'EMEA'"
    }
  ]
}
```

### Column Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Source column name (empty for calculated columns) |
| `newname` | String | Target column name (rename during processing) |
| `datatype` | String | Target data type for casting |
| `fieldroles` | Array | List of roles this column fulfills |
| `expression` | String | Spark SQL expression for calculated columns |

### Supported Data Types

- `string` - StringType
- `integer` - IntegerType
- `long` - LongType
- `float` - FloatType
- `double` - DoubleType
- `boolean` - BooleanType
- `date` - DateType
- `timestamp` - TimestampType
- `decimal(precision,scale)` - DecimalType (e.g., `decimal(18,2)`)

## Field Roles

Field roles define how columns are used during processing. A column can have multiple roles.

### `businesskey`

Columns marked as `businesskey` form the natural key of the entity. They are used to:
- Generate the primary key hash (`PK_<destination>`)
- Match records during merge operations
- Detect duplicates (throws `DuplicateBusinesskeyException`)

```json
{
  "name": "customer_id",
  "fieldroles": ["businesskey"]
}
```

Multiple columns can be business keys - they are combined to form a composite key:

```json
{
  "columns": [
    {"name": "region", "fieldroles": ["businesskey"]},
    {"name": "customer_id", "fieldroles": ["businesskey"]}
  ]
}
```

### `partition`

Columns marked as `partition` are used as Delta Lake partition columns. This affects:
- How data is physically organized on storage
- Partition pruning during merge operations
- Dynamic partition overwrite behavior

```json
{
  "name": "year",
  "fieldroles": ["partition"]
}
```

### `calculated`

Calculated columns are derived from expressions rather than source data. When a column has `calculated` role:
- The `name` field can be empty
- The `expression` field contains a Spark SQL expression
- The column is added to the DataFrame during transformation

```json
{
  "name": "",
  "newname": "ProcessedDate",
  "datatype": "date",
  "fieldroles": ["calculated"],
  "expression": "current_date()"
}
```

### Combining Roles

A column can have multiple roles. For example, a calculated column that is also part of the business key:

```json
{
  "name": "",
  "newname": "Administration",
  "datatype": "integer",
  "fieldroles": ["calculated", "businesskey", "partition"],
  "expression": "950"
}
```

## Settings Hierarchy

Settings can be defined at three levels, with more specific levels overriding general ones:

```
Environment Settings (most general)
    └── Connection Settings
        └── Entity Settings (most specific)
```

### Environment Settings

Defined in the `environment` block:

```json
{
  "environment": {
    "name": "production",
    "root_folder": "/mnt/datalake",
    "timezone": "Europe/Amsterdam",
    "raw_path": "/${connection}/${entity}",
    "bronze_path": "/${connection}/${entity}",
    "silver_path": "/${connection}/${destination}",
    "systemfield_prefix": "dlf_",
    "output_method": "paths"
  }
}
```

### Connection Settings

Defined in each connection's `settings` block:

```json
{
  "connections": [
    {
      "name": "AdventureWorksSql",
      "enabled": true,
      "settings": {
        "server": "sql-server.example.com",
        "database": "AdventureWorks2022"
      }
    }
  ]
}
```

### Entity Settings

Defined in each entity's `settings` block (overrides connection settings):

```json
{
  "entities": [
    {
      "id": 1,
      "name": "customer",
      "settings": {
        "silver_table": "silver_sales.dim_customer",
        "delete_missing": true
      }
    }
  ]
}
```

### Common Settings

| Setting | Level | Description |
|---------|-------|-------------|
| `raw_path` | Environment/Entity | Path pattern for raw files |
| `bronze_path` | Environment/Entity | Path pattern for bronze layer |
| `silver_path` | Environment/Entity | Path pattern for silver layer |
| `bronze_table` | Entity | Unity Catalog table name for bronze |
| `silver_table` | Entity | Unity Catalog table name for silver |
| `output_method` | Environment/Entity | Output type: `paths` or `catalog` |
| `delete_missing` | Entity | Enable delete inference (see [Delete Inference](../processing/DELETE_INFERENCE.md)) |

### Expression Variables

Path and table settings support expression variables:

| Variable | Description |
|----------|-------------|
| `${connection}` | Connection name |
| `${entity}` | Entity source name |
| `${destination}` | Entity destination name |
| `${today}` | Current date (yyyyMMdd format) |
| `${settings_<key>}` | Value of any setting by key |

Example:
```json
{
  "silver_table": "silver_${connection}.${destination}",
  "bronze_path": "/${connection}/${entity}/${today}"
}
```

## Entity Transformations

Transformations allow injecting custom Spark SQL expressions during processing:

```json
{
  "transformations": [
    {
      "column": "full_name",
      "expression": "concat(first_name, ' ', last_name)"
    },
    {
      "column": "is_active",
      "expression": "status = 'A'"
    }
  ]
}
```

Transformations are applied early in the processing pipeline, before system columns are added.

## Example: Complete Entity Configuration

```json
{
  "id": 42,
  "name": "sales_order",
  "group": "sales",
  "destination": "fact_sales",
  "enabled": true,
  "connection": "AdventureWorksSql",
  "processtype": "merge",
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "expression": "'${last_value}'"
    }
  ],
  "columns": [
    {
      "name": "SalesOrderID",
      "fieldroles": ["businesskey"]
    },
    {
      "name": "OrderDate",
      "datatype": "date",
      "fieldroles": ["partition"]
    },
    {
      "name": "TotalDue",
      "datatype": "decimal(18,2)",
      "fieldroles": []
    },
    {
      "name": "",
      "newname": "LoadDate",
      "datatype": "timestamp",
      "fieldroles": ["calculated"],
      "expression": "current_timestamp()"
    }
  ],
  "settings": {
    "silver_table": "silver_sales.fact_sales",
    "delete_missing": true
  },
  "transformations": []
}
```

## See Also

- [Processing Strategies](../processing/PROCESSING_STRATEGIES.md)
- [Watermarks](../processing/WATERMARKS.md)
- [IO Output Modes](../outputs/IO_OUTPUT_MODES.md)
