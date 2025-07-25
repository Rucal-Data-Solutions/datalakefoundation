# Configuration Reference

This page provides comprehensive documentation for all configuration options available in Datalake Foundation.

## Configuration Hierarchy

Configuration settings follow a cascading hierarchy where more specific settings override general ones:

1. **Environment Settings** - Global defaults applied to all entities
2. **Connection Settings** - Override environment settings for specific connections
3. **Entity Settings** - Most specific, override both environment and connection settings

## Environment Configuration

### Basic Settings

```json
{
  "environment": {
    "timezone": "UTC",
    "output_method": "paths",
    "system_prefix": "sys_",
    "date_format": "yyyy-MM-dd",
    "timestamp_format": "yyyy-MM-dd HH:mm:ss"
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `timezone` | String | "UTC" | Default timezone for processing |
| `output_method` | String | "paths" | Output mode: "paths" or "catalog" |
| `system_prefix` | String | "sys_" | Prefix for system-generated columns |
| `date_format` | String | "yyyy-MM-dd" | Default date format pattern |
| `timestamp_format` | String | "yyyy-MM-dd HH:mm:ss" | Default timestamp format |

### System Column Configuration

```json
{
  "environment": {
    "hash_column_name": "source_hash",
    "valid_from_column": "valid_from",
    "valid_to_column": "valid_to", 
    "is_current_column": "is_current",
    "last_seen_column": "last_seen",
    "is_deleted_column": "is_deleted"
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `hash_column_name` | String | "source_hash" | Column name for source hash |
| `valid_from_column` | String | "valid_from" | Column name for validity start |
| `valid_to_column` | String | "valid_to" | Column name for validity end |
| `is_current_column` | String | "is_current" | Column name for current flag |
| `last_seen_column` | String | "last_seen" | Column name for last seen timestamp |
| `is_deleted_column` | String | "is_deleted" | Column name for deletion flag |

### Data Quality Settings

```json
{
  "environment": {
    "null_string_representations": ["NULL", "null", "", "N/A"],
    "duplicate_handling": "error",
    "validation_mode": "strict",
    "schema_evolution": "error"
  }
}
```

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `null_string_representations` | Array | ["NULL", "null", ""] | String values treated as null |
| `duplicate_handling` | String | "error" | How to handle duplicates: "error", "warn", "ignore" |
| `validation_mode` | String | "strict" | Validation strictness: "strict", "lenient" |
| `schema_evolution` | String | "error" | Schema change handling: "error", "warn", "auto" |

## Connection Configuration

### Basic Connection Structure

```json
{
  "connections": [
    {
      "id": 1,
      "name": "production_db",
      "type": "azure_storage",
      "description": "Production database connection",
      "settings": {
        "bronze_path": "/mnt/bronze/production",
        "silver_path": "/mnt/silver/production"
      }
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | Integer | Yes | Unique connection identifier |
| `name` | String | Yes | Connection name (must be unique) |
| `type` | String | No | Connection type for documentation |
| `description` | String | No | Human-readable description |
| `settings` | Object | No | Connection-specific settings |

### Connection Types

#### Azure Storage Connection

```json
{
  "type": "azure_storage",
  "settings": {
    "storage_account": "mystorageaccount",
    "container": "datalake",
    "bronze_path": "/bronze",
    "silver_path": "/silver",
    "secure_suffix": "_secure"
  }
}
```

#### Database Connection

```json
{
  "type": "database",
  "settings": {
    "jdbc_url": "jdbc:sqlserver://server:1433;database=mydb",
    "bronze_table_prefix": "bronze_",
    "silver_table_prefix": "silver_"
  }
}
```

#### File System Connection

```json
{
  "type": "filesystem", 
  "settings": {
    "root_path": "/data/lake",
    "bronze_subfolder": "bronze",
    "silver_subfolder": "silver"
  }
}
```

## Entity Configuration

### Core Entity Properties

```json
{
  "entities": [
    {
      "id": 42,
      "name": "customer",
      "description": "Customer dimension data",
      "source_connection": 1,
      "destination": "dim_customer",
      "processing_type": "Merge",
      "enabled": true
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | Integer | Yes | Unique entity identifier |
| `name` | String | Yes | Entity name |
| `description` | String | No | Human-readable description |
| `source_connection` | Integer | Yes | Reference to connection ID |
| `destination` | String | Yes | Target table/path name |
| `processing_type` | String | No | "Full", "Merge", or "Historic" |
| `enabled` | Boolean | No | Whether entity is enabled for processing |

### Column Definitions

```json
{
  "columns": [
    {
      "name": "customer_id",
      "type": "integer",
      "source_name": "cust_id",
      "role": "business_key",
      "required": true,
      "description": "Unique customer identifier"
    },
    {
      "name": "customer_name",
      "type": "string",
      "max_length": 255,
      "default_value": "Unknown"
    },
    {
      "name": "registration_date",
      "type": "date",
      "source_type": "string",
      "format": "MM/dd/yyyy"
    }
  ]
}
```

#### Column Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | String | Yes | Target column name |
| `type` | String | Yes | Data type (integer, string, date, timestamp, decimal, boolean) |
| `source_name` | String | No | Source column name (if different) |
| `source_type` | String | No | Source data type for casting |
| `role` | String | No | Column role: "business_key", "partition", "watermark" |
| `required` | Boolean | No | Whether column is required (not null) |
| `description` | String | No | Column description |

#### Data Type Configuration

**String Columns**:
```json
{
  "name": "description",
  "type": "string",
  "max_length": 1000,
  "min_length": 1,
  "trim": true,
  "case": "upper"
}
```

**Numeric Columns**:
```json
{
  "name": "amount",
  "type": "decimal",
  "precision": 15,
  "scale": 2,
  "min_value": 0,
  "max_value": 999999999.99
}
```

**Date/Time Columns**:
```json
{
  "name": "transaction_date",
  "type": "timestamp",
  "format": "yyyy-MM-dd HH:mm:ss",
  "timezone": "UTC"
}
```

### Watermark Configuration

```json
{
  "watermark": {
    "column": "modified_timestamp",
    "type": "timestamp",
    "format": "yyyy-MM-dd HH:mm:ss",
    "initial_value": "1900-01-01 00:00:00"
  }
}
```

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `column` | String | Yes | Watermark column name |
| `type` | String | Yes | Data type of watermark column |
| `format` | String | No | Date/timestamp format |
| `initial_value` | String | No | Initial watermark value |

### Partitioning Configuration

```json
{
  "partitioning": {
    "enabled": true,
    "columns": ["partition_date", "region"],
    "strategy": "hive_style",
    "max_partitions": 1000
  }
}
```

### Entity Settings

```json
{
  "settings": {
    "bronze_table": "${connection}_${entity}_bronze",
    "silver_table": "${connection}_${destination}",
    "secure_suffix": "_secure",
    "retention_days": 90,
    "compression": "snappy",
    "delta_properties": {
      "delta.autoOptimize.optimizeWrite": "true",
      "delta.autoOptimize.autoCompact": "true"
    }
  }
}
```

## Processing Strategy Configuration

### Full Load Strategy

```json
{
  "processing_type": "Full",
  "full_settings": {
    "overwrite_mode": "dynamic",
    "partition_overwrite": true,
    "validation_enabled": true
  }
}
```

### Merge Strategy

```json
{
  "processing_type": "Merge",
  "merge_settings": {
    "merge_key": "customer_id",
    "update_all_columns": true,
    "delete_handling": "logical",
    "partition_pruning": true,
    "schema_evolution": "merge"
  }
}
```

### Historic Strategy

```json
{
  "processing_type": "Historic",
  "historic_settings": {
    "scd_type": 2,
    "effective_date_column": "effective_date",
    "expiry_date_column": "expiry_date",
    "current_flag_column": "is_current",
    "version_column": "version_number"
  }
}
```

## Output Configuration

### Paths Output Mode

```json
{
  "output_method": "paths",
  "path_settings": {
    "bronze_template": "/bronze/${connection}/${entity}",
    "silver_template": "/silver/${connection}/${destination}",
    "partition_template": "/year=${year}/month=${month}/day=${day}"
  }
}
```

### Catalog Output Mode

```json
{
  "output_method": "catalog",
  "catalog_settings": {
    "database": "datalake",
    "bronze_prefix": "bronze_",
    "silver_prefix": "silver_",
    "table_properties": {
      "owner": "datalake_team",
      "environment": "production"
    }
  }
}
```

## Transformation Configuration

### Custom Transformations

```json
{
  "transformations": [
    {
      "name": "full_name",
      "type": "inject",
      "expression": "concat(first_name, ' ', last_name)",
      "data_type": "string",
      "description": "Concatenated full name"
    },
    {
      "name": "age_group",
      "type": "conditional",
      "conditions": [
        {
          "when": "age < 18",
          "then": "'Minor'",
          "data_type": "string"
        },
        {
          "when": "age >= 65",
          "then": "'Senior'",
          "data_type": "string"
        }
      ],
      "else": "'Adult'",
      "data_type": "string"
    }
  ]
}
```

### Validation Rules

```json
{
  "validation_rules": [
    {
      "name": "positive_amount",
      "type": "expression",
      "expression": "amount > 0",
      "error_message": "Amount must be positive",
      "severity": "error"
    },
    {
      "name": "valid_email",
      "type": "regex",
      "column": "email",
      "pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
      "error_message": "Invalid email format",
      "severity": "warning"
    }
  ]
}
```

## Advanced Configuration

### Expression Variables

Available variables for expression evaluation:

| Variable | Description | Example |
|----------|-------------|---------|
| `${today}` | Current date | 2025-01-01 |
| `${now}` | Current timestamp | 2025-01-01 12:30:45 |
| `${entity}` | Entity name | customer |
| `${destination}` | Destination name | dim_customer |
| `${connection}` | Connection name | production_db |
| `${connection_id}` | Connection ID | 1 |
| `${settings_*}` | Any setting value | ${settings_bronze_path} |

### Security Configuration

```json
{
  "security": {
    "encryption_at_rest": true,
    "encryption_in_transit": true,
    "access_control": {
      "read_roles": ["data_reader", "data_analyst"],
      "write_roles": ["data_engineer", "etl_service"],
      "admin_roles": ["data_admin"]
    },
    "data_classification": {
      "sensitivity_level": "internal",
      "contains_pii": false,
      "retention_policy": "7_years"
    }
  }
}
```

### Performance Tuning

```json
{
  "performance": {
    "spark_config": {
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true",
      "spark.sql.adaptive.skewJoin.enabled": "true"
    },
    "delta_config": {
      "delta.autoOptimize.optimizeWrite": "true",
      "delta.autoOptimize.autoCompact": "true",
      "delta.tuneFileSizesForRewrites": "true"
    },
    "processing_hints": {
      "target_file_size": "128MB",
      "max_records_per_file": 1000000,
      "partition_size_threshold": "10GB"
    }
  }
}
```

## Configuration Validation

### Validation Rules

The system validates configuration according to these rules:

1. **Required Fields**: All required fields must be present
2. **Unique Identifiers**: Entity and connection IDs must be unique
3. **Reference Integrity**: Entity source_connection must reference existing connection
4. **Data Type Compatibility**: Column types must be valid Spark SQL types
5. **Expression Syntax**: All expressions must be valid Spark SQL
6. **Circular Dependencies**: No circular references in entity dependencies

### Error Messages

Common validation errors and solutions:

**Duplicate Entity ID**:
```
Error: Entity ID 42 is used multiple times
Solution: Ensure each entity has a unique ID
```

**Invalid Connection Reference**:
```
Error: Entity 'customer' references non-existent connection 99
Solution: Update source_connection to reference existing connection ID
```

**Invalid Data Type**:
```
Error: Unknown data type 'varchar' in column 'name'
Solution: Use 'string' instead of 'varchar'
```

## Configuration Best Practices

### Organization

1. **Logical Grouping**: Group related entities under appropriate connections
2. **Consistent Naming**: Use standardized naming conventions
3. **Documentation**: Include descriptions for all entities and complex settings
4. **Version Control**: Keep configuration under version control

### Security

1. **Least Privilege**: Configure minimal required permissions
2. **Sensitive Data**: Use secure containers for PII and sensitive data
3. **Access Auditing**: Enable access logging for compliance
4. **Encryption**: Enable encryption for sensitive data

### Performance

1. **Partitioning Strategy**: Choose appropriate partition columns
2. **File Sizing**: Configure optimal file sizes for your workload
3. **Compression**: Use appropriate compression algorithms
4. **Resource Allocation**: Size clusters appropriately for data volume

### Maintenance

1. **Regular Review**: Periodically review and update configurations
2. **Testing**: Test configuration changes in non-production environments
3. **Monitoring**: Monitor processing performance and adjust settings
4. **Cleanup**: Remove unused entities and connections

## Related Topics

- [Metadata Management](Metadata-Management.md) - Managing configuration through metadata
- [Processing Strategies](Processing-Strategies.md) - Understanding processing behavior
- [Transformations](Transformations.md) - Configuring data transformations