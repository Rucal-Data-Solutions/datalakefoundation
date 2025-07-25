# Folder Structure

Datalake Foundation follows a standardized folder structure for organizing data across Bronze and Silver layers. Understanding this structure is essential for proper configuration and data flow.

## Overview

The library uses a layered approach with clear separation between raw and processed data:

- **Bronze Layer**: Raw data slices in Parquet format
- **Silver Layer**: Processed and transformed data in Delta Lake format

## Standard Layout

### Bronze Layer Structure

Raw Parquet files are organized in the Bronze layer following this pattern:

```
<root_folder>/bronze/<connection>/<entity_name>/<slice_files>
```

#### Example Bronze Structure
```
/datalake/
  bronze/
    production_db/
      customers/
        2025-01-01-slice.parquet
        2025-01-02-slice.parquet
        2025-01-03-slice.parquet
      orders/
        2025-01-01-slice.parquet
        2025-01-02-slice.parquet
    staging_db/
      products/
        2025-01-01-slice.parquet
```

### Silver Layer Structure

Processed Delta tables are stored in the Silver layer:

```
<root_folder>/silver/<connection>/<destination_table>/
```

#### Example Silver Structure
```
/datalake/
  silver/
    production_db/
      dim_customer/
        _delta_log/
        part-00000-*.parquet
        part-00001-*.parquet
      fact_orders/
        _delta_log/
        part-00000-*.parquet
    staging_db/
      dim_product/
        _delta_log/
        part-00000-*.parquet
```

## Path Resolution

### Entity Path Methods

The `Entity` metadata exposes paths through the `getPaths` property:

```scala
val entity = metadata.getEntity(42)
val paths = entity.getPaths

// Access Bronze path
val bronzePath = paths.bronzepath

// Access Silver path  
val silverPath = paths.silverpath
```

### Processing Path Composition

The `Processing` class composes the full slice path by combining:
- `paths.bronzepath` (from entity metadata)
- Slice file name (provided to constructor)

```scala
val processing = new Processing(entity, "2025-01-01-slice.parquet")
// Internally resolves to: {bronzepath}/2025-01-01-slice.parquet
```

## Configuration Options

### Output Method Configuration

Control how paths are handled using the `output_method` setting:

#### Paths Mode (Default)
```json
{
  "environment": {
    "output_method": "paths"
  }
}
```
Returns filesystem paths for Bronze and Silver locations.

#### Catalog Mode
```json
{
  "environment": {
    "output_method": "catalog"
  }
}
```
Returns table names for catalog-based access instead of direct paths.

### Custom Path Templates

Use expression evaluation to customize path patterns:

```json
{
  "entities": [
    {
      "id": 42,
      "settings": {
        "bronze_path": "/data/raw/${connection}/${entity}",
        "silver_path": "/data/curated/${connection}/${destination}",
        "archive_path": "/data/archive/${today}/${entity}"
      }
    }
  ]
}
```

## Naming Conventions

### Slice File Naming

Bronze layer slice files should follow a consistent naming pattern:

**Recommended Pattern**: `YYYY-MM-DD-slice.parquet`

Examples:
- `2025-01-01-slice.parquet`
- `2025-01-02-slice.parquet`
- `2025-12-31-slice.parquet`

**Alternative Patterns**:
- `YYYY-MM-DD-HH-slice.parquet` (hourly slices)
- `batch-NNNN-slice.parquet` (sequence-based)
- `delta-timestamp-slice.parquet` (incremental loads)

### Entity and Connection Naming

Follow consistent naming conventions:

**Connections**:
- Use descriptive names: `production_db`, `staging_storage`, `external_api`
- Avoid special characters except underscores and hyphens
- Use lowercase for consistency

**Entities**:
- Use singular nouns: `customer`, `order`, `product`
- Match source system naming where possible
- Use underscores for multi-word names: `customer_address`

**Destinations**:
- Follow dimensional modeling conventions: `dim_customer`, `fact_sales`
- Use consistent prefixes: `dim_`, `fact_`, `bridge_`

## Partitioning Strategy

### Bronze Layer Partitioning

Bronze data can be partitioned by:
- **Date**: Most common for time-series data
- **Connection**: Logical separation by source system
- **Entity**: Further subdivision by data type

Example partitioned Bronze structure:
```
/datalake/bronze/
  year=2025/
    month=01/
      day=01/
        connection=prod/
          entity=customer/
            slice.parquet
```

### Silver Layer Partitioning

Silver layer partitioning depends on access patterns:

**Date Partitioning** (recommended for most cases):
```
/datalake/silver/dim_customer/
  partition_date=2025-01-01/
    part-00000-*.parquet
  partition_date=2025-01-02/
    part-00000-*.parquet
```

**Business Key Partitioning** (for large dimension tables):
```
/datalake/silver/dim_customer/
  customer_region=north/
    part-00000-*.parquet
  customer_region=south/
    part-00000-*.parquet
```

## Secure Containers

For sensitive data, Datalake Foundation supports secure container suffixes:

### Configuration
```json
{
  "entities": [
    {
      "id": 42,
      "settings": {
        "secure_suffix": "_secure"
      }
    }
  ]
}
```

### Resulting Structure
```
/datalake/
  bronze_secure/
    sensitive_connection/
      pii_entity/
        slice.parquet
  silver_secure/
    sensitive_connection/
      dim_customer_pii/
        _delta_log/
        part-00000-*.parquet
```

## Access Patterns

### Read Operations

**Bronze Layer Access**:
```scala
// Direct file access
spark.read.parquet(s"${paths.bronzepath}/2025-01-01-slice.parquet")

// Batch processing multiple slices
val slicePattern = s"${paths.bronzepath}/*.parquet"
spark.read.parquet(slicePattern)
```

**Silver Layer Access**:
```scala
// Delta table access
spark.read.format("delta").load(paths.silverpath)

// Catalog access (when using catalog mode)
spark.read.table(entity.destination)
```

### Write Operations

The Processing class handles write operations automatically based on the configured strategy and paths.

## Storage Considerations

### Performance Optimization

**File Sizing**:
- Target 128MB-1GB per Parquet file in Bronze
- Use appropriate Delta table sizing for Silver layer
- Consider compaction strategies for frequently updated tables

**Partitioning**:
- Avoid over-partitioning (too many small partitions)
- Choose partition columns based on query patterns
- Limit partition count to improve metadata operations

### Cost Optimization

**Storage Tiering**:
- Use appropriate storage tiers (hot/cool/archive) based on access patterns
- Implement lifecycle policies for older Bronze data
- Consider compression settings for long-term storage

**Data Retention**:
- Define retention policies for Bronze layer data
- Implement automated cleanup for processed slices
- Archive historical Silver data when appropriate

## Troubleshooting

### Common Path Issues

**Path Not Found Errors**:
- Verify Bronze/Silver folder structure exists
- Check access permissions for the processing account
- Validate path expressions in metadata configuration

**Permission Denied**:
- Ensure service principal has read/write access to all layers
- Verify Azure Storage or S3 IAM permissions
- Check network access and firewall rules

**Invalid Path Characters**:
- Avoid special characters in connection/entity names
- Use URL encoding for problematic characters
- Validate path components before processing

### Debugging Path Resolution

Enable detailed logging to troubleshoot path issues:

```scala
// Add logging to see resolved paths
val entity = metadata.getEntity(42)
val paths = entity.getPaths
println(s"Bronze path: ${paths.bronzepath}")
println(s"Silver path: ${paths.silverpath}")
```

## Best Practices

### Organization
- **Consistent naming**: Use standardized naming conventions across all layers
- **Logical grouping**: Group related entities under appropriate connections
- **Clear hierarchy**: Maintain clear separation between different data domains

### Maintenance
- **Regular cleanup**: Implement automated cleanup for temporary files
- **Monitoring**: Monitor storage usage and growth patterns
- **Documentation**: Document custom path conventions and naming standards

### Security
- **Access control**: Implement proper access controls at the folder level
- **Encryption**: Use encryption at rest and in transit
- **Auditing**: Enable access logging for sensitive data areas

## Related Topics
- [Configuration Reference](Configuration-Reference.md) - Detailed path configuration options
- [Metadata Management](Metadata-Management.md) - Configuring connections and entities
- [Processing Strategies](Processing-Strategies.md) - How strategies affect path usage