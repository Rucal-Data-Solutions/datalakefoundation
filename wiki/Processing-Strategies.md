# Processing Strategies

At the heart of Datalake Foundation is the `Processing` class (in `datalake.processing`). You instantiate it with an `Entity` (from metadata) and a **slice** file name, then call `Process()`.

The processing strategy is automatically chosen based on the entity metadata configuration. The examples below show explicit strategy selection for illustration; normally you would call `processing.Process()` without parameters.

## Available Strategies

Datalake Foundation supports three main processing strategies:

1. **Full** - Complete data replacement
2. **Merge** - Incremental delta processing with upserts
3. **Historic** - SCD Type 2 with full history tracking

## 1. Full Load Processing

**Full** processing is used for initial loads or complete data refreshes. It performs an **overwrite** operation into the Silver table using dynamic partition mode.

### Usage Example
```scala
val entity = metadata.getEntity(42)
val processing = new Processing(entity, "2025-07-01-slice.parquet")
processing.Process(Full)
```

### Characteristics
- Completely replaces existing data in the target table
- Uses dynamic partitioning for efficient overwrites
- Ideal for reference data or complete refreshes
- No dependency on existing table structure

## 2. Merge (Delta) Processing

**Merge** processing supports incremental data updates and executes a Delta Lake **MERGE** operation into the existing Silver table.

### Usage Example
```scala
val entity = metadata.getEntity(42)
val processing = new Processing(entity, "2025-07-01-slice.parquet")
processing.Process(Merge)
```

### Behavior Details
- **Fallback**: Automatically falls back to Full load when the Silver table doesn't exist
- **Partition filtering**: Optionally filters on partition values present in the slice to limit merge scope
- **Schema monitoring**: Logs warnings when schema drifts are detected
- **Upsert logic**: Based on computed primary key and source hash

### Merge Logic
The merge operation follows this logic based on primary key matching and source hash comparison:

| Condition | Action |
|-----------|--------|
| Source row marked as deleted | Update target row to deleted status |
| Different source hash | Update all columns with new values |
| Identical hash | Update only `lastSeen` timestamp |
| No primary key match | Insert new row |

## 3. Historic (SCD Type 2) Processing

**Historic** processing implements Slowly Changing Dimension (SCD) Type 2 methodology. It maintains complete history by keeping previous versions and tracking validity periods.

### Usage Example
```scala
val entity = metadata.getEntity(42)
val processing = new Processing(entity, "2025-07-01-slice.parquet")
processing.Process(Historic)
```

### Key Features
- **History preservation**: Keeps all previous versions of records
- **Validity tracking**: Uses `ValidFrom`, `ValidTo`, `IsCurrent` columns
- **Change detection**: Based on `SourceHash` comparison
- **Version management**: Properly closes old versions and creates new ones

### Processing Logic
- **First run**: Defaults to Full load behavior
- **Subsequent runs**: 
  - Matches on primary key where `IsCurrent = true`
  - For changed records: closes current version (`ValidTo` set, `IsCurrent = false`) and inserts new version
  - New versions are appended to the Silver table

### Historic Columns
The following system columns are automatically managed:

| Column | Purpose |
|--------|---------|
| `ValidFrom` | Start date/time for this record version |
| `ValidTo` | End date/time for this record version (NULL for current) |
| `IsCurrent` | Boolean flag indicating if this is the current version |
| `SourceHash` | Hash of all source columns for change detection |

## Processing Time Override

All processing strategies support an optional `processing.time` parameter to override the default processing timestamp.

### Usage Example
```scala
val options = Map("processing.time" -> "2025-05-05T12:00:00")
val processing = new Processing(entity, "2025-04-30-slice.parquet", options)
processing.Process(Historic)
```

### Important Notes
- **Format**: Accepts ISO-8601 formatted timestamps
- **Default**: Current system time when not specified
- **Validation**: Invalid timestamps are logged and default to current time
- **Warning**: The library does not validate temporal succession; providing an earlier `processing.time` than previously processed data can result in inconsistent `ValidFrom/ValidTo` ranges

## Strategy Selection

The processing strategy is typically defined in the entity metadata configuration. However, you can override it programmatically when needed:

```scala
// Use metadata-defined strategy
processing.Process()

// Override with specific strategy
processing.Process(Full)
processing.Process(Merge)
processing.Process(Historic)
```

## Best Practices

### When to Use Each Strategy

**Full Load**
- Reference/lookup tables
- Small datasets that change completely
- Initial data migrations
- Data quality corrections requiring complete refresh

**Merge Processing**
- Transactional data with updates and deletes
- Large datasets with incremental changes
- Real-time or near-real-time processing
- When you need current state only

**Historic Processing**
- Audit requirements demanding full history
- Slowly changing dimensions
- Regulatory compliance scenarios
- Analytics requiring point-in-time analysis

### Performance Considerations
- **Partitioning**: Ensure proper partitioning strategy for your data volume
- **Merge scope**: Use partition filtering in Merge processing for large tables
- **Historic growth**: Monitor table growth with Historic processing
- **Checkpointing**: Regular checkpointing for long-running processes

## Related Topics
- [Metadata Management](Metadata-Management.md) - Configuring processing strategies in metadata
- [Transformations](Transformations.md) - Understanding data transformations applied during processing
- [Configuration Reference](Configuration-Reference.md) - Detailed configuration options