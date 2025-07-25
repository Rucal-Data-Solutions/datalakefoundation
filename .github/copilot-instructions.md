# Datalake Foundation AI Agent Instructions

## Project Overview
This is a Scala library for building data lake processing pipelines using DataLakehouse principles. The core functionality processes data slices (Parquet files) through bronze and silver layers using metadata-driven configurations.

## Key Concepts

### IO Output System
- Two output modes: `paths` (filesystem) and `catalog` (table names)
- Configuration hierarchy: entity settings override environment settings
- See `Entity.scala:parseOutput()` for implementation
- Example settings:
```json
{
  "environment": {
    "output_method": "paths",  // or "catalog"
  },
  "entities": [{
    "settings": {
      "output_method": "catalog",  // Overrides environment
      "bronze_table": "${connection}_${entity}",
      "silver_table": "${connection}_${destination}"
    }
  }]
}
```

### Processing Strategies
- Three main types: `Full`, `Merge`, `Historic` 
- Implemented in `ProcessStrategy` trait with specific implementations
- `Historic` processing requires `processing.time` option for temporal context

### Entity Configuration
Key files:
- `Entity.scala`: Core entity logic and metadata parsing
- `EntityColumn.scala`: Column definitions and transformations
- `Watermark.scala`: Incremental processing controls

Patterns:
- Use expression evaluation for dynamic string values (paths, table names)
- Settings cascade: Connection settings -> Entity settings
- Business keys and partition columns defined via column roles

## Development Workflow

### Building
```bash
sbt clean compile
sbt test        # Run tests
sbt package     # Create JAR
```

### Testing
- Tests in `src/test/scala`
- Use `BenchmarkSpec` as reference for integration testing
- Metadata examples in `src/test/scala/example/metadata.json`

### Key Integration Points
1. Metadata Sources:
   - JSON via `JsonMetadataSettings`
   - SQL Server via `SqlMetadataSettings`

2. Processing Pipeline:
   - Bronze layer: Raw data slices in Parquet
   - Silver layer: Processed/transformed data
   - Optional secure containers with suffix

## Common Patterns

### Expression Evaluation
Used for dynamic string resolution:
- Available variables: `today`, `entity`, `destination`, `connection`
- Settings accessible via `settings_` prefix
- Example: `/${connection}/${entity}` resolves to actual paths

### Error Handling
- Prefer explicit exceptions with meaningful messages
- Use `DatalakeException` for library-specific errors
- Log important state changes via `LogManager`

## Migrations/Updates
- Currently migrating to Scala 2.13.12
- Align with Databricks Runtime 16.4 LTS 
- Check dependency compatibility when upgrading
