# Datalake Foundation

## Introduction
Datalake Foundation is a library designed to process data and prepare it for transformation (your business logic). Built on DataLakehouse principles, it seamlessly integrates into DataLakehouse architectures. The library processes data slices (parquet files) from the bronze layer and applies transformations based on metadata configurations.

## Features

### Processing
- **Full Load**: Processes entire datasets.
- **Delta Processing**: Handles incremental updates efficiently.
- **Historic Processing**: Supports processing with custom timestamps for backfilling and historic data handling.

### Metadata
- **JSON Metadata**: Supports configurations via `JsonMetadataSettings`.
- **SQL Server Database**: Leverages `SqlMetadataSettings` for metadata management.

### Data Factory
- **Item Generator**: Automates repetitive tasks using loops.

### Historic Processing
- **Custom Timestamps**: Allows processing with user-defined timestamps using the `processing.time` option.
- **Backfilling Support**: Facilitates historic data processing and ensures temporal integrity when used correctly.

## Folder Structure

### Bronze Layer
Stores raw data slices.
```
<root_folder>/bronze/<connection>/<entity_name>/<slice_file>
```

### Silver Layer
Holds processed and transformed data.
```
<root_folder>/silver/<connection>/<entity_name>/<slice_file>
```

## Configuration

### `processing.time` Option
The `processing.time` option allows you to specify a custom timestamp for processing. This is particularly useful for historic processing or backfilling data. If not provided, the current system time is used.

#### Example Usage
```scala
val options = Map("processing.time" -> "2025-05-05T12:00:00")
val processing = new Processing(entity, sliceFile, options)
processing.Process()
```

- **Format**: The timestamp must be in ISO-8601 format (e.g., `YYYY-MM-DDTHH:MM:SS`).
- **Fallback**: If an invalid timestamp is provided, the system will log an error and fall back to the current time.
- **Disclaimer**: The date is not checked for temporal succession, meaning that if an earlier date is used, anomalies (especially in historic processing) may occur.

## Scala 2.13 Migration
Databricks Runtime 16.4 LTS introduces Scala 2.13 support. The project now uses
Scala 2.13.12 with Spark 3.5.1 and Delta Lake 3.3.1 to match this runtime.
Review custom code and dependencies for compatibility with Scala 2.13 when
upgrading your environment.

## Notes
This documentation is a work in progress. For additional details or questions,
please contact the maintainer.

