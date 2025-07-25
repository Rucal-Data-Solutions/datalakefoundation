# Datalake Foundation Wiki

Welcome to the Datalake Foundation documentation wiki. This wiki provides comprehensive documentation for the Datalake Foundation library, an open-source data ingestion library built around Data Lakehouse principles.

## What is Datalake Foundation?

Datalake Foundation acts as a *foundation layer* between the raw **bronze** storage area and the curated **silver** layer. The library reads *data slices* (Parquet files) produced by upstream ingestion pipelines, applies metadata-driven transformations and merges them into Delta Lake tables. Its primary objective is to standardise ingestion logic across projects by supplying pre-built processing strategies, metadata handlers and utility routines.

## Quick Navigation

### Getting Started
- [Installation Guide](Installation-Guide.md) - Prerequisites, installation steps, and deployment
- [Quick Start](Quick-Start.md) - Get up and running quickly

### Core Concepts
- [Processing Strategies](Processing-Strategies.md) - Full, Merge, and Historic processing modes
- [Metadata Management](Metadata-Management.md) - JSON and SQL Server metadata configuration
- [Folder Structure](Folder-Structure.md) - Bronze and Silver layer organization

### Configuration & Administration
- [Configuration Reference](Configuration-Reference.md) - Detailed configuration options
- [Transformations and System Columns](Transformations.md) - Data transformation capabilities
- [Migration Guide](Migration-Guide.md) - Scala 2.13 migration information

### Advanced Topics
- [Data Factory Integration](Data-Factory-Integration.md) - Azure Data Factory item generator
- [API Reference](API-Reference.md) - Detailed API documentation

## Key Features

- **Metadata-driven processing** - Configure entities and transformations through JSON or SQL Server
- **Multiple processing strategies** - Full loads, incremental merges, and SCD Type 2 historic tracking
- **Delta Lake integration** - Native support for Delta Lake MERGE operations
- **Databricks optimized** - Built for Databricks Runtime 16.4 LTS with Scala 2.13.12
- **Extensible architecture** - Pluggable transformations and metadata sources

## Requirements

- **Databricks Runtime**: 16.4 LTS or later
- **Scala**: 2.13.12 or 2.12.18
- **Spark**: 3.5.1
- **Delta Lake**: 3.3.1

## Support

For questions, issues, or contributions, please refer to the project repository or contact the maintainers.

---

*This documentation covers version 1.4.5 of Datalake Foundation.*