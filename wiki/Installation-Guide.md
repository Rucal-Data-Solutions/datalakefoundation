# Installation Guide

This guide covers the prerequisites and installation steps for Datalake Foundation.

## Prerequisites

Before deploying Datalake Foundation, ensure you have the following:

### Runtime Requirements
- **Databricks Runtime**: 16.4 LTS or later
- **Scala**: 2.13.12 or 2.12.18 
- **Spark**: 3.5.1
- **Delta Lake**: 3.3.1

### Infrastructure Requirements
- A Spark cluster (Databricks or vanilla) with Scala 2.12/2.13 support and Delta Lake connectors
- Access to Bronze and Silver storage layers using a unified path convention
- A metadata repository in either JSON files or a SQL Server function
- Standard build tooling such as **sbt** to compile Scala sources into a JAR

## Installation Steps

### Option 1: Build from Source

1. **Obtain the source code**
   ```bash
   git clone https://github.com/Rucal-Data-Solutions/datalakefoundation.git
   cd datalakefoundation
   ```

2. **Build the library**
   ```bash
   sbt package
   ```
   This compiles the Scala code and produces a JAR file.

3. **Deploy to your cluster**
   - Upload the JAR to your Spark/Databricks cluster, or
   - Include it as a library in your job configuration

### Option 2: Download Pre-built Libraries

Alternatively, you can download the pre-built libraries from the [GitHub releases page](https://github.com/Rucal-Data-Solutions/datalakefoundation/releases).

## Deployment Options

### Databricks Deployment
- Upload the JAR through the Databricks UI in the Libraries section
- Add as a cluster library or notebook-scoped library
- Ensure the cluster has the correct Scala version and Delta Lake connectors

### Custom Packaging Workflows
If you have custom packaging workflows, adapt the steps accordingly:
- **Databricks wheel packages**
- **AWS EMR bootstrap actions**
- **Container-based deployments**

The library is platform-agnostic as long as the runtime requirements are met.

## Verification

After deployment, verify that:

1. **Dependencies are available**: Ensure the cluster has the correct Spark & Delta versions
2. **Metadata is reachable**: Verify that JSON files or SQL metadata sources are accessible
3. **Storage access**: Confirm access to Bronze and Silver storage layers
4. **Import test**: Try importing the library in a test notebook or application:
   ```scala
   import datalake.processing.Processing
   import datalake.metadata.JsonMetadataSettings
   ```

## Troubleshooting

### Common Issues

**Scala Version Mismatch**
- Ensure your cluster Scala version matches the compiled JAR version
- Rebuild with the correct Scala version if necessary

**Missing Dependencies**
- Verify Delta Lake connectors are installed on the cluster
- Check that all required Spark modules are available

**Storage Access Issues**
- Verify authentication and permissions for Bronze/Silver storage
- Test path accessibility from the cluster

**Metadata Connection Issues**
- For JSON metadata: Verify file paths and permissions
- For SQL Server metadata: Test JDBC connectivity and function availability

## Next Steps

Once installation is complete, continue with:
- [Quick Start Guide](Quick-Start.md) - Basic usage examples
- [Configuration Reference](Configuration-Reference.md) - Detailed configuration options
- [Processing Strategies](Processing-Strategies.md) - Understanding processing modes

---

*For additional installation help or platform-specific guidance, consult your organization's deployment standards or contact the maintainers.*