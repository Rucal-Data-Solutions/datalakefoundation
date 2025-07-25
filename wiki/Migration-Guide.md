# Migration Guide

This guide covers migration from earlier versions of Datalake Foundation and platform upgrades, particularly the Scala 2.13 migration for Databricks Runtime 16.4 LTS.

## Scala 2.13 Migration

Datalake Foundation has been upgraded to Scala 2.13 to align with Databricks Runtime 16.4 LTS. This section covers the migration process and compatibility considerations.

### Version Overview

| Component | Previous Version | Current Version |
|-----------|-----------------|-----------------|
| Scala | 2.12.18 | 2.13.12 |
| Spark | 3.4.x | 3.5.1 |
| Delta Lake | 3.1.x | 3.3.1 |
| Databricks Runtime | 15.x LTS | 16.4 LTS |

### Pre-Migration Checklist

Before upgrading to Scala 2.13:

1. **Review Dependencies**: Ensure all custom libraries and dependencies support Scala 2.13
2. **Test Environment**: Set up a test environment with Databricks Runtime 16.4 LTS
3. **Backup Configuration**: Save current metadata configurations and custom code
4. **Resource Planning**: Plan downtime for cluster upgrades and testing

### Migration Steps

#### 1. Update Build Configuration

Update your `build.sbt` to use Scala 2.13:

```scala
// Old configuration (Scala 2.12)
scalaVersion := "2.12.18"

// New configuration (Scala 2.13)  
scalaVersion := "2.13.12"

// Support cross-compilation if needed
crossScalaVersions := Seq("2.12.18", "2.13.12")
```

#### 2. Dependency Updates

Update dependency versions in `build.sbt`:

```scala
// Updated dependencies for Scala 2.13
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
  "org.apache.spark" %% "spark-hive" % "3.5.1" % Provided,
  "io.delta" %% "delta-spark" % "3.3.1" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)
```

#### 3. Rebuild Libraries

Rebuild all JARs for Scala 2.13:

```bash
# Clean previous builds
sbt clean

# Compile for Scala 2.13
sbt +compile

# Create new JAR packages
sbt +package
```

#### 4. Update Databricks Clusters

Upgrade your Databricks clusters:

1. **Create new cluster** with Databricks Runtime 16.4 LTS
2. **Install updated JARs** compiled for Scala 2.13
3. **Test basic functionality** before full migration
4. **Update job configurations** to use new cluster

#### 5. Validate Functionality

Run comprehensive tests to ensure compatibility:

```scala
// Test basic library imports
import datalake.processing.Processing
import datalake.metadata.JsonMetadataSettings

// Test processing functionality
val metadata = new JsonMetadataSettings("test-metadata.json")
val entity = metadata.getEntity(1)
val processing = new Processing(entity, "test-slice.parquet")
processing.Process()
```

### Code Changes Required

Most existing code should work without changes, but review these areas:

#### Collection API Changes

Scala 2.13 has some collection API changes that may require updates:

```scala
// Old (may need updating)
import scala.collection.JavaConverters._

// New (preferred in Scala 2.13)
import scala.jdk.CollectionConverters._
```

#### Deprecation Warnings

Address any deprecation warnings that appear during compilation:

```scala
// Example: Update deprecated string interpolation
// Old: s"String with $variable"
// New: Use modern string interpolation patterns if needed
```

### Breaking Changes

#### Library Compatibility

Some third-party libraries may not be compatible between Scala 2.12 and 2.13:

1. **Custom transformations**: Review and recompile custom transformation libraries
2. **External connectors**: Verify connector libraries support Scala 2.13
3. **Testing frameworks**: Update test libraries to Scala 2.13 compatible versions

#### Runtime Behavior

Monitor for subtle runtime behavior changes:

- **Performance characteristics**: Benchmark critical processing paths
- **Memory usage**: Monitor memory consumption patterns
- **Threading behavior**: Verify multi-threaded processing behavior

### Testing Strategy

#### Compatibility Testing

1. **Unit Tests**: Run all existing unit tests with new runtime
2. **Integration Tests**: Test end-to-end processing scenarios
3. **Performance Tests**: Compare processing performance
4. **Data Validation**: Verify output data integrity

#### Regression Testing

Create specific regression tests for:
- Metadata loading and parsing
- All processing strategies (Full, Merge, Historic)
- Custom transformations and calculations
- Error handling and edge cases

### Deployment Strategy

#### Phased Rollout

1. **Development Environment**: Upgrade development clusters first
2. **Testing Environment**: Comprehensive testing with production data samples
3. **Staging Environment**: Full end-to-end testing
4. **Production Environment**: Gradual rollout with monitoring

#### Rollback Plan

Prepare rollback procedures:
- Keep Scala 2.12 JARs available
- Maintain previous cluster configurations
- Document rollback steps and validation procedures

### Performance Considerations

#### Expected Improvements

Scala 2.13 and Spark 3.5.1 provide:
- **Better performance** for collection operations
- **Improved memory management**
- **Enhanced SQL optimization**

#### Monitoring Points

Monitor these metrics post-migration:
- Processing time for typical workloads
- Memory usage patterns
- CPU utilization
- Delta Lake operation performance

### Common Issues and Solutions

#### Import Resolution Issues

**Problem**: Import errors after migration
```scala
// Error: object JavaConverters is not a member of package scala.collection
import scala.collection.JavaConverters._
```

**Solution**: Update to new collection converters
```scala
import scala.jdk.CollectionConverters._
```

#### Compilation Errors

**Problem**: Code that compiled in Scala 2.12 fails in 2.13

**Solution**: 
1. Update deprecated API usage
2. Check for library compatibility
3. Review compiler warnings for guidance

#### Runtime Errors

**Problem**: Code runs but produces different results

**Solution**:
1. Enable detailed logging
2. Compare outputs with previous version
3. Check for behavior changes in dependencies

### Validation Checklist

After migration, verify:

- [ ] All metadata configurations load correctly
- [ ] Processing strategies work as expected
- [ ] Custom transformations function properly
- [ ] Performance meets expectations
- [ ] Error handling works correctly
- [ ] Data integrity is maintained
- [ ] Logging and monitoring function properly

## Version Compatibility Matrix

| Datalake Foundation | Scala | Spark | Delta Lake | Databricks Runtime |
|-------------------|--------|-------|------------|-------------------|
| 1.3.x | 2.12.18 | 3.4.x | 3.1.x | 15.x LTS |
| 1.4.x | 2.13.12 | 3.5.1 | 3.3.1 | 16.4 LTS |

## Legacy Support

### Continued Scala 2.12 Support

Limited support for Scala 2.12 is maintained for:
- Critical bug fixes
- Security updates
- Migration assistance

### End of Life Timeline

- **Scala 2.12 builds**: Deprecated as of version 1.4.0
- **Support cutoff**: Consider upgrading by end of 2025
- **Legacy branches**: Available for emergency fixes only

## Migration Support

### Resources

- **Documentation**: This migration guide and updated API documentation
- **Sample Code**: Updated examples in the test suite
- **Community**: GitHub issues for migration-specific questions

### Professional Services

For large-scale migrations, consider:
- Professional migration assistance
- Custom compatibility testing
- Performance optimization services

---

*For specific migration questions or issues, please create an issue in the GitHub repository with detailed information about your environment and specific problems.*