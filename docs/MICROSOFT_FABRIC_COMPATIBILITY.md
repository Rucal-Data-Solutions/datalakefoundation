# Microsoft Fabric Compatibility Report

*Investigation date: February 2026*

## Summary

Datalake Foundation can run on Microsoft Fabric. The recommended path is **Fabric Runtime 2.0**, which provides an exact version match for Spark 4.0.0, Scala 2.13.16, and Delta Lake 4.0.0. Runtime 2.0 is currently in Experimental Preview (since December 2025), with GA expected mid-to-late 2026.

## Fabric Runtime Compatibility Matrix

| Requirement | Runtime 1.3 (GA) | Runtime 2.0 (Experimental Preview) |
|---|---|---|
| Spark 4.0.0 | NO (3.5.5) | YES (4.0.0) |
| Scala 2.13.16 | NO (2.12.17) | YES (2.13.16) |
| Delta Lake 4.0.0 | NO (3.2) | YES (4.0.0) |
| Java 17 | NO (Java 11) | PARTIAL (Java 21, forward-compatible) |
| Custom JAR support | YES (Environment + inline) | LIMITED (inline only for now) |
| Delta MERGE | YES | NOT YET in early preview |
| SCD Type 2 | YES | Depends on MERGE support |
| Dynamic partition overwrite | YES | YES (core Spark feature) |

## Route A: Runtime 2.0 (Recommended)

### What works as-is
- All path-based operations (paths are metadata-driven, no hardcoded `dbfs:/`)
- `Full` processing strategy (dynamic partition overwrite)
- SparkSession creation (standard `getOrCreate()`)
- SQL metadata source (Azure SQL JDBC accessible from Fabric)
- JSON metadata sources
- Log4j logging (standard Log4j 2.x)
- commons-io (bundled)

### Minor code changes needed
1. **Unity Catalog config checks** — Databricks-specific Unity Catalog config checks (`spark.databricks.unityCatalog.enabled`, `spark.databricks.defaultCatalog`) should be replaced with Fabric-equivalent or made configurable. Currently falls back gracefully to path-based writes, so non-blocking.
2. **Java 21 testing** — Verify JAR compiled for Java 17 runs correctly on Java 21 (forward-compatible bytecode, but should be tested).
3. **Scala compiler on classpath** — Verify `scala-compiler` (used by `Expressions.scala` ToolBox) is available in Runtime 2.0.

### Runtime 2.0 limitations (as of Feb 2026)
- Delta MERGE not yet available (blocks `Merge` and `Historic` strategies)
- Environment-based library management not supported (use `%%configure` with `spark.jars` instead)
- No starter pools (2-5 min session startup)
- No V-Order, autocompaction, or optimize write
- No R language support
- No Data Science integrations or Copilot
- No connectors (Kusto, SQL Analytics, Cosmos DB, MySQL)

### JAR installation workaround for Runtime 2.0
Upload the JAR to a Lakehouse, then use inline configuration:
```scala
%%configure -f
{
    "conf": {
        "spark.jars": "abfss://<lakehouse>.dfs.fabric.microsoft.com/<path>/datalakefoundation.jar"
    }
}
```

## Route B: Runtime 1.3 (if production needed before Runtime 2.0 GA)

Requires significant backporting effort:

1. **Cross-compile to Scala 2.12** — Add `"2.12.17"` to `build.sbt`, fix any 2.13-only syntax.
2. **Downgrade Spark/Delta API usage from 4.0 to 3.x:**
   - `DeltaMergeBuilder.execute()` returns `Unit` in Delta 3.x (not `DataFrame`) — rewrite metrics extraction in `Merge.scala:124-127` and `Historic.scala:106`.
   - `VARIANT` data type does not exist — replace with `STRING` in `TableAppender.scala:146`.
   - `parse_json()` function does not exist — replace with `from_json()` or store as plain string.
3. **Verify `whenNotMatchedBySource`** — Available since Delta 2.4, should work on Runtime 1.3's Delta 3.2.

## Codebase Portability Assessment

The codebase is well-architected for cross-platform use. Only two Databricks-specific code references exist:

| Location | What | Impact |
|---|---|---|
| Unity Catalog config checks | Databricks Unity Catalog config checks | Falls back gracefully; non-blocking |
| Databricks logger suppression | Suppresses Databricks-specific loggers | Harmless no-op on Fabric |

Everything else (paths, Delta operations, metadata handling, processing strategies) uses standard Spark/Delta APIs and is metadata-driven.

## Fabric Lakehouse Storage vs Databricks

| Feature | Microsoft Fabric | Databricks |
|---------|-----------------|------------|
| Storage layer | OneLake (ADLS Gen2) | DBFS / Unity Catalog |
| Table format | Delta Lake | Delta Lake |
| Catalog | Fabric workspace hierarchy | Unity Catalog |
| Path protocol | `abfss://` | `dbfs://`, `abfss://`, `s3://` |
| Path pattern | `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>` | varies |

The library's `PathLocation` mode maps to `abfss://` OneLake paths. The `TableLocation` mode maps to Fabric Lakehouse SQL endpoint table references. Both are configurable via metadata (`root_folder` environment setting).

## Recommended Timeline

1. **Now**: Test the JAR on Runtime 2.0 preview using the `Full` strategy only. Upload JAR to OneLake and use `%%configure` to load it.
2. **When Runtime 2.0 gets MERGE support**: Test `Merge` and `Historic` strategies end-to-end.
3. **When Runtime 2.0 reaches GA** (expected mid-late 2026): Deploy for production workloads with Environment-based library management.

## References

- https://learn.microsoft.com/en-us/fabric/data-engineering/runtime
- https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0
- https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-1-3
- https://learn.microsoft.com/en-us/fabric/data-engineering/lifecycle
- https://learn.microsoft.com/en-us/fabric/data-engineering/library-management
- https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-library
- https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables
