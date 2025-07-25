# Datalake Foundation – usage and functionality guide

## 1. Introduction and scope

Datalake Foundation is an open‑source data ingestion library developed around **Data Lakehouse** principles. It acts as a *foundation layer* between the raw **bronze** storage area and the curated **silver** layer. The library reads *data slices* (Parquet files) produced by upstream ingestion pipelines, applies metadata‑driven transformations and merges them into Delta Lake tables. Its primary objective is to standardise ingestion logic across projects by supplying pre‑built processing strategies, metadata handlers and utility routines.

The documentation below explains how to use the library in your environment, describes the main processing strategies and summarises the configurable options. It assumes familiarity with Apache Spark, Delta Lake and Scala‑based Databricks environments. Installation guidance is kept deliberately brief; adapt to your organisation’s packaging standards.

## 2. Pre‑requisites and installation

The library targets Databricks Runtime **16.4 LTS** or later and is compiled for **Scala 2.13.12** or **Scala 2.12.18** . It depends on **Spark 3.5.1** and **Delta Lake 3.3.1**. Ensure the following before deploying:

- A Spark cluster (Databricks or vanilla) with Scala 2.12 / 2.13 support and Delta Lake connectors.
- Access to the Bronze and Silver storage layers using a unified path convention (see section 5).
- A metadata repository in either JSON files or a SQL Server function (see section 4).
- Standard build tooling such as *sbt* to compile the Scala sources into a JAR.

### Quick installation steps

1. **Obtain the source** – clone the repository or download the release package.
2. **Build the library** – run `sbt package` to compile the Scala code and produce a JAR. alternatively you can download the libraries from the github page.
3. **Deploy to your cluster** – upload the JAR to your Spark/Databricks cluster or include it as a library in your job configuration.
4. **Verify dependencies** – make sure the cluster has the right Spark & Delta versions and that JSON/SQL metadata is reachable.

> *Note:* If you have a custom packaging workflow (e.g., Databricks wheel or AWS EMR bootstrap action), adapt the steps accordingly. The library is platform‑agnostic as long as the above conditions are met.

## 3. Processing strategies

At the heart of Datalake Foundation is the `Processing` class (in `datalake.processing`). You instantiate it with an `Entity` (from metadata) and a **slice** file name. You then call `Process`.
The process strategy is automaticaly chosen (set in metadata of the entity). the examples below are illustratory, normaly you would call processing.Process() (without parameter) the parameter forces the strategy.

### 3.1. Full load

**Full** is used for initial loads or full refreshes. It performs an **overwrite** write into the Silver table (dynamic partition mode).

```scala
val entity = metadata.getEntity(42)
val processing = new Processing(entity, "2025‑07‑01–slice.parquet")
processing.Process(Full)
```

### 3.2. Delta (merge) processing

**Merge** supports incremental (delta) processing and executes a Delta Lake **MERGE** into the existing Silver table. Typical behaviour:

- Falls back to a full load when the Silver table does not exist.
- Optionally filters on partition values present in the slice to limit the merge scope.
- Logs warnings on schema drifts.
- Applies upsert logic based on a computed primary key and a source hash:
  - Marked-as-deleted source rows update the target to deleted.
  - Different hashes update all columns.
  - Identical rows only update `lastSeen`.
  - No match → insert.

### 3.3. Historic (SCD Type 2) processing

**Historic** implements SCD Type 2. It keeps previous versions and tracks validity using `ValidFrom`, `ValidTo`, `IsCurrent` and `SourceHash`:

- First run defaults to a full load.
- Subsequent runs match on primary key where `IsCurrent = true`.
- Changed rows are closed off (`ValidTo` set, `IsCurrent = false`) and a new row is inserted.
- New versions are appended to the Silver table.

### 3.4. Processing time override

All strategies accept an optional `processing.time` option (ISO‑8601). If absent, current time is used. Invalid timestamps are logged and default to current time.

```scala
val options = Map("processing.time" -> "2025-05-05T12:00:00")
val processing = new Processing(entity, "2025‑04‑30–slice.parquet", options)
processing.Process(Historic)
```

*Disclaimer:* The library does not validate temporal succession; providing an earlier `processing.time` than already processed data can result in inconsistent `ValidFrom/ValidTo` ranges.

## 4. Metadata management

### 4.1. JSON metadata

Use `JsonMetadataSettings` to load metadata from a JSON file. The JSON defines:

- **Connections** – e.g., storage accounts or database instances.
- **Entities** – id, source connection, destination table, column definitions (with types & business‑key indicators), optional watermark columns and preferred processing type (Full, Merge, Historic).
- **Environment** – global settings such as timezone and system field prefixes.

Duplicate entity identifiers are rejected. Use `getEntity(id)` to retrieve an `Entity` object for processing.

### 4.2. SQL Server metadata

Use `SqlMetadataSettings` to read JSON configuration from SQL Server via JDBC (e.g., function `cfg.fnGetFoundationConfig()`). Provide a `SqlServerSettings` case class (server, port, database, username, password). Errors are logged and raised.

### 4.3. Metadata‑driven helpers

- `getConnectionEntities(connection)` – all entities for a connection.
- `getGroupEntities(group)` – all entities in a logical group.
- `getConnection(...)` / `getConnectionByName(...)` – fetch connection definitions.
- `getEnvironment()` – retrieve environment settings.

These helpers enable dynamic pipeline generation (e.g., loop all entities of a connection).

## 5. Folder structure

Assumed storage layout:

**Bronze layer** – raw Parquet slices

```
<root_folder>/bronze/<connection>/<entity_name>/<slice_file>
```

**Silver layer** – processed Delta tables

```
<root_folder>/silver/<connection>/<entity_name>/<slice_file>
```

The `Entity` metadata exposes these paths via `entity.getPaths`. `Processing` composes the full slice path using `paths.bronzepath` + the slice file name.

## 6. Transformations and system columns

Before writing to Silver, the library applies:

- **Inject transformations** – custom logic declared in metadata.
- **Calculated columns** – derived fields.
- **Source hash** – SHA‑256 over all columns to detect changes.
- **Temporal tracking columns** – `ValidFrom`, `ValidTo`, `IsCurrent` for historic processing.
- **Primary key generation** – builds a hash from the business key columns if not present; rejects duplicates.
- **Column casting, renaming and normalisation** – schema enforcement.
- **Deleted flag** – to physically/logically mark deletions during merge.
- **Last seen** – timestamp updates for re-processed records.

## 7. Data Factory item generator

Provides a structured list (JSON/array) of connections, entities and slice files for use in orchestration tools like Azure Data Factory:

1. Call the generator with metadata settings and an optional date range.
2. Receive a structured set of entities and slice identifiers.
3. Feed the list into a For Each activity to execute notebooks/scripts that run `Processing` for each item.

## 8. Migration to Scala 2.13

Datalake Foundation is upgraded to Scala 2.13 for Databricks Runtime 16.4 LTS. Ensure:

- Your custom libraries are also built for Scala 2.13.
- Spark and Delta versions on your cluster match the requirements.
- You rebuild JARs that were compiled for Scala 2.12.

## 9. Summary

### IO Output
Use `io_output` in the `environment` section to configure whether entities return file system paths (`paths`) or catalog table names (`catalog`).
Individual entities can override this setting in their `settings` block using the same property. The default value is `paths` for backward compatibility.

## Scala 2.13 Migration
Databricks Runtime 16.4 LTS introduces Scala 2.13 support. The project now uses
Scala 2.13.12 with Spark 3.5.1 and Delta Lake 3.3.1 to match this runtime.
Review custom code and dependencies for compatibility with Scala 2.13 when
upgrading your environment.

---

*This documentation is a work in progress. For additional details or questions, please contact the maintainer.*
