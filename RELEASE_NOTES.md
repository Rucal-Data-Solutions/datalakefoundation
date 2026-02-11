# Datalake Foundation Release Notes

## v1.6.0

### Platform upgrades

- **Spark 4.0.0** — upgraded from Spark 3.5.2.
- **Scala 2.13 only** — dropped Scala 2.12 cross-compilation target.
- **Delta Lake 4.0** — `merge.execute()` now returns a metrics DataFrame directly; all strategies consume it.
- **Removed `enableHiveSupport()`** from all `SparkSession.builder()` calls for Spark Connect / Databricks compatibility.

### Structured logging

The logging subsystem has been redesigned from the ground up.

- **`ProcessingSummary`** — every processing strategy (Full, Merge, Historic) emits structured metrics at the end of each run: `records_in_slice`, `inserted`, `updated`, `deleted`, `unchanged`, `touched`.
- **`TableAppender`** (new) — logs can now be written to a Unity Catalog Delta table in addition to Parquet files. Configurable via `log_appender_type` (`table` | `parquet`) and `log_output` in the environment.
- **Async logging** — all log appenders are wrapped in an `AsyncAppender` with a 1024-event ring buffer to avoid blocking processing.
- **Run ID** — each session is tagged with a UUID `run_id` propagated to every log entry for end-to-end traceability.
- **Structured context data** — `DatalakeLogManager.withData()` attaches arbitrary JSON payloads (e.g., entity metadata, stack traces) to log events via Log4j `ThreadContext`.
- **Graceful shutdown** — a `SparkListener` and JVM shutdown hook ensure the log buffer is flushed before the session ends.
- **`println` eliminated** — Watermark expression errors and schema diff output now go through Log4j instead of stdout.
- **Noisy log suppression** — Spark parser cache, SparkSession Connect warnings, and Databricks edge config messages are suppressed at `WARN`/`ERROR` level.

### Reliability and data safety

- **`isFirstRun` hardened** — the duplicated first-run detection logic has been extracted to `ProcessStrategy.isFirstRun()`. It now catches only `AnalysisException` (legitimate "table does not exist") and **re-throws** all other exceptions (permission errors, network timeouts, catalog misconfiguration) instead of silently defaulting to a full overwrite. This prevents accidental data loss.
- **Calculated column failures are now fatal** — a failing expression in `addCalculatedColumns` throws a `DatalakeException` instead of logging and continuing with a broken DataFrame.

### Processing metrics improvements

- **Merge** — derives `inserted` / `updated` / `deleted` from Delta merge metrics and source-side arithmetic. The expensive pre-merge `source JOIN target` scan used to pre-calculate real vs. touch updates has been removed.
- **Historic** — computes `unchanged` by joining source against current target records post-merge, guaranteeing the identity `inserted + updated + unchanged = recordCount`. `deleted` is read from the Delta metrics row.
- **Full** — reports `inserted = recordCount`.

### New features

- **Entity connection groups** — entities can now carry an optional `connectiongroup` field. A new `EntityConnectionGroup` class and `Metadata.getEntities(connectionGroup)` query enable batch-processing entities by connection group.
- **Entity `toJson`** — `Entity` now exposes a `toJson` method for structured logging and diagnostics.

### Code cleanup

- Removed ~80 lines of dead commented-out code from `implicits.scala` (unused file-lock helpers, stale imports).
- Removed unused `joda.time.DateTime` import from `Watermark.scala`.
- Cleaned up unused imports (`DataFrameWriter`, `DataFrameReader`, `LocalDateTime`) across source files.
- Replaced `LogManager.getLogger` with `DatalakeLogManager.getLogger` throughout processing classes.
- Simplified `addLastSeen` (removed unused local variables).
- Refined `addFilenameColumn` to only warn about the missing column when running against Unity Catalog table sources.

### Documentation

New documentation added:

- [Entity Configuration](docs/configuration/ENTITY_CONFIGURATION.md)
- [Metadata Sources](docs/configuration/METADATA_SOURCES.md)
- [Processing Strategies](docs/processing/PROCESSING_STRATEGIES.md)
- [Watermarks](docs/processing/WATERMARKS.md)
- [Delete Inference](docs/processing/DELETE_INFERENCE.md)
- [IO Output Modes](docs/outputs/IO_OUTPUT_MODES.md)

### Test coverage

New test suites:

- `AsyncContextSpec` — async logging context propagation
- `LoggingImpactSpec` — logging performance impact
- `TableAppenderSpec` — Delta table log appender
- `HistoricProcessingSpec` — SCD Type 2 strategy end-to-end
- `FullProcessingSpec` — full-load strategy end-to-end
- `IsFirstRunSpec` — first-run detection edge cases
- `CalculatedColumnSpec` — calculated column expression handling
- `WatermarkSpec` — watermark expression evaluation

### Migration notes

| Topic | Action required |
|---|---|
| **Spark version** | Upgrade your cluster to Spark 4.0.0. |
| **Scala version** | Rebuild all dependent JARs for Scala 2.13. Scala 2.12 is no longer supported. |
| **Logging output** | If you previously relied on `dlf_log.parquet` output, no action needed (still the default). To use the new table appender, set `log_appender_type` to `table` and `log_output` to a catalog table name in your environment configuration. |
| **Error handling** | Calculated column failures now throw instead of being silently ignored. Review your entity column expressions to ensure they are valid. |
| **First-run detection** | Permission or catalog errors during table-existence checks now throw instead of falling back to a full load. Ensure your service principal has read access to the silver catalog/paths. |

---

## v1.5.1

Initial tracked release on `main`. Spark 3.5.2, Scala 2.12 + 2.13 cross-build. Includes Merge, Historic, and Full processing strategies with JSON, SQL Server, and directory-based metadata sources.
