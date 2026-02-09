# Processing Strategies

Datalake Foundation provides three processing strategies for moving data from bronze to silver layer. This document explains each strategy, when to use it, and how it behaves.

## Overview

| Strategy | Use Case | Behavior |
|----------|----------|----------|
| **Full** | Initial loads, complete refreshes | Overwrites silver table with partition pruning |
| **Merge** | Incremental delta processing | Upserts based on primary key and source hash |
| **Historic** | SCD Type 2 tracking | Maintains version history with temporal columns |

## Common Processing Pipeline

All strategies share a common transformation pipeline before writing:

1. **Inject transformations** - Apply custom expressions from entity config
2. **Add calculated columns** - Create derived columns with expressions
3. **Compute source hash** - SHA-256 of all columns for change detection
4. **Add temporal columns** - ValidFrom, ValidTo, IsCurrent (Historic only)
5. **Add filename column** - Track source slice file
6. **Generate primary key** - Hash of business key columns
7. **Cast columns** - Convert to target data types
8. **Rename columns** - Apply column name mappings
9. **Add deleted flag** - Soft-delete tracking column
10. **Add lastSeen timestamp** - Track when record was last processed
11. **Normalize column names** - Standardize naming format
12. **Cache DataFrame** - Cache the result for reuse during processing

## Full Strategy

The simplest strategy - performs a complete overwrite of the silver table.

### Configuration

```json
{
  "id": 1,
  "name": "reference_data",
  "processtype": "full",
  ...
}
```

### Behavior

1. Reads the source parquet slice from bronze layer
2. Applies the transformation pipeline
3. Writes to silver using `SaveMode.Overwrite`
4. Uses dynamic partition overwrite mode (only replaces partitions present in source)

### When to Use

- Initial data loads
- Reference/dimension tables that are small and fully refreshed
- Recovery scenarios requiring complete reload
- Tables without incremental change tracking

### Code Flow

```scala
val processing = new Processing(entity, "2025-07-01-slice.parquet")
processing.Process(Full)
// or
processing.Process() // Uses entity's configured processtype
```

### Partition Handling

With dynamic partition overwrite mode, only partitions present in the source slice are replaced:

```
Source slice contains: year=2024, year=2025
Silver table has: year=2023, year=2024, year=2025

Result: year=2024 and year=2025 are replaced
        year=2023 is preserved
```

## Merge Strategy

Incremental processing using Delta Lake MERGE operations. Handles inserts, updates, and soft deletes.

### Configuration

```json
{
  "id": 2,
  "name": "transactions",
  "processtype": "merge",
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "expression": "'${last_value}'"
    }
  ],
  ...
}
```

### Behavior

1. **First run**: Falls back to Full strategy (no existing table to merge into)
2. **Subsequent runs**:
   - Matches records by primary key
   - Applies partition filters to limit merge scope
   - Executes merge with these rules:

| Condition | Action |
|-----------|--------|
| Source `deleted = true` | Update target's `deleted` flag and `lastSeen` |
| Source hash ≠ Target hash | Update all columns |
| Source hash = Target hash | Update only `lastSeen` |
| No match in target | Insert new record |

### When to Use

- Large tables with incremental changes
- Systems that provide change data capture (CDC)
- Tables where you need upsert semantics
- When you want soft-delete support

### Metrics

Metrics are reported for incremental runs. On first run, processing diverts to the Full strategy.

After processing, the strategy logs detailed metrics:

| Metric | Description |
|--------|-------------|
| **recordsInSlice** | Total records in the source slice |
| **inserted** | New records added (no matching primary key in target) |
| **updated** | All matched records, including both data changes and lastSeen-only touches |
| **deleted** | Source records that arrived with the deleted flag set to true. This is a source-side metric and does not include records removed via [delete inference](DELETE_INFERENCE.md) |

The metric identity holds: **`inserted + updated + deleted = recordsInSlice`**

> **Note: Breaking change from previous versions.**
> Earlier versions reported separate `updated` (source hash changed) and `touched` (source hash unchanged, only lastSeen updated) metrics. These have been intentionally combined into a single `updated` metric to eliminate an expensive pre-merge join. The `touched` metric is always reported as 0 and should not be relied upon. If you have monitoring dashboards or alerting that depends on the old `updated` vs `touched` distinction, you must update them to use the combined `updated` metric. Users who need to distinguish real data changes from touch-only updates should compare source hashes independently.

### Delete Inference

When `delete_missing` is enabled, records in the target that are missing from the source (within the watermark window) are automatically soft-deleted. See [Delete Inference](DELETE_INFERENCE.md) for details.

## Historic Strategy (SCD Type 2)

Implements Slowly Changing Dimensions Type 2, maintaining a complete version history.

### Configuration

```json
{
  "id": 3,
  "name": "customer",
  "processtype": "historic",
  ...
}
```

### System Columns

The Historic strategy adds temporal tracking columns:

| Column | Type | Description |
|--------|------|-------------|
| `{prefix}ValidFrom` | Timestamp | When this version became active |
| `{prefix}ValidTo` | Timestamp | When this version was superseded (null = current) |
| `{prefix}IsCurrent` | Boolean | Whether this is the current version |
| `{prefix}SourceHash` | String | Hash for change detection |

### Behavior

1. **First run**: Falls back to Full strategy
2. **Subsequent runs**:
   - Matches on primary key WHERE `IsCurrent = true`
   - When source hash differs from target:
     - Close current version: Set `ValidTo` and `IsCurrent = false`
     - Insert new version with updated values
   - New records (no PK match) are inserted directly

### Version Lifecycle

```
Initial load:
  Customer 123: {data_v1, ValidFrom=2024-01-01, ValidTo=null, IsCurrent=true}

Update received:
  Customer 123: {data_v2, ...}

After processing:
  Customer 123: {data_v1, ValidFrom=2024-01-01, ValidTo=2024-06-15, IsCurrent=false}
  Customer 123: {data_v2, ValidFrom=2024-06-15, ValidTo=null, IsCurrent=true}
```

### When to Use

- Dimension tables requiring audit trail
- Regulatory/compliance requirements for history
- Analytics needing point-in-time snapshots
- Master data where changes must be tracked

### Metrics

Metrics are reported for incremental runs. On first run, processing diverts to the Full strategy.

After processing, the strategy logs detailed metrics:

| Metric | Description |
|--------|-------------|
| **recordsInSlice** | Total records in the source slice |
| **inserted** | New records with no matching primary key in the target |
| **updated** | Records where the source hash differs from the current target version. Each updated record closes the previous version (sets ValidTo and IsCurrent=false) and appends a new current version. The `updated` count directly equals the number of new historical versions created in this processing run. |
| **unchanged** | Records that matched a current target record with the same hash. No new version is created. |
| **deleted** | Target records removed via [delete inference](DELETE_INFERENCE.md) (not matched by source). This is a target-side metric from the Delta merge operation and is not included in the source-side identity. |

Source-side identity: **`inserted + updated + unchanged = recordsInSlice`**

> **Note:** The `deleted` metric has different semantics between Merge and Historic strategies. In Merge, `deleted` counts source records arriving with the deleted flag (source-side). In Historic, `deleted` counts target records removed via delete inference (target-side). These values are not directly comparable. Unlike Merge metrics, Historic metrics also include an `unchanged` count for records that matched but did not require a new version.

### Querying Historic Data

```sql
-- Current state
SELECT * FROM silver.customer WHERE dlf_IsCurrent = true

-- Point-in-time snapshot
SELECT * FROM silver.customer
WHERE dlf_ValidFrom <= '2024-03-15'
  AND (dlf_ValidTo > '2024-03-15' OR dlf_ValidTo IS NULL)

-- Full history for a customer
SELECT * FROM silver.customer
WHERE customer_id = 123
ORDER BY dlf_ValidFrom
```

## Processing Time Override

All strategies accept an optional processing time parameter. This controls the timestamp used for temporal columns.

### Usage

```scala
val options = Map("processing.time" -> "2025-05-05T12:00:00")
val processing = new Processing(entity, "slice.parquet", options)
processing.Process()
```

### Behavior

- If not specified, uses current system time
- Must be ISO-8601 format
- Invalid timestamps log a warning and fall back to current time

### Caution

The library does not validate temporal succession. Providing an earlier `processing.time` than previously processed data can result in inconsistent `ValidFrom/ValidTo` ranges in Historic mode.

## Choosing a Strategy

```
                    ┌─────────────────────────────┐
                    │   Do you need version       │
                    │   history (SCD Type 2)?     │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────┴───────────────┐
                    ▼                             ▼
                   Yes                           No
                    │                             │
                    ▼                             ▼
               Historic              ┌────────────────────────┐
                                     │  Is data incrementally │
                                     │  changing?             │
                                     └───────────┬────────────┘
                                                 │
                                    ┌────────────┴────────────┐
                                    ▼                         ▼
                                   Yes                       No
                                    │                         │
                                    ▼                         ▼
                                 Merge                      Full
```

## Error Handling

### Duplicate Business Keys

If the source slice contains duplicate business keys, processing fails with `DuplicateBusinesskeyException`. This prevents data quality issues in the silver layer.

### Schema Drift

All strategies detect schema differences between source and target:
- Column additions are logged as warnings
- Column removals are logged as warnings
- Type mismatches are logged as warnings

Processing continues with the current source schema.

## See Also

- [Entity Configuration](../configuration/ENTITY_CONFIGURATION.md)
- [Watermarks](WATERMARKS.md)
- [Delete Inference](DELETE_INFERENCE.md)
