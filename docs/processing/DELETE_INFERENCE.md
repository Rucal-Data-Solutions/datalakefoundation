# Delete Inference

Delete inference automatically soft-deletes records that exist in the target but are missing from the source slice. This document explains how the feature works, its configuration, and edge cases to be aware of.

## Overview

When processing incremental data, source systems may not always send explicit delete records. The delete inference feature (`delete_missing`) detects these implicit deletes by comparing what records *should* exist in the current slice (based on the watermark window) against what actually exists.

## Enabling Delete Inference

Enable delete inference in the entity settings:

```json
{
  "id": 1,
  "name": "orders",
  "processtype": "merge",
  "settings": {
    "delete_missing": true
  },
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "expression": "'${last_value}'"
    }
  ]
}
```

Requirements:
- `delete_missing: true` in entity settings
- At least one watermark column defined
- Merge or Historic processing strategy

## How It Works

### Step 1: Define the Watermark Window

The watermark window defines the range of records expected in the current slice:

```
Previous watermark value: 2024-06-01  (last processed)
Current watermark value:  2024-06-15  (max in current slice)

Window: ModifiedDate >= '2024-06-01' AND ModifiedDate <= '2024-06-15'
```

### Step 2: Identify Missing Records

During the merge operation, records in the target that:
1. Fall within the watermark window
2. Match the partition filter (if applicable)
3. Are NOT already deleted
4. Have no matching record in the source

...are candidates for deletion.

### Step 3: Soft Delete

Missing records are marked as deleted:
- `{prefix}deleted = true`
- `{prefix}lastSeen = <processing_time>`
- For Historic: `{prefix}IsCurrent = false`, `{prefix}ValidTo = <processing_time>`

## Safety Filters

Delete inference applies multiple safety filters to prevent accidental deletions:

### Watermark Window

Only considers records within the expected data range. Records outside this window are not affected.

```sql
target.ModifiedDate >= '2024-06-01'
AND target.ModifiedDate <= '2024-06-15'
```

### Partition Filter

If the entity has partition columns, only records in partitions present in the source slice are considered.

```sql
target.Region IN ('EMEA', 'APAC')  -- Partitions found in source
```

### Not Already Deleted

Avoids repeatedly updating already-deleted records.

```sql
target.dlf_deleted = false
```

### IsCurrent (Historic Only)

For SCD Type 2, only operates on current versions.

```sql
target.dlf_IsCurrent = true
```

## Edge Cases

### First Run (No Previous Watermark)

On the first run, there is no previous watermark value. Delete inference is safely disabled:

```
WARNING: inferDeletesFromMissing is enabled but no previous watermark values
available. No deletes will be inferred. This is expected on first run.
```

### NULL Watermark Values

Records with NULL values in watermark columns will NOT be considered for deletion. This is logged as a warning:

```
WARNING: Records with NULL values in watermark columns will NOT be
considered for deletion.
```

### Missing Watermark Columns

If the watermark column doesn't exist in the target schema, that column is skipped:

```
WARNING: Watermark column 'ModifiedDate' not found in target schema;
skipping it for delete detection.
```

### Gaps in Processing

Large gaps between the previous and current watermark may cause issues:

```
Previous: 2024-01-01
Current:  2024-12-31  (11 month gap)
```

If the source system only provides recent changes, records modified in the middle of this gap may be incorrectly marked as deleted. Design your watermark expressions to minimize gaps.

## Strategy-Specific Behavior

### Merge Strategy

When a record in the Silver table is not matched by any record in the current source batch (within the watermark window), the library marks it as soft-deleted by setting the `deleted` flag to `true` and updating `lastSeen` to the current processing timestamp.

### Historic Strategy

When a record in the Silver table is not matched by any record in the current source batch (within the watermark window), the library closes the current version by:
- Setting `deleted` to `true`
- Setting `IsCurrent` to `false`
- Setting `ValidTo` to the current processing timestamp
- Updating `lastSeen` to the current processing timestamp

## Logging

Delete inference logs key information:

```
INFO: Delete detection window for column 'ModifiedDate':
      [2024-06-01 <= ModifiedDate <= 2024-06-15]

INFO: inferDeletesFromMissing is active. Records in the watermark window
      that are missing from the source will be marked as deleted.

WARNING: Note: Records with NULL values in watermark columns will NOT be
         considered for deletion.
```

## Example Scenario

### Source System Behavior

Orders system sends all orders modified since last sync. When an order is deleted, it simply stops appearing in subsequent syncs.

### Configuration

```json
{
  "id": 10,
  "name": "orders",
  "processtype": "merge",
  "watermark": [
    {
      "column_name": "LastModified",
      "operation": "and",
      "expression": "'${last_value}'"
    }
  ],
  "columns": [
    {"name": "OrderID", "fieldroles": ["businesskey"]},
    {"name": "CustomerID", "fieldroles": []},
    {"name": "LastModified", "fieldroles": []}
  ],
  "settings": {
    "delete_missing": true
  }
}
```

### Processing Flow

**Run 1 (Initial)**
- Source: Orders 1, 2, 3
- Previous watermark: None
- Delete inference: Disabled (first run)
- Result: All orders inserted, watermark set to max(LastModified)

**Run 2 (Normal)**
- Source: Orders 2, 3, 4
- Previous watermark: 2024-06-01
- Current watermark: 2024-06-15
- Delete inference: Active
- Order 1 is in window but missing from source → Soft deleted
- Order 4 is new → Inserted

**Run 3 (With Late Data)**
- Source: Orders 2, 3
- Previous watermark: 2024-06-15
- Current watermark: 2024-06-20
- Delete inference: Active
- Order 4 now in window but missing → Soft deleted

## Best Practices

1. **Use appropriate watermark expressions** - Include buffer time if source has late-arriving data
2. **Monitor delete counts** - Unusual spikes may indicate issues
3. **Test with realistic data** - Verify behavior matches source system patterns
4. **Consider partitioning** - Reduces scope of delete detection
5. **Handle NULL watermarks** - Ensure source data has valid watermark values

## Disabling Delete Inference

If delete inference causes issues, disable it by removing or setting `delete_missing: false`:

```json
{
  "settings": {
    "delete_missing": false
  }
}
```

## See Also

- [Watermarks](WATERMARKS.md)
- [Processing Strategies](PROCESSING_STRATEGIES.md)
- [Entity Configuration](../configuration/ENTITY_CONFIGURATION.md)
