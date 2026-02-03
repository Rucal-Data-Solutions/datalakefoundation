# Watermarks

Watermarks enable incremental processing by tracking the last processed values. This document explains how to configure watermarks and write watermark expressions.

## Overview

A watermark defines a column and expression that determines which records to process in each run. The library stores the last processed value and uses it to filter source data on subsequent runs.

## Watermark Configuration

Watermarks are defined in the entity's `watermark` array:

```json
{
  "id": 1,
  "name": "orders",
  "processtype": "merge",
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "operation_group": 0,
      "expression": "'${last_value}'"
    }
  ]
}
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `column_name` | String | The column to track for incremental processing |
| `operation` | String | Logical operator: `and` or `or` |
| `operation_group` | Integer | Group watermarks for complex conditions |
| `expression` | String | Dynamic expression for the watermark value |

## Expression System

Watermark expressions are evaluated at runtime using a dynamic expression engine. The expression produces the filter value used in processing.

### Available Parameters

| Parameter | Description |
|-----------|-------------|
| `${last_value}` | The last processed watermark value |
| `${watermark}` | Alias for `${last_value}` |
| `${b19_epoch_day}` | Days since 1900-01-01 (for date-based systems) |
| `${reflex_now}` | Alias for `${b19_epoch_day}` |

### Available Libraries

Expressions can use these Java time classes:

- `java.time.LocalDate`
- `java.time.LocalDateTime`
- `java.time.LocalTime`
- `java.time.format.DateTimeFormatter`

### Pre-defined Objects

| Object | Value |
|--------|-------|
| `defaultFormat` | `DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")` |
| `b19_epoch_day` | Days from 1900-01-01 to today |

## Expression Examples

### Simple Last Value

Use the last processed value directly:

```json
{
  "column_name": "ModifiedDate",
  "operation": "and",
  "expression": "'${last_value}'"
}
```

### Date Arithmetic

Process records from 7 days before the last value:

```json
{
  "column_name": "EventDate",
  "operation": "and",
  "expression": "LocalDate.parse('${last_value}').minusDays(7).toString()"
}
```

### Epoch Day Calculation

For systems using days since epoch:

```json
{
  "column_name": "DayNumber",
  "operation": "and",
  "expression": "${b19_epoch_day} - 1"
}
```

### Formatted Date

Parse and reformat the last value:

```json
{
  "column_name": "ProcessDate",
  "operation": "and",
  "expression": "LocalDateTime.parse('${last_value}', defaultFormat).format(DateTimeFormatter.ISO_LOCAL_DATE)"
}
```

## Multiple Watermarks

Entities can have multiple watermarks combined with logical operators.

### AND Conditions

Both conditions must be met:

```json
{
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "operation_group": 0,
      "expression": "'${last_value}'"
    },
    {
      "column_name": "Region",
      "operation": "and",
      "operation_group": 0,
      "expression": "'EMEA'"
    }
  ]
}
```

Generated filter: `ModifiedDate >= <value> AND Region >= 'EMEA'`

### OR Conditions

Either condition triggers processing:

```json
{
  "watermark": [
    {
      "column_name": "CreateDate",
      "operation": "or",
      "operation_group": 0,
      "expression": "'${last_value}'"
    },
    {
      "column_name": "UpdateDate",
      "operation": "or",
      "operation_group": 0,
      "expression": "'${last_value}'"
    }
  ]
}
```

Generated filter: `CreateDate >= <value> OR UpdateDate >= <value>`

### Operation Groups

Group watermarks for complex logic:

```json
{
  "watermark": [
    {
      "column_name": "ModifiedDate",
      "operation": "and",
      "operation_group": 0,
      "expression": "'${last_value}'"
    },
    {
      "column_name": "Status",
      "operation": "or",
      "operation_group": 1,
      "expression": "'PENDING'"
    },
    {
      "column_name": "Status",
      "operation": "or",
      "operation_group": 1,
      "expression": "'ACTIVE'"
    }
  ]
}
```

Generated filter: `ModifiedDate >= <value> AND (Status = 'PENDING' OR Status = 'ACTIVE')`

## Watermark Lifecycle

### First Run

On the first run (no previous watermark value stored):
1. Watermark filter is not applied
2. All source records are processed
3. Current max value is stored for next run

### Subsequent Runs

1. Previous watermark value is retrieved
2. Expression is evaluated with `${last_value}`
3. Source is filtered using the watermark condition
4. After successful processing, new max value is stored

### Watermark Storage

Watermark values are stored in a system table and associated with the entity ID. The `WatermarkData` class handles storage and retrieval.

## Resetting Watermarks

To reprocess data, you can reset watermarks programmatically:

```scala
val entity = metadata.getEntity(42)

// Reset to null (will trigger full reprocess)
entity.Watermark.foreach(_.Reset)

// Reset to specific value
entity.Watermark.foreach(_.Reset("2024-01-01 00:00:00.0"))
```

## Common Patterns

### Timestamp-Based Incremental

```json
{
  "column_name": "LastModified",
  "operation": "and",
  "expression": "'${last_value}'"
}
```

### Sequence Number

```json
{
  "column_name": "SeqNr",
  "operation": "and",
  "expression": "'${last_value}'"
}
```

### Date with Buffer

Include a 3-day overlap to catch late-arriving data:

```json
{
  "column_name": "EventDate",
  "operation": "and",
  "expression": "LocalDate.parse('${last_value}').minusDays(3).toString()"
}
```

### Legacy Epoch Day Systems

For systems using days since 1900-01-01:

```json
{
  "column_name": "BatchDay",
  "operation": "and",
  "expression": "${reflex_now}"
}
```

## Watermarks and Delete Inference

Watermarks play a critical role in [Delete Inference](DELETE_INFERENCE.md). The watermark window defines the scope of records that should be present in the source slice. Records in the target within this window that are missing from the source are candidates for soft-delete.

## Error Handling

If expression evaluation fails:
1. The error is logged
2. The watermark value returns `None`
3. Processing continues without that watermark filter

Common errors:
- Invalid date format in expression
- Missing `last_value` on first run (handled gracefully)
- Syntax errors in expressions

## See Also

- [Processing Strategies](PROCESSING_STRATEGIES.md)
- [Delete Inference](DELETE_INFERENCE.md)
- [Entity Configuration](../configuration/ENTITY_CONFIGURATION.md)
