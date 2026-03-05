# Expression Languages

Datalake Foundation uses two different expression languages depending on context. This document describes both and where each is used.

## Overview

| Language | Used For | Evaluated By |
|----------|----------|--------------|
| **Spark SQL expressions** | Calculated columns, transformations | Apache Spark |
| **Scala expressions** | Path/table settings, watermark values | Scala runtime |

## Spark SQL Expressions

Calculated columns and entity transformations use **Apache Spark SQL expression syntax**. These expressions support all Spark SQL built-in functions, column references, literals, arithmetic, string operations, conditional logic, and more.

### Calculated Columns

When a column has the `calculated` field role, its `expression` field contains a Spark SQL expression that produces the column value.

```json
{
  "name": "",
  "newname": "Region",
  "datatype": "string",
  "fieldroles": ["calculated"],
  "expression": "'EMEA'"
}
```

The expression can reference other columns in the DataFrame:

```json
{
  "name": "",
  "newname": "FullName",
  "datatype": "string",
  "fieldroles": ["calculated"],
  "expression": "concat(first_name, ' ', last_name)"
}
```

More examples:

| Expression | Result |
|------------|--------|
| `"current_date()"` | Today's date |
| `"current_timestamp()"` | Current timestamp |
| `"'EMEA'"` | String literal `EMEA` |
| `"upper(name)"` | Column `name` in uppercase |
| `"year(order_date)"` | Year extracted from `order_date` column |
| `"coalesce(phone, 'N/A')"` | `phone` column with fallback |
| `"950"` | Integer literal `950` |

### Entity Transformations

Transformations apply Spark SQL SELECT expressions to reshape the entire DataFrame. Each transformation is defined as an array of column expressions, similar to a SQL `SELECT` clause.

```json
{
  "transformations": [
    ["customer_id", "upper(name) as Name", "concat(city, ', ', country) as Location"],
    ["*", "year(order_date) as OrderYear"]
  ]
}
```

Each array in the list represents one transformation step. Multiple steps are applied in sequence. A transformation step replaces the current columns — only columns listed in the expression array are kept (use `"*"` to include all existing columns alongside new ones).

A single-expression transformation can also be specified as a plain string:

```json
{
  "transformations": [
    ["*", "upper(name) as Name"]
  ]
}
```

### Spark SQL Reference

For a complete list of available functions, see the [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/) reference.

## Scala Expressions

Path settings, table settings, and watermark values use **Scala string interpolation**. Variables are referenced with `${variable}` syntax and are resolved at runtime. Any valid Scala expression that produces a string is supported.

### Path and Table Settings

Path and table settings support the following variables:

| Variable | Description |
|----------|-------------|
| `${connection}` | Connection name |
| `${entity}` | Entity source name |
| `${destination}` | Entity destination name |
| `${today}` | Current date in `yyyyMMdd` format |
| `${settings_<key>}` | Value of any setting by key |

Examples:

```json
{
  "silver_table": "silver_${connection}.${destination}",
  "bronze_path": "/${connection}/${entity}/${today}",
  "silver_path": "/${settings_database}/${destination}"
}
```

These expressions are evaluated whenever path or table names are resolved, so `${today}` always reflects the current processing date.

### Watermark Expressions

Watermark expressions use a different set of variables and also support Java time libraries for date arithmetic.

| Variable | Description |
|----------|-------------|
| `${last_value}` | The last processed watermark value |
| `${watermark}` | Alias for `${last_value}` |
| `${b19_epoch_day}` | Days since 1900-01-01 (for date-based systems) |

Available Java time classes:

- `java.time.LocalDate`
- `java.time.LocalDateTime`
- `java.time.LocalTime`
- `java.time.format.DateTimeFormatter`

A pre-defined `defaultFormat` object is available: `DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")`.

Examples:

```json
// Simple last value passthrough
{ "expression": "'${last_value}'" }

// Date arithmetic: 7 days before last value
{ "expression": "${LocalDate.parse(last_value).minusDays(7)}" }

// Epoch day calculation
{ "expression": "${b19_epoch_day - 1}" }

// Reformat the last value
{ "expression": "${LocalDateTime.parse(last_value, defaultFormat).format(DateTimeFormatter.ISO_LOCAL_DATE)}" }
```

For full watermark configuration details, see [Watermarks](../processing/WATERMARKS.md).

## See Also

- [Entity Configuration](ENTITY_CONFIGURATION.md)
- [Watermarks](../processing/WATERMARKS.md)
- [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/)
