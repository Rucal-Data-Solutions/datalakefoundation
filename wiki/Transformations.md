# Transformations and System Columns

Before writing data to the Silver layer, Datalake Foundation applies a comprehensive set of transformations and adds system columns for tracking and processing purposes.

## Overview

The transformation pipeline ensures data quality, consistency, and traceability by:
- Applying custom business logic transformations
- Adding calculated columns and derived fields
- Generating tracking columns for change detection
- Implementing data quality and validation rules
- Normalizing schema and data types

## System Columns

Datalake Foundation automatically adds several system columns to track data lineage, changes, and validity periods.

### Standard System Columns

| Column | Purpose | Data Type | Populated By |
|--------|---------|-----------|--------------|
| `SourceHash` | SHA-256 hash of all source columns | String | All strategies |
| `ValidFrom` | Start timestamp for record validity | Timestamp | Historic strategy |
| `ValidTo` | End timestamp for record validity | Timestamp | Historic strategy |
| `IsCurrent` | Flag indicating current record version | Boolean | Historic strategy |
| `LastSeen` | Timestamp when record was last processed | Timestamp | Merge strategy |
| `IsDeleted` | Flag for logical deletion | Boolean | Merge strategy |

### Source Hash Generation

The source hash is calculated using SHA-256 over all source columns to detect changes:

```scala
// Conceptual implementation
val sourceColumns = entity.columns.filterNot(_.isSystemColumn)
val hashInput = sourceColumns.map(col => row.getAs[Any](col.name)).mkString("|")
val sourceHash = MessageDigest.getInstance("SHA-256").digest(hashInput.getBytes).map("%02x".format(_)).mkString
```

**Usage**:
- **Change detection**: Compare hashes to identify modified records
- **Duplicate detection**: Identify identical records across processing runs
- **Data integrity**: Verify data hasn't been corrupted during processing

### Temporal Tracking Columns (Historic Strategy)

For SCD Type 2 processing, temporal columns track record validity:

**ValidFrom**:
- Set to processing time for new records
- Inherited from previous version for unchanged records
- Used to establish record timeline

**ValidTo**:
- NULL for current records (`IsCurrent = true`)
- Set to processing time when record is superseded
- Defines end of validity period

**IsCurrent**:
- `true` for the most recent version of each business key
- `false` for historical versions
- Used in queries to get current state

### Example Historic Tracking

```sql
-- Sample data showing historic tracking
SELECT customer_id, customer_name, ValidFrom, ValidTo, IsCurrent, SourceHash
FROM dim_customer 
WHERE customer_id = 123
ORDER BY ValidFrom;

-- Results:
-- customer_id | customer_name | ValidFrom           | ValidTo             | IsCurrent | SourceHash
-- 123         | John Doe      | 2025-01-01 10:00:00 | 2025-01-15 14:30:00 | false     | abc123...
-- 123         | John D. Smith | 2025-01-15 14:30:00 | NULL                | true      | def456...
```

## Custom Transformations

### Inject Transformations

Define custom logic in metadata to apply during processing:

```json
{
  "entities": [
    {
      "id": 42,
      "name": "customer",
      "transformations": [
        {
          "type": "inject",
          "name": "full_name",
          "expression": "concat(first_name, ' ', last_name)",
          "dataType": "string"
        },
        {
          "type": "inject", 
          "name": "age_category",
          "expression": "case when age < 18 then 'Minor' when age < 65 then 'Adult' else 'Senior' end",
          "dataType": "string"
        }
      ]
    }
  ]
}
```

### Calculated Columns

Add derived fields based on existing data:

```json
{
  "transformations": [
    {
      "type": "calculated",
      "name": "annual_revenue",
      "expression": "monthly_revenue * 12",
      "dataType": "decimal(15,2)"
    },
    {
      "type": "calculated",
      "name": "profit_margin",
      "expression": "(revenue - costs) / revenue * 100",
      "dataType": "decimal(5,2)"
    }
  ]
}
```

### Conditional Transformations

Apply transformations based on conditions:

```json
{
  "transformations": [
    {
      "type": "conditional",
      "name": "risk_score",
      "conditions": [
        {
          "when": "credit_score >= 750",
          "then": "'Low'",
          "dataType": "string"
        },
        {
          "when": "credit_score >= 650",
          "then": "'Medium'",
          "dataType": "string"
        }
      ],
      "else": "'High'",
      "dataType": "string"
    }
  ]
}
```

## Column Processing

### Data Type Casting

Automatic casting ensures schema consistency:

```json
{
  "columns": [
    {
      "name": "customer_id",
      "type": "integer",
      "source_type": "string"  // Auto-cast from string to integer
    },
    {
      "name": "created_date",
      "type": "date",
      "source_type": "string",
      "format": "yyyy-MM-dd"  // Parse string to date
    }
  ]
}
```

### Column Renaming

Standardize column names during processing:

```json
{
  "columns": [
    {
      "name": "customer_id",
      "source_name": "cust_id"  // Rename from cust_id to customer_id
    },
    {
      "name": "full_name",
      "source_name": "customer_name"
    }
  ]
}
```

### Column Normalization

Apply standard formatting and validation:

```json
{
  "columns": [
    {
      "name": "email",
      "type": "string",
      "transformations": [
        {
          "type": "normalize",
          "operation": "lower_case"
        },
        {
          "type": "validate",
          "pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
        }
      ]
    }
  ]
}
```

## Primary Key Generation

### Business Key Processing

Generate primary keys from business key columns:

```json
{
  "columns": [
    {
      "name": "customer_id",
      "type": "integer",
      "role": "business_key"
    },
    {
      "name": "account_number",
      "type": "string", 
      "role": "business_key"
    }
  ]
}
```

The system automatically:
1. Combines business key columns
2. Generates a hash-based primary key
3. Validates uniqueness within the slice
4. Rejects duplicates with detailed error messages

### Composite Key Example

```scala
// For customer_id=123 and account_number="ACC001"
// Primary key = hash("123|ACC001")
val businessKeyValue = "123|ACC001"
val primaryKey = generateHash(businessKeyValue)
```

## Data Quality Rules

### Validation Rules

Define validation rules in metadata:

```json
{
  "columns": [
    {
      "name": "age",
      "type": "integer",
      "validation": {
        "min": 0,
        "max": 150,
        "required": true
      }
    },
    {
      "name": "email",
      "type": "string",
      "validation": {
        "pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        "required": false
      }
    }
  ]
}
```

### Null Handling

Configure null value handling:

```json
{
  "columns": [
    {
      "name": "phone_number",
      "type": "string",
      "null_handling": {
        "strategy": "default",
        "default_value": "Unknown"
      }
    },
    {
      "name": "optional_field",
      "type": "string",
      "null_handling": {
        "strategy": "preserve"
      }
    }
  ]
}
```

## Deletion Handling

### Logical Deletion

Mark records as deleted without physical removal:

```json
{
  "entities": [
    {
      "id": 42,
      "deletion_strategy": "logical",
      "deletion_column": "is_deleted",
      "deletion_marker": true
    }
  ]
}
```

### Physical Deletion

Remove records during merge operations:

```json
{
  "entities": [
    {
      "id": 42,
      "deletion_strategy": "physical"
    }
  ]
}
```

## Configuration Options

### System Column Prefixes

Customize system column naming:

```json
{
  "environment": {
    "system_prefix": "sys_",
    "hash_column_name": "sys_source_hash",
    "valid_from_column": "sys_valid_from",
    "valid_to_column": "sys_valid_to"
  }
}
```

### Transformation Settings

Global transformation settings:

```json
{
  "environment": {
    "timezone": "UTC",
    "date_format": "yyyy-MM-dd",
    "timestamp_format": "yyyy-MM-dd HH:mm:ss",
    "null_string_representations": ["NULL", "null", "", "N/A"]
  }
}
```

## Advanced Transformations

### Custom Expression Language

Use Spark SQL expressions in transformations:

```json
{
  "transformations": [
    {
      "type": "inject",
      "name": "masked_ssn",
      "expression": "concat('XXX-XX-', substring(ssn, -4, 4))",
      "dataType": "string"
    },
    {
      "type": "inject",
      "name": "days_since_registration", 
      "expression": "datediff(current_date(), registration_date)",
      "dataType": "integer"
    }
  ]
}
```

### Lookup Transformations

Enrich data with reference lookups:

```json
{
  "transformations": [
    {
      "type": "lookup",
      "name": "country_name",
      "lookup_table": "dim_country",
      "lookup_key": "country_code",
      "source_key": "country_cd",
      "return_column": "country_name",
      "dataType": "string"
    }
  ]
}
```

## Performance Considerations

### Transformation Optimization

- **Order transformations** by complexity (simple first)
- **Minimize complex expressions** in hot paths
- **Use vectorized operations** when possible
- **Cache intermediate results** for complex calculations

### Memory Management

- **Stream processing** for large transformations
- **Partition-aware transformations** to avoid shuffles
- **Lazy evaluation** of transformation chains

### Monitoring

Track transformation performance:
- Processing time per transformation
- Memory usage during transformation
- Data volume changes after transformation

## Error Handling

### Transformation Failures

Handle transformation errors gracefully:

```json
{
  "transformations": [
    {
      "type": "inject",
      "name": "calculated_field",
      "expression": "complex_calculation(field1, field2)",
      "error_handling": {
        "strategy": "default_value",
        "default": 0,
        "log_errors": true
      }
    }
  ]
}
```

### Data Quality Issues

Configure responses to data quality problems:

```json
{
  "data_quality": {
    "duplicate_handling": "error",  // error, warn, ignore
    "null_business_key": "error",
    "validation_failures": "warn"
  }
}
```

## Best Practices

### Design Guidelines

1. **Keep transformations simple** - Complex logic belongs in upstream processes
2. **Document custom expressions** - Use comments in metadata
3. **Test transformations thoroughly** - Validate with representative data
4. **Monitor performance impact** - Track processing time changes

### Maintenance

1. **Version control transformations** - Track changes to transformation logic
2. **Regular validation** - Verify transformation results periodically
3. **Performance monitoring** - Watch for degradation over time
4. **Documentation updates** - Keep transformation documentation current

## Related Topics

- [Processing Strategies](Processing-Strategies.md) - How transformations work with different strategies
- [Metadata Management](Metadata-Management.md) - Configuring transformations in metadata
- [Configuration Reference](Configuration-Reference.md) - Complete transformation configuration options