# Log Level Configuration

The Data Lake Foundation library now supports configurable log levels through the metadata configuration file.

## Configuration

Add the `log_level` field to the `environment` section of your metadata JSON:

```json
{
  "environment": {
    "name": "PRODUCTION",
    "timezone": "UTC",
    "root_folder": "/data",
    "raw_path": "raw",
    "bronze_path": "bronze", 
    "silver_path": "silver",
    "secure_container_suffix": "",
    "log_level": "INFO"
  },
  ...
}
```

## Supported Log Levels

The following log levels are supported (in order of verbosity):

- `TRACE` - Most verbose, shows all log messages
- `DEBUG` - Detailed diagnostic information
- `INFO` - General information about application execution
- `WARN` - Warning messages for potentially harmful situations
- `ERROR` - Error messages for error conditions
- `FATAL` - Very severe error events that may cause termination
- `OFF` - No logging output

## Default Behavior

If `log_level` is not specified in the environment configuration, the library defaults to `WARN` level.

## Examples

### Production Environment
```json
{
  "environment": {
    "log_level": "INFO"
  }
}
```

### Development Environment
```json
{
  "environment": {
    "log_level": "DEBUG"
  }
}
```

### Quiet Environment
```json
{
  "environment": {
    "log_level": "WARN"
  }
}
```

This configuration applies to all loggers created through the Data Lake Foundation's `DatalakeLogManager`.
