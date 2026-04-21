# Known Issues Reference

This skill provides a searchable catalog of confirmed issues encountered during serverless migration. When Genie Code encounters an error during migration or testing, search this catalog by error message pattern to find the proven resolution.

## How to Use

When you encounter an error:
1. Search this file for the error class name (e.g., CAST_INVALID_INPUT, CONFIG_NOT_AVAILABLE)
2. Find the matching issue
3. Apply the proven resolution

## Issues by Error Pattern

### ANSI / Type Cast Errors

| Error Pattern | Cause | Resolution |
|---|---|---|
| `CAST_INVALID_INPUT` "cannot be cast to DATE/INT/TIMESTAMP" | Dirty data (empty strings, invalid dates like '00000000', '2299-12-34', non-numeric strings like 'ENG') | Use `TRY_CAST` instead of `CAST` |
| `CANNOT_PARSE_TIMESTAMP` "could not be parsed" | Invalid timestamp strings ('00000000', '99999999') | Use `try_to_timestamp` instead of `to_timestamp` |
| `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` "BOOLEAN and INT" | Code compares boolean column to integer (e.g., `is_active = 1`) | Change to `is_active IS TRUE` |
| `UNRESOLVED_COLUMN` with `yyyyMMdd` | `F.lit()` wrapping a format string in `to_date`/`to_timestamp` | Remove `F.lit()` wrapper. Use plain string: `F.to_date(col, "yyyyMMdd")` not `F.to_date(col, F.lit("yyyyMMdd"))` |
| `ARITHMETIC_OVERFLOW` | Integer multiplication/addition exceeds type max, or SUM on INT column | Cast to BIGINT before operation: `CAST(col AS BIGINT) * other_col` |
| `DIVIDE_BY_ZERO` | Division where denominator is zero | Use `TRY_DIVIDE(a, b)` or `CASE WHEN b = 0 THEN NULL ELSE a/b END` |

### Config Not Available Errors

| Error Pattern | Config | Resolution |
|---|---|---|
| `CONFIG_NOT_AVAILABLE` `retentionDurationCheck.enabled` | `spark.databricks.delta.retentionDurationCheck.enabled` | Use table property: `ALTER TABLE SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '30 days')` |
| `CONFIG_NOT_AVAILABLE` `schema.autoMerge.enabled` | `spark.databricks.delta.schema.autoMerge.enabled` | Use `MERGE WITH SCHEMA EVOLUTION` SQL syntax |
| `CONFIG_NOT_AVAILABLE` `broadcastTimeout` | `spark.sql.broadcastTimeout` | Remove. Not supported on serverless. |
| `CONFIG_NOT_AVAILABLE` `optimizeWrite.enabled` | `spark.databricks.delta.optimizeWrite.enabled` | Remove. Default ON in serverless. |
| `CONFIG_NOT_AVAILABLE` `autoCompact.enabled` | `spark.databricks.delta.autoCompact.enabled` | Remove. Default ON for managed tables. |
| `CONFIG_NOT_AVAILABLE` `caseSensitive` | `spark.sql.caseSensitive` | Remove. Not supported. Rewrite code to not depend on case sensitivity. |
| `CONFIG_NOT_AVAILABLE` `stateSchemaCheck` | `spark.sql.streaming.stateStore.stateSchemaCheck` | Remove if not doing stateful streaming. |
| `CANNOT_MODIFY_CONFIG` `network.timeout` | `spark.network.timeout` | Remove. Use `spark.databricks.execution.timeout` if timeout needed. |

### Unsupported Operations

| Error Pattern | Operation | Resolution |
|---|---|---|
| `NOT_SUPPORTED_WITH_SERVERLESS` `REFRESH TABLE` | REFRESH TABLE | Remove. Serverless auto-refreshes. |
| `Operation not allowed` `MSCK REPAIR TABLE` on Delta | MSCK REPAIR TABLE | Remove. Not needed for Delta tables. |
| `MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED` | CREATE MATERIALIZED VIEW | Must use SQL Warehouse, not serverless general compute. |
| `.persist()` not supported | .persist()/.cache() | Remove. Serverless auto-manages memory. |
| `DATA_SOURCE_NOT_FOUND` `com.crealytics.spark.excel` | spark-excel JAR library | Replace with pandas + openpyxl. Read via `pd.read_excel()`, write via local_disk0/tmp then copy to Volume. |

### Schema and Inference Errors

| Error Pattern | Cause | Resolution |
|---|---|---|
| `CANNOT_INFER_TYPE_FOR_FIELD` | `spark.createDataFrame()` on complex/nested JSON without explicit schema | Provide explicit `StructType` schema. |
| `_metadata` column cannot be resolved | Table has row tracking enabled, creating internal `_metadata` column | Disable row tracking on the table. |

### Performance Issues

| Symptom | Cause | Resolution |
|---|---|---|
| Job runtime 2-5x longer on serverless | External tables with poor file layout (many small files) | Run `OPTIMIZE` and `ANALYZE TABLE COMPUTE STATISTICS` on external tables before migration. |
| `RecursionError: maximum recursion depth` | 50+ chained `.withColumn()` calls | Replace with single `.withColumns()` call. |
| `Fatal error: Python kernel unresponsive` (OOM) | Large self-joins, excessive `.withColumn()` chains | Use `.withColumns()`, increase shuffle partitions, use stage tables. |
| `Photon ran out of memory` | Large JSON files with Photon simdjson | Set `spark.sql.files.maxPartitionBytes` to 64MB or 32MB. |
| ThreadPoolExecutor slower on serverless | External threading conflicts with serverless parallelism | Use Databricks workflow for-each tasks instead. |
| Cost increase after migration | Pure SQL jobs on serverless general compute | Move to DBSQL Serverless SQL Warehouse for SQL-only workloads. |

### File and Path Errors

| Error Pattern | Cause | Resolution |
|---|---|---|
| `PermissionError` `/tmp/` | Serverless has different filesystem permissions | Use `/local_disk0/tmp/` instead of `/tmp/` |
| Library installation failed | Wheel file not compatible with Python 3.12 | Rebuild wheel on DBR 16.4. Ensure filename contains `cp312`. |
| CSV null values have double quotes | Serverless handles CSV nulls differently | Add `.option("nullValue","").option("quote","")` |

### DevOps Issues

| Issue | Cause | Resolution |
|---|---|---|
| Job ID changes on deployment | Using jobs/create instead of jobs/reset | Use `jobs/reset` endpoint for existing jobs to preserve job ID. |
| `%env_name%` not resolved in job JSON | Missing `.Replace()` in PowerShell deployment script | Add `$bodyJson.Replace("%env_name%","$env_name")` to the script. |
| Egress blocked to external endpoints | Serverless egress control | Add required domain URLs to egress control allowlist. |
