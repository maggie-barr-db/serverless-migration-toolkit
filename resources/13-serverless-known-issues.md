# Serverless Migration Known Issues Catalog

> **Source:** Serverless migration issue log
> **Purpose:** Searchable reference of every confirmed issue encountered during migration from classic compute to serverless, indexed by error message pattern with proven resolutions
> **Last updated:** 2026-04-16

---

## Summary by Category

| Category | Issues | Issue Numbers |
|---|---|---|
| ANSI / Type Cast Issues | 7 | 1-7 |
| Config Not Available | 8 | 8-15 |
| Unsupported Operations | 5 | 16-20 |
| Library and Dependency Issues | 5 | 21-25 |
| Schema Inference Issues | 2 | 26-27 |
| _metadata Column Conflict | 1 | 28 |
| Performance Issues | 11 | 29-39 |
| Networking and Access Issues | 3 | 40-42 |
| Row Filter / Execution Plan Issues | 2 | 43-44 |
| File Path Issues | 1 | 45 |
| Materialized View Conflicts | 1 | 46 |
| Spark Logging | 1 | 47 |
| DevOps Issues | 1 | 48 |
| **Total** | **48** | |

---

## ANSI / Type Cast Issues

These issues arise because serverless compute enables ANSI mode by default, which enforces strict type checking and rejects invalid type conversions that classic compute silently handled.

---

### Issue 1: CAST to timestamp fails on empty/invalid strings

- **Environment:** UAT
- **Job:** ETL pipeline job (job `545285009490448`)
- **Error message:**
  ```
  CAST(' ' AS TIMESTAMP)
  ```
  Fails under ANSI mode.
- **Root cause:** ANSI mode enabled on serverless rejects invalid type conversions. Empty strings and whitespace-only strings cannot be cast to TIMESTAMP.
- **Resolution:** Use `try_cast` for proper datatype conversion. Do NOT use `spark.sql.ansi.enabled = false` as a permanent fix -- it is unreliable for INSERT operations. Also remove unsupported SQL commands like `spark.sql('MSCK REPAIR TABLE <table_name>')`.
- **Prevention:** Search codebase for `CAST(` patterns and validate that all source values are valid for the target type. Flag any CAST on string columns that may contain empty strings, whitespace, or placeholder values.

---

### Issue 2: BOOLEAN compared to INT

- **Environment:** UAT
- **Job:** Data lake L2 processing job (job `972306814207736`)
- **Error message:**
  ```
  [DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "(Claim_IsFinal = 1)" due to data type mismatch: the left and right operands of the binary operator have incompatible types ("BOOLEAN" and "INT"). SQLSTATE: 42K09
  ```
  Also:
  ```
  [NOT_SUPPORTED_WITH_SERVERLESS] REFRESH TABLE is not supported on serverless compute. SQLSTATE: 0A000
  ```
- **Root cause:** Classic compute implicitly converts between BOOLEAN and INT. ANSI mode on serverless enforces strict type matching.
- **Resolution:** Update code to use `Claim_IsFinal IS TRUE` instead of `Claim_IsFinal = 1`. Remove all `REFRESH TABLE` statements.
- **Prevention:** Search for patterns like `boolean_column = 1` or `boolean_column = 0` across all SQL and PySpark code. Grep for `REFRESH TABLE`.

---

### Issue 3: Invalid timestamp string '00000000'

- **Environment:** UAT
- **Job:** Data lake L1 ingestion job (job `493435840076236`)
- **Error message:**
  ```
  [CANNOT_PARSE_TIMESTAMP] Text '00000000' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 0. Use try_to_timestamp to tolerate invalid input string and return NULL instead. SQLSTATE: 22007
  ```
- **Root cause:** Source data contains placeholder date strings like `'00000000'` that are not valid dates. Classic compute returned NULL; ANSI mode throws an error.
- **Resolution:** Use `try_to_timestamp` instead of `to_timestamp`.
- **Prevention:** Identify all `to_timestamp` and `to_date` calls. Check whether source columns contain placeholder or sentinel values like `'00000000'`, `'99999999'`, `'19000101'`, etc.

---

### Issue 4: Another BOOLEAN = 1 pattern

- **Environment:** UAT
- **Job:** Reporting workflow job (job `278153250151405`)
- **Error message:**
  ```
  [DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "(claim_ismemberenrolledondateofservice = 1)" due to data type mismatch: the left and right operands of the binary operator have incompatible types ("BOOLEAN" and "INT"). SQLSTATE: 42K09
  ```
- **Root cause:** Same as Issue 2 -- BOOLEAN compared to INT literal.
- **Resolution:** Update to `claim_ismemberenrolledondateofservice IS TRUE`.
- **Prevention:** Same as Issue 2. Search for all `= 1` and `= 0` comparisons against boolean-typed columns.

---

### Issue 5: Invalid date string '2299-12-34'

- **Environment:** PROD
- **Job:** Claims update workflow job
- **Error message:**
  ```
  [CAST_INVALID_INPUT] The value '2299-12-34' of the type "STRING" cannot be cast to "DATE" because it is malformed. SQLSTATE: 22018
  ```
- **Root cause:** Source data contains impossible date values (day 34 does not exist). Classic compute returned NULL; ANSI mode raises an error.
- **Resolution:** Fix source data or use `try_cast`.
- **Prevention:** Profile string columns that are cast to DATE/TIMESTAMP. Look for sentinel values, out-of-range dates, and impossible calendar values.

---

### Issue 6: String 'ENG' cast to INT in join

- **Environment:** PROD
- **Job:** Daily enrollments refresh job (job `892809447365869`)
- **Error message:**
  ```
  [CAST_INVALID_INPUT] The value 'ENG' of the type "STRING" cannot be cast to "INT" because it is malformed. SQLSTATE: 22018
  ```
- **Root cause:** A join condition implicitly casts a STRING column to INT. The column contains non-numeric values like `'ENG'`. Classic compute silently failed; ANSI mode throws an error.
- **Resolution:** Use `TRY_CAST` in join conditions.
- **Prevention:** Review all join conditions where column types differ between the two sides. Look for STRING-to-INT implicit casts, especially on columns that could contain alphabetic codes.

---

### Issue 7: f.lit() wrapping format string in to_date

- **Environment:** PROD
- **Job:** Near real-time data extract job (job `391992294743764`)
- **Error message:**
  ```
  [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name 'yyyyMMdd HH:mm:ss' cannot be resolved
  ```
- **Root cause:** Code used `f.to_date(col, f.lit("yyyyMMdd"))` -- the `f.lit()` wrapper is wrong. Format strings should be plain Python strings, not Column expressions. Classic compute may have tolerated this; serverless does not.
- **Resolution:** Change `f.to_date(col, f.lit("yyyyMMdd"))` to `f.to_date(col, "yyyyMMdd")`. Change `f.try_to_timestamp(col, f.lit('yyyyMMdd HH:mm:ss'))` to `f.try_to_timestamp(col, 'yyyyMMdd HH:mm:ss')`.
- **Prevention:** Search for `f.lit(` used as the second argument to `to_date`, `to_timestamp`, `try_to_timestamp`, `date_format`, and similar functions that take a format string parameter.

---

## Config Not Available

These issues occur because serverless compute does not support arbitrary Spark configuration overrides. Many Delta and Spark configs that were set at the cluster level or in notebook init sections are either not available, not needed, or must be replaced with table-level properties or SQL syntax.

---

### Issue 8: delta.retentionDurationCheck.enabled

- **Environment:** UAT
- **Job:** Storage vacuum job (job `815987068818680`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.databricks.delta.retentionDurationCheck.enabled is not available
  ```
- **Root cause:** This Spark config is not supported on serverless compute.
- **Resolution:** Use table property instead: `ALTER TABLE table_name SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '30 days')` or specify retention directly: `VACUUM table_name RETAIN num HOURS`.
- **Prevention:** Scan all notebooks and init scripts for `spark.databricks.delta.retentionDurationCheck`. Flag for replacement with table-level property.

---

### Issue 9: delta.schema.autoMerge.enabled

- **Environment:** UAT
- **Job:** Workflow metadata download job (job `649339587918483`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.databricks.delta.schema.automerge.enabled is not available
  ```
- **Root cause:** Session-level schema auto-merge config is not supported on serverless.
- **Resolution:** Use `MERGE WITH SCHEMA EVOLUTION` SQL syntax instead.
- **Prevention:** Search for `delta.schema.autoMerge` or `delta.schema.automerge` in all notebooks and config blocks.

---

### Issue 10: broadcastTimeout

- **Environment:** UAT
- **Job:** Claims extract workflow job (job `82530538514630`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.sql.broadcastTimeout is not available. SQLSTATE: 42K0I
  ```
- **Root cause:** Broadcast timeout is not configurable on serverless. Serverless manages broadcast behavior internally.
- **Resolution:** Remove the config. Serverless manages broadcast behavior automatically.
- **Prevention:** Search for `broadcastTimeout` in all notebooks and config blocks.

---

### Issue 11: stateStore.stateSchemaCheck

- **Environment:** UAT
- **Job:** Near real-time data extract job (job `107564644640354`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.sql.streaming.stateStore.stateSchemaCheck is not available
  ```
- **Root cause:** This streaming config is not available on serverless.
- **Resolution:** Not needed on this job (no stateful operation). Remove the config.
- **Prevention:** Search for `stateStore.stateSchemaCheck` in all notebooks. Verify whether the job actually uses stateful streaming before investigating alternatives.

---

### Issue 12: delta.optimizeWrite.enabled

- **Environment:** UAT
- **Job:** Near real-time data load job (job `805887616889507`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.databricks.delta.optimizeWrite.enabled is not available
  ```
- **Root cause:** Optimized writes are enabled by default on serverless. The session-level config is not needed and not available.
- **Resolution:** Remove the config. Use `OPTIMIZE` with `ZORDER` or Liquid Clustering for file layout optimization.
- **Prevention:** Search for `delta.optimizeWrite` in all notebooks and config blocks. Safe to remove -- serverless has this on by default.

---

### Issue 13: delta.autoCompact.enabled

- **Environment:** UAT
- **Job:** Near real-time data load job (job `805887616889507`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.databricks.delta.autoCompact.enabled
  ```
- **Root cause:** Auto-compaction is enabled by default for MERGE/UPDATE/DELETE on Unity Catalog managed tables running on serverless. Session-level config is not available.
- **Resolution:** Remove the config. Use `OPTIMIZE` and Liquid Clustering for explicit file layout management.
- **Prevention:** Search for `delta.autoCompact` in all notebooks and config blocks. Safe to remove.

---

### Issue 14: caseSensitive

- **Environment:** UAT
- **Job:** Near real-time incremental load job (job `400772929647624`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration spark.sql.caseSensitive is not available. SQLSTATE: 42K0I
  ```
- **Root cause:** Case sensitivity configuration is not supported on serverless.
- **Resolution:** Rewrite code to not depend on case sensitivity settings. Use explicit column name extraction from schema rather than relying on case-insensitive resolution.
- **Prevention:** Search for `spark.sql.caseSensitive` in all notebooks. Review downstream code that depends on case-insensitive column matching.

---

### Issue 15: Environment variables not available

- **Environment:** PROD
- **Job:** Predictive model job (job `341751071748079`)
- **Error message:**
  ```
  [CONFIG_NOT_AVAILABLE] Configuration yrmo_latest.date is not available. SQLSTATE: 42K0I
  ```
- **Root cause:** Environment variables set at the cluster level are not available on serverless compute. Code was using `spark.conf.get("yrmo_latest.date")` to read cluster-level environment variables.
- **Resolution:** Use widgets instead. In the parent notebook: `dbutils.widgets.text("yrmo_latest.date", yearmo_string)`. In the child notebook: `dbutils.widgets.get("yrmo_latest.date")`.
- **Prevention:** Search for `spark.conf.get(` and `spark.conf.set(` with custom (non-Spark, non-Delta) configuration keys. These are likely environment variable patterns that must be converted to widgets or task parameters.

---

## Unsupported Operations

These issues involve Spark or Databricks operations that are explicitly not supported on serverless compute and must be removed or replaced with serverless-compatible alternatives.

---

### Issue 16: .persist() not supported

- **Environment:** PROD (all jobs)
- **Error message:**
  ```
  .persist() method in PySpark not supported in the serverless computation
  ```
- **Root cause:** Serverless compute manages memory and caching automatically. Manual `.persist()` and `.cache()` are not supported.
- **Resolution:** Remove `.persist()`. Serverless auto-manages memory. If the DataFrame is needed multiple times and performance is critical, materialize to a temporary table instead.
- **Prevention:** Search for `.persist(` and `.cache(` across all PySpark notebooks.

---

### Issue 17: REFRESH TABLE not supported

- **Environment:** UAT
- **Job:** Dimension measure job (job `30205435652809`)
- **Error message:**
  ```
  REFRESH TABLE is not supported on serverless compute
  ```
- **Root cause:** `REFRESH TABLE` is not a valid command on serverless. Serverless automatically handles metadata cache for external tables.
- **Resolution:** Remove `REFRESH TABLE` statements. Serverless automatically updates cache for external tables.
- **Prevention:** Search for `REFRESH TABLE` across all SQL and notebook code.

---

### Issue 18: MSCK REPAIR TABLE not supported

- **Environment:** UAT
- **Job:** ETL pipeline job (job `545285009490448`)
- **Error message:** `MSCK REPAIR TABLE` removed as not supported on serverless.
- **Root cause:** Hive-style partition repair is not supported on serverless compute.
- **Resolution:** Remove the command. Use `ALTER TABLE ... ADD PARTITION` if explicit partition discovery is needed.
- **Prevention:** Search for `MSCK REPAIR TABLE` across all SQL and notebook code.

---

### Issue 19: CREATE MATERIALIZED VIEW

- **Environment:** UAT
- **Job:** Data quality analytics DDL job (job `834218714642152`)
- **Error message:**
  ```
  The materialized view operation CREATE is not allowed: Cannot CREATE the Materialized View from general compute, please use DBSQL Serverless (recommended) or Pro warehouse
  ```
- **Root cause:** Materialized views cannot be created or refreshed from serverless general compute. They require a SQL Warehouse.
- **Resolution:** Create and refresh materialized views via SQL Warehouse (Serverless SQL Warehouse recommended, Pro warehouse also works). Grant SPN (Service Principal) access to the SQL Warehouse.
- **Follow-up:** SPN needed `PERMISSION_DENIED` resolved by granting SQL Warehouse access.
- **Prevention:** Identify all `CREATE MATERIALIZED VIEW` and `REFRESH MATERIALIZED VIEW` statements. These jobs must run on a SQL Warehouse, not serverless general compute.

---

### Issue 20: ThreadPoolExecutor performance degradation

- **Environment:** UAT
- **Job:** Archival metadata load workflow job (job `860836477075797`)
- **Error message / symptom:**
  ```
  Classic: ~1hr
  Serverless with ThreadPoolExecutor: ~4hr
  Serverless without ThreadPoolExecutor: ~8hr
  ```
- **Root cause:** Serverless manages parallelism internally. External threading via `ThreadPoolExecutor` adds overhead and contention rather than improving performance.
- **Resolution:** For batch operations, use Databricks Workflows with for-each tasks instead of `ThreadPoolExecutor`. Note: performance may still be slower than classic for heavily parallelized workloads.
- **Prevention:** Search for `ThreadPoolExecutor`, `concurrent.futures`, and `multiprocessing` in notebooks. Flag for architecture review -- these patterns need workflow-level parallelism instead.

---

## Library and Dependency Issues

These issues arise because serverless compute does not support arbitrary JAR libraries, has a different Python version, and has restricted filesystem access.

---

### Issue 21: com.crealytics.spark.excel not available

- **Environment:** UAT
- **Job:** Daily outbound data export job (job `7139954452030`)
- **Error message:**
  ```
  [DATA_SOURCE_NOT_FOUND] Failed to find the data source: com.crealytics.spark.excel
  ```
- **Root cause:** JAR libraries (including `com.crealytics.spark.excel`) are not supported on serverless compute.
- **Resolution:** Use pandas + openpyxl instead. Pattern for writing Excel:
  1. Convert DataFrame to pandas
  2. Write to `/local_disk0/tmp/filename.xlsx`
  3. Copy to a Unity Catalog Volume path
  4. Copy to `abfss://` if needed via `dbutils.fs.cp`
- **Prevention:** Search for `.format("com.crealytics.spark.excel")` and any other JAR-based data source references. Identify all Excel read/write operations.

---

### Issue 22: Excel read from abfss path

- **Environment:** UAT/PROD (multiple jobs)
- **Error message:** Cannot read Excel from `abfss://` path directly.
- **Root cause:** Excel file handling requires local filesystem access that is not directly available from cloud storage paths on serverless.
- **Resolution for READING:**
  1. `spark.read.format("binaryFile").load("abfss://...").first().content`
  2. Write bytes to `/local_disk0/tmp/filename.xlsx`
  3. `pd.read_excel("/local_disk0/tmp/filename.xlsx")`
  4. `spark.createDataFrame(pandas_df)`
- **Resolution for WRITING:**
  1. Convert to pandas DataFrame
  2. Write to `/local_disk0/tmp/filename.xlsx`
  3. Copy to Unity Catalog Volume path
  4. Copy to `abfss://` via `dbutils.fs.cp`
- **Prevention:** Identify all Excel read/write operations. Any direct `abfss://` Excel access must be refactored to use the local disk staging pattern.

---

### Issue 23: Permission denied /tmp/ path

- **Environment:** PROD
- **Job:** Analytics projection job (job `483893966858899`)
- **Error message:**
  ```
  PermissionError: [Errno 13] Permission denied: '/tmp/temp.xlsx'
  ```
- **Root cause:** Serverless compute has different filesystem permissions than classic clusters. The `/tmp` directory is not writable.
- **Resolution:** Use `/local_disk0/tmp/` instead of `/tmp/`. Serverless allows writes to `/local_disk0/tmp/`.
- **Prevention:** Search for `'/tmp/'` and `"/tmp/"` in all notebooks. Replace with `/local_disk0/tmp/`.

---

### Issue 24: Incompatible wheel file

- **Environment:** UAT
- **Job:** Near real-time call data load job (job `158222698341163`)
- **Error message:** Library installation failed with incompatible wheel.
- **Root cause:** Wheel files built for older Python versions or different platforms are incompatible with serverless, which runs Python 3.12 on DBR 16.4.
- **Resolution:** Recreate wheel files using DBR 16.4. Ensure the wheel filename contains `cp312` (indicating Python 3.12 compatibility). Check version compatibility of all dependent packages before deploying.
- **Prevention:** Inventory all custom wheel files and third-party libraries. Verify Python version compatibility (`cp312`) and rebuild as needed before migration.

---

### Issue 25: CSV null value handling

- **Environment:** PROD
- **Job:** Input metadata load workflow job (job `278589292631939`)
- **Error message / symptom:** CSV write adding double quotes for null values.
- **Root cause:** Default CSV writer behavior on serverless handles null values differently, wrapping them in quotes.
- **Resolution:** Add `.option("nullValue","").option("quote","")` when writing CSV files.
- **Prevention:** Review all CSV write operations. Test output file format in UAT to verify null handling matches expected downstream format.

---

## Schema Inference Issues

These issues occur when serverless compute's stricter schema inference cannot automatically determine the type of complex or empty fields.

---

### Issue 26: CANNOT_INFER_TYPE_FOR_FIELD `details`

- **Environment:** UAT
- **Job:** Autoscale events retrieval job (job `730705190841128`)
- **Error message:**
  ```
  [CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field 'details'
  ```
- **Root cause:** Newer runtime schema inference handles nested/complex fields differently. Code was using `spark.createDataFrame()` on API response JSON without providing a schema.
- **Resolution:** Explicitly specify schema with `StructType` for fields that fail inference.
- **Prevention:** Search for `spark.createDataFrame()` calls that do not specify a schema, especially those processing API responses or JSON data with nested fields.

---

### Issue 27: CANNOT_INFER_TYPE_FOR_FIELD `status`

- **Environment:** PROD
- **Job:** Job `822204838502176`
- **Error message:**
  ```
  [CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field 'status'
  ```
- **Root cause:** `spark.createDataFrame(job_runs['runs'])` on Jobs API response works on interactive serverless but fails on job serverless due to different schema inference behavior.
- **Resolution:** Predefine the schema for the DataFrame using `StructType`.
- **Prevention:** Same as Issue 26. All `spark.createDataFrame()` calls on dynamic data (API responses, parsed JSON) should have explicit schemas.

---

## _metadata Column Conflict

---

### Issue 28: _metadata column resolution error

- **Environment:** PROD
- **Job:** Data load workflow job (job `234987797088729`)
- **Error message:**
  ```
  A column, variable, or function parameter with name '_metadata' cannot be resolved. SQLSTATE: 42703
  ```
- **Root cause:** Tables were generated with row tracking enabled, which creates an internal `_metadata` column that conflicts with user code referencing `_metadata`.
- **Resolution:** Disable row tracking on the affected tables.
- **Prevention:** Check whether target tables have row tracking enabled. If code references `_metadata` (e.g., for file-level metadata from `spark.read`), verify there is no conflict with row tracking's internal `_metadata` column.

---

## Performance Issues

These issues cover runtime regressions, memory errors, and cost changes observed after migrating to serverless compute. Many stem from patterns that worked on classic clusters but are anti-patterns on serverless.

---

### Issue 29: VACUUM performance -- sequential vs multiprocessing

- **Environment:** PROD
- **Job:** Storage vacuum job (job `192924104096691`)
- **Error message / symptom:**
  ```
  Classic: 2.11 hours
  Serverless with multiprocessing: 3.10 hours
  Serverless sequential: 4.55+ hours (cancelled)
  ```
- **Root cause:** VACUUM operations are slower on serverless, especially when using Python multiprocessing for parallelism.
- **Resolution:** Avoid Python multiprocessing for VACUUM on serverless. Run VACUUM operations sequentially or use Databricks Workflows to parallelize across separate tasks. VACUUM performance on serverless is generally acceptable for individual tables when run without external threading.
- **Note:** VACUUM LITE (Public Preview) was tested and reduced execution time by 50%, but is not GA. Use standard VACUUM until VACUUM LITE reaches GA.
- **Prevention:** Identify all VACUUM jobs during assessment. Evaluate if multiprocessing can be replaced with workflow-level parallelism.

---

### Issue 30: VACUUM LITE error on tables without prior VACUUM FULL

- **Environment:** PROD
- **Job:** Storage vacuum job (job `192924104096691`)
- **Error message:**
  ```
  [DELTA_CANNOT_VACUUM_LITE] VACUUM LITE cannot delete all eligible files as some files are not referenced by the Delta log. Please run VACUUM FULL.
  ```
- **Root cause:** `VACUUM LITE` (Public Preview) depends on the Delta transaction log being complete. Tables that have never had `VACUUM FULL` may have unreferenced files not tracked in the log.
- **Resolution:** This error only occurs with VACUUM LITE (Public Preview). Use standard `VACUUM` instead until VACUUM LITE reaches GA.
- **Prevention:** Do not use VACUUM LITE in production until it is GA.

---

### Issue 31: Job runtime 5-10 min to 3 hours (external tables)

- **Environment:** PROD
- **Job:** Population analytics job (job `342286338536406`)
- **Root cause:** External tables have no Predictive Optimization and poor file layout. Serverless does not benefit from cached data on long-running clusters.
- **Resolution:** Run `OPTIMIZE` with `ZORDER` on the tables. Run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`. External tables need explicit maintenance since Predictive Optimization does not apply.
- **Prevention:** Identify all external tables used by migrating jobs. Schedule `OPTIMIZE` and `ANALYZE` before migration. Consider converting to managed tables where possible.

---

### Issue 32: Job runtime 40min to 3.5 hours (count/collect anti-patterns)

- **Environment:** PROD
- **Job:** Base ETL pipeline job (job `918560612590945`)
- **Root cause:** Unnecessary `.count()` actions, `PartitionBy` in writes, and poor table layout. These patterns are expensive on serverless where there is no persistent cluster cache.
- **Resolution:**
  1. Comment out unnecessary `count()` calls (saved 30-40 min)
  2. Remove `PartitionBy` from write operations
  3. Add Liquid Clustering
  4. Use `.first() is not None` instead of `.count() > 0`
  5. Run `OPTIMIZE` + `ANALYZE`
  - Final result: serverless 51min vs expected 1hr.
- **Prevention:** Search for `.count()`, `.collect()`, and `PartitionBy` in PySpark code. Flag `.count()` used only for empty-check (replace with `.first() is not None`). Flag `PartitionBy` for removal on serverless.

---

### Issue 33: Job runtime 2h42m to 5h44m (enrichment job)

- **Environment:** PROD
- **Job:** Data enrichment pipeline job (job `801731365618383`)
- **Root cause:** `PartitionBy` in child notebook writes, no clustering on tables, no statistics on join tables.
- **Resolution:**
  1. Remove `PartitionBy`
  2. Use Liquid Clustering
  3. Run `OPTIMIZE` + `ANALYZE` on all join tables
  4. Use broadcast hints for small tables (e.g., `/*+ BROADCAST(small_table) */`)
- **Prevention:** Review child notebooks for `PartitionBy`. Identify all join tables and verify they have current statistics. Flag small dimension tables for broadcast hints.

---

### Issue 34: RecursionError maximum recursion depth

- **Environment:** PROD
- **Job:** Data quality accuracy job (job `685602536286187`)
- **Error message:**
  ```
  RecursionError: maximum recursion depth exceeded
  ```
- **Root cause:** Multiple `.withColumn()` operations chained together cause deep recursion in Spark's query planner. This is more likely to hit limits on serverless due to different default recursion depths.
- **Resolution:** Replace chained `.withColumn()` calls with a single `.withColumns()` call that applies all column transformations at once.
- **Prevention:** Search for sequences of `.withColumn(` in PySpark code. Flag notebooks with more than ~20 chained `.withColumn()` calls for refactoring to `.withColumns()`.

---

### Issue 35: Python kernel unresponsive (OOM)

- **Environment:** PROD
- **Job:** L1 ETL pipeline jobs (job `502536177300535`)
- **Error message:**
  ```
  Fatal error: The Python kernel is unresponsive. The Python process exited with exit code 137 (SIGKILL: Killed). This may have been caused by an OOM error.
  ```
- **Root cause:** Memory-intensive operations combined with inefficient patterns (chained `withColumn`, self-joins, insufficient partitioning) exhausted available memory.
- **Resolution:**
  1. Replace `withColumn` chains with `withColumns`
  2. Set `shuffle.partitions` to 400
  3. Use stage tables before inserting into final tables
  4. Replace self-joins with window functions
- **Prevention:** Profile memory-intensive jobs on classic. Identify self-joins, large `withColumn` chains, and jobs that process very large datasets. Plan for partition tuning.

---

### Issue 36: Photon out of memory

- **Environment:** UAT
- **Job:** Near real-time call data load job (job `400772929647624`)
- **Error message:**
  ```
  SparkException: Photon ran out of memory... Photon failed to reserve 768.0 MiB for simdjson internal usage
  ```
- **Root cause:** Large partition sizes cause Photon to run out of memory during JSON/CSV parsing.
- **Resolution:** Set `spark.conf.set("spark.sql.files.maxPartitionBytes", "67108864")` (64MB) to create smaller, more manageable partitions. Try 32MB (`"33554432"`) if 64MB is not sufficient.
- **Prevention:** Identify jobs that process large files (JSON, CSV, Parquet). Check current partition sizes and flag any that may exceed serverless memory limits.

---

### Issue 37: Cost increase -- false alarm

- **Environment:** PROD
- **Job:** Data quality accuracy job (job `704798476678852`)
- **Symptom:** Cost per run appeared to increase 55%.
- **Root cause:** Uptick was temporary due to increased data processing volume. Monthly average was comparable to pre-migration levels.
- **Resolution:** No action required. Analyze system tables over a longer period (at least one full month) before concluding there is a cost increase.
- **Prevention:** Establish baseline cost metrics before migration. Compare monthly averages, not individual run costs. Account for data volume fluctuations.

---

### Issue 38: SQL job cost higher on serverless general compute

- **Environment:** PROD
- **Job:** Reporting SQL workflow job (job `679841779691820`)
- **Root cause:** Pure SQL jobs running on serverless general compute are more expensive than on serverless SQL Warehouse.
- **Resolution:** Run pure SQL jobs on Serverless SQL Warehouse instead of Serverless General Compute. SQL Warehouse is optimized for SQL workloads and more cost-effective.
- **Prevention:** During assessment, identify pure SQL jobs (no PySpark, no Python). Route these to SQL Warehouse instead of serverless general compute.

---

### Issue 39: Parallelized tasks cost increase

- **Environment:** PROD
- **Job:** Table audit job set (multiple job IDs)
- **Symptom:**
  ```
  Runtime: 2hr on classic -> 7hr on serverless
  ```
  Migrating parallelized tasks caused significant cost and runtime increase.
- **Root cause:** Jobs that rely on cluster-level parallelism with multiple concurrent tasks do not perform the same way on serverless.
- **Resolution:** Partially migrated -- shorter-running tasks moved to serverless, kept longer parallel tasks on classic. Not everything benefits from serverless.
- **Prevention:** Identify heavily parallelized job sets during assessment. Benchmark representative tasks on serverless before committing to full migration. Some workloads are better left on classic compute.

---

## Networking and Access Issues

---

### Issue 40: Egress control blocking external endpoints

- **Environment:** PROD
- **Job:** External data parsing job (job `92985562923573`)
- **Error message:**
  ```
  HTTPSConnectionPool... Failed to establish a new connection: [Errno -3] Temporary failure in name resolution
  ```
- **Root cause:** Serverless Egress Control blocks outbound network connections to external domains by default.
- **Resolution:** Add required external domain URLs to the Serverless Egress Control allowlist. Coordinate with the platform engineering team to approve and configure the allowlist.
- **Prevention:** Inventory all external network calls in notebooks (API calls, web scraping, external database connections, package downloads). Submit allowlist requests before migration.

---

### Issue 41: Serverless not enabled in prod workspace

- **Environment:** PROD
- **Job:** Provider portal data job (job `192796312195994`)
- **Error message:** Deployment failed in prod workspace.
- **Root cause:** Serverless compute was not enabled in the production workspace.
- **Resolution:** Enable serverless compute in the workspace before deploying jobs.
- **Prevention:** Verify serverless is enabled in all target workspaces (DEV, UAT, PROD) before beginning migration. This is a workspace-level admin setting.

---

### Issue 42: Session terminated during long VACUUM

- **Environment:** PROD
- **Job:** Storage vacuum job (job `192924104096691`)
- **Error message:**
  ```
  BAD_REQUEST: session_id is no longer usable... reason=UNDERLYING_CLUSTER_TERMINATED
  ```
- **Root cause:** Serverless environment version 1 had session stability issues for long-running operations.
- **Resolution:** Upgrade to environment version 4. Implement retry logic for intermittent session failures.
- **Prevention:** Ensure workspace is running the latest serverless environment version. For long-running operations (multi-hour VACUUM, large ETL), test session stability in UAT first.

---

## Row Filter / Execution Plan Issues

These issues involve differences in how serverless compute constructs logical execution plans compared to classic compute, particularly when row filters or `SELECT *` are involved.

---

### Issue 43: Row filter causing MISSING_ATTRIBUTES error

- **Environment:** UAT
- **Jobs:** Reconciliation L2 job (job `908283765956390`) + reporting reconciliation workflow job (job `45614751548631`)
- **Error message:**
  ```
  [MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION] Resolved attribute(s) "part_state", "cim"... missing from...
  ```
- **Root cause:** Tables with row filters enabled cause column resolution failures on serverless due to different logical execution plan construction. The row filter's required columns are not propagated correctly through the plan.
- **Resolution:** Use specific column names instead of `SELECT *`. Use proper table aliases in all queries. Note: Dropping the row filter also resolves the issue but is not a permanent solution if the filter is required for data governance.
- **Prevention:** Identify all tables with row filters or column masks. Test queries against these tables on serverless in UAT. Rewrite `SELECT *` to explicit column lists.

---

### Issue 44: SELECT * with different execution plan

- **Environment:** PROD
- **Job:** Transfer log details job (job `588637698264566`)
- **Error message:** Insert statement erroring with column resolution issues.
- **Root cause:** Serverless creates logical execution plans differently than classic compute. `SELECT *` can produce different column ordering or resolution behavior.
- **Resolution:** List all columns explicitly in SQL statements instead of using `SELECT *`.
- **Prevention:** Search for `SELECT *` in all SQL code. Replace with explicit column lists, especially in `INSERT INTO ... SELECT *` patterns.

---

## File Path Issues

---

### Issue 45: /tmp vs /local_disk0/tmp permissions

- **Environment:** UAT
- **Job:** Outbound data workflow job (job `536134128241835`)
- **Error message:** Permission denied when writing to `/tmp`.
- **Root cause:** Serverless compute does not allow writes to `/tmp`. The writable local path is `/local_disk0/tmp`.
- **Resolution:** Use `/local_disk0/tmp` for all local file operations. Then copy to a Unity Catalog Volume path, then to `abfss://` if needed.
- **Prevention:** Search for `/tmp/` references in all notebooks. Replace with `/local_disk0/tmp/`. See also Issue 23.

---

## Materialized View Conflicts

---

### Issue 46: VACUUM conflicts with materialized view refresh

- **Environment:** PROD
- **Job:** Measure crosswalk load job (job `1063132271896440`)
- **Root cause:** Automatic VACUUM (via Predictive Optimization) on managed source tables removes data files that a concurrently running materialized view refresh is still reading.
- **Resolution:** Use scheduled refreshes for materialized views instead of manual/on-demand refresh. Alternatively, stagger VACUUM and materialized view refresh schedules so they do not overlap.
- **Prevention:** Identify all materialized views and their source tables. Check whether Predictive Optimization is enabled on source tables. Schedule refreshes to avoid overlap with VACUUM windows.

---

## Spark Logging

---

### Issue 47: Spark logs not available on serverless

- **Environment:** PROD (all jobs)
- **Symptom:** log4j and stdout logs are not available on serverless compute.
- **Root cause:** By design. Serverless does not expose traditional Spark driver/executor logs.
- **Resolution:** Use Query History and Query Profile for troubleshooting instead of log4j output. Reference: https://learn.microsoft.com/en-us/azure/databricks/sql/user/queries/query-profile
- **Prevention:** Identify jobs that depend on log parsing for monitoring or alerting. Migrate logging-dependent workflows to use Query History APIs or system tables for observability.

---

## DevOps Issues

---

### Issue 48: Job ID not retained on JSON update

- **Environment:** UAT
- **Job:** Reporting enrollment workflow job (job `573685615905816`)
- **Problem:** Job ID changes when updating workflow JSON through DevOps pipeline, breaking Autosys references and external scheduling dependencies.
- **Root cause:** The DevOps pipeline was using the Jobs API `create` endpoint, which creates a new job with a new ID, instead of updating the existing job in place.
- **Resolution:** Use the Jobs API `reset` endpoint (which updates the existing job and preserves the job ID) instead of the `create` endpoint. Review and update the DevOps build and release package.
- **Prevention:** Review CI/CD pipelines before migration. Verify that job update workflows use the `reset` API endpoint. Identify all external systems (Autosys, schedulers, monitoring) that reference job IDs.

---

## Quick Reference: Error Pattern to Issue Mapping

Use this table to quickly find the relevant issue when you encounter an error message.

| Error Pattern | Issue | Category |
|---|---|---|
| `CAST_INVALID_INPUT` | 1, 5, 6 | ANSI / Type Cast |
| `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` | 2, 4 | ANSI / Type Cast |
| `CANNOT_PARSE_TIMESTAMP` | 3 | ANSI / Type Cast |
| `UNRESOLVED_COLUMN.WITH_SUGGESTION` | 7 | ANSI / Type Cast |
| `CONFIG_NOT_AVAILABLE` | 8-15 | Config Not Available |
| `.persist()` not supported | 16 | Unsupported Operations |
| `REFRESH TABLE is not supported` | 17 | Unsupported Operations |
| `MSCK REPAIR TABLE` | 18 | Unsupported Operations |
| `Cannot CREATE the Materialized View from general compute` | 19 | Unsupported Operations |
| `ThreadPoolExecutor` performance degradation | 20 | Unsupported Operations |
| `DATA_SOURCE_NOT_FOUND` (spark.excel) | 21 | Library / Dependency |
| Excel read from `abfss://` | 22 | Library / Dependency |
| `PermissionError: Permission denied: '/tmp/'` | 23, 45 | Library / File Path |
| Incompatible wheel / `cp312` | 24 | Library / Dependency |
| CSV null double-quoting | 25 | Library / Dependency |
| `CANNOT_INFER_TYPE_FOR_FIELD` | 26, 27 | Schema Inference |
| `_metadata` cannot be resolved | 28 | _metadata Conflict |
| `DELTA_CANNOT_VACUUM_LITE` | 30 | Performance |
| `RecursionError: maximum recursion depth` | 34 | Performance |
| `exit code 137 (SIGKILL)` / OOM | 35 | Performance |
| `Photon ran out of memory` | 36 | Performance |
| `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION` | 43 | Row Filter / Execution Plan |
| `Failed to establish a new connection` / egress | 40 | Networking |
| `session_id is no longer usable` | 42 | Networking |
| Job ID changes on deploy | 48 | DevOps |

---

## Assessment Checklist: Patterns to Search For

Before migrating any job to serverless, scan the codebase for these patterns:

**ANSI / Type Cast (Issues 1-7):**
- [ ] `CAST(` with string-to-date/timestamp/numeric conversions
- [ ] `to_timestamp(`, `to_date(` with potentially invalid source data
- [ ] `boolean_column = 1` or `boolean_column = 0`
- [ ] `f.lit(` as format string argument in `to_date`/`to_timestamp`

**Config (Issues 8-15):**
- [ ] `spark.databricks.delta.retentionDurationCheck`
- [ ] `spark.databricks.delta.schema.autoMerge`
- [ ] `spark.sql.broadcastTimeout`
- [ ] `spark.sql.streaming.stateStore.stateSchemaCheck`
- [ ] `spark.databricks.delta.optimizeWrite`
- [ ] `spark.databricks.delta.autoCompact`
- [ ] `spark.sql.caseSensitive`
- [ ] Custom `spark.conf.set(` / `spark.conf.get(` with non-standard keys

**Unsupported Operations (Issues 16-20):**
- [ ] `.persist()` / `.cache()`
- [ ] `REFRESH TABLE`
- [ ] `MSCK REPAIR TABLE`
- [ ] `CREATE MATERIALIZED VIEW`
- [ ] `ThreadPoolExecutor` / `concurrent.futures`

**Libraries (Issues 21-25):**
- [ ] `com.crealytics.spark.excel` or other JAR data sources
- [ ] Excel read/write operations
- [ ] `/tmp/` file paths (must be `/local_disk0/tmp/`)
- [ ] Custom wheel files (need `cp312` rebuild)
- [ ] CSV write operations (null handling)

**Schema / Metadata (Issues 26-28):**
- [ ] `spark.createDataFrame()` without explicit schema
- [ ] References to `_metadata` column

**Performance (Issues 29-39):**
- [ ] `VACUUM` operations (avoid multiprocessing on serverless)
- [ ] External tables (need `OPTIMIZE` + `ANALYZE`)
- [ ] `.count()` used for empty checks
- [ ] `PartitionBy` in write operations
- [ ] Chained `.withColumn()` calls (20+)
- [ ] Self-joins (replace with window functions)
- [ ] Pure SQL jobs (route to SQL Warehouse)
- [ ] Heavily parallelized task sets

**Networking / Access (Issues 40-42):**
- [ ] External HTTP/HTTPS calls
- [ ] Serverless enabled in target workspace
- [ ] Long-running operations (session stability)

**Execution Plan (Issues 43-44):**
- [ ] Tables with row filters
- [ ] `SELECT *` in INSERT statements

**DevOps (Issue 48):**
- [ ] CI/CD pipeline using Jobs API `create` vs `reset`
