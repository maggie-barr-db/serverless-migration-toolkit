# Path D: SQL-Only / PySpark-SQL to DBSQL Serverless Current Channel

This resource covers the migration of pure SQL workloads (or PySpark notebooks that exclusively use `spark.sql()` / `%sql` magic commands) from classic compute (DBR 13.3 LTS clusters) to Databricks SQL Serverless Current Channel SQL notebooks.

This document serves two purposes:
1. **Reference for Genie Code** — regex detection patterns and automated fix rules for code migration
2. **Edge case documentation** — for manual review of patterns that cannot be auto-fixed

---

## 1. Migration Overview

### What Changes

| Aspect | Before (Classic Compute) | After (DBSQL Serverless) |
|--------|--------------------------|--------------------------|
| Compute type | All-purpose or job cluster (DBR 13.3 LTS) | SQL Serverless Warehouse (Current Channel) |
| Notebook language | Python (with `spark.sql()`) or SQL | SQL only |
| ANSI mode | Configurable (`spark.sql.ansi.enabled = false` by default on 13.3) | **Always ON** — no option to disable |
| Spark configs | Set via `spark.conf.set()` or cluster config | Managed by warehouse — most not applicable |
| Caching | Manual `.cache()` / `CACHE TABLE` | Warehouse-managed result cache |
| Runtime | PySpark + Spark SQL engine | DBSQL SQL engine (Photon enabled by default) |
| Parameters | `dbutils.widgets`, Python variables, `%run` | SQL parameters (`:param` or `$param` syntax) |
| Display | `display(df)`, `df.show()`, `print()` | Direct `SELECT` — auto-displayed |

### Eligibility Criteria

A notebook qualifies for Path D (DBSQL Serverless) if **all** of the following are true:

| # | Criterion | Detection |
|---|-----------|-----------|
| 1 | All cells are SQL, `%sql` magic, or Python cells that **only** call `spark.sql()` | Scan for non-SQL operations in Python cells |
| 2 | No DataFrame operations (`.filter()`, `.select()`, `.withColumn()`, `.join()`, etc.) | Regex scan for DataFrame API calls |
| 3 | No Python UDFs — or they can be rewritten as SQL UDFs | Check for `@udf`, `F.udf(`, `spark.udf.register(` |
| 4 | No streaming operations | Check for `readStream`, `writeStream`, `trigger` |
| 5 | No ML operations | Check for `ml.`, `MLflow`, `sklearn` imports |
| 6 | No file system operations beyond Unity Catalog Volumes | Check for `dbfs:`, `/mnt/`, `dbutils.fs.` |
| 7 | No `dbutils` beyond `.widgets` and `.secrets` | Check for `dbutils.notebook.run`, `dbutils.fs.`, `dbutils.library` |
| 8 | No `%run` dependencies — or they can be restructured as workflow tasks | Check for `%run` or `dbutils.notebook.run()` |
| 9 | No Python logic beyond variable assignment for `spark.sql()` interpolation | Check for `if/else`, `for`, `while`, `def`, `class` outside of trivial patterns |

### When to Use This Path vs Serverless General Compute

| Scenario | Recommendation |
|----------|----------------|
| Notebook is 100% SQL | **Path D — DBSQL Serverless** |
| Notebook is PySpark but only `spark.sql()` calls and `display()` | **Path D — DBSQL Serverless** (extract SQL) |
| Notebook has simple Python UDFs that are just SQL logic in Python | **Path D — DBSQL Serverless** (convert UDFs to SQL UDFs first) |
| Notebook has DataFrame operations mixed with SQL | **Path C — Serverless General Compute** |
| Notebook has ML, streaming, or heavy Python logic | **Path C — Serverless General Compute** |
| Notebook uses Python for orchestration beyond simple SQL execution | **Path C — Serverless General Compute** |

---

## 2. Notebook Format Conversion

### PySpark `spark.sql()` to SQL Cells

Every `spark.sql("...")` call in a Python cell becomes a direct SQL statement in a SQL cell.

**Detect — Python cells calling spark.sql:**
```regex
spark\.sql\s*\(
```

**Before (Python cell):**
```python
result = spark.sql("""
  SELECT member_id, claim_date, paid_amount
  FROM claims_silver
  WHERE claim_date >= '2024-01-01'
""")
```

**After (SQL cell):**
```sql
SELECT member_id, claim_date, paid_amount
FROM claims_silver
WHERE claim_date >= '2024-01-01'
```

### Extracting SQL from spark.sql() Calls

**Simple single-line:**
```python
# Before:
spark.sql("SELECT COUNT(*) FROM claims_silver")

# After (SQL cell):
SELECT COUNT(*) FROM claims_silver
```

**Multi-line with triple quotes:**
```python
# Before:
spark.sql("""
  MERGE INTO target USING source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# After (SQL cell):
MERGE INTO target USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Detect — triple-quoted spark.sql:**
```regex
spark\.sql\s*\(\s*(?:f?"""[\s\S]*?"""|f?'''[\s\S]*?'''|f?"[^"]*"|f?'[^']*')
```

### Handling Multi-Statement Cells

A single Python cell may call `spark.sql()` multiple times. Each call becomes a separate SQL cell.

**Before (single Python cell):**
```python
spark.sql("USE CATALOG prod_catalog")
spark.sql("CREATE OR REPLACE TEMPORARY VIEW active_members AS SELECT * FROM members WHERE status = 'ACTIVE'")
spark.sql("SELECT COUNT(*) FROM active_members")
```

**After (three SQL cells):**

Cell 1:
```sql
USE CATALOG prod_catalog
```

Cell 2:
```sql
CREATE OR REPLACE TEMPORARY VIEW active_members AS
SELECT * FROM members WHERE status = 'ACTIVE'
```

Cell 3:
```sql
SELECT COUNT(*) FROM active_members
```

### Converting Python Variable Interpolation to SQL Parameters

**Detect — f-string or .format() in spark.sql:**
```regex
spark\.sql\s*\(\s*f["']{1,3}
```
```regex
spark\.sql\s*\([^)]*\.format\s*\(
```
```regex
spark\.sql\s*\([^)]*%\s*\(
```

**Before (Python f-string interpolation):**
```python
env = dbutils.widgets.get("env")
start_date = dbutils.widgets.get("start_date")

spark.sql(f"""
  SELECT *
  FROM {env}_catalog.claims_schema.claims_silver
  WHERE claim_date >= '{start_date}'
""")
```

**After (SQL with parameters):**
```sql
SELECT *
FROM IDENTIFIER(:env || '_catalog.claims_schema.claims_silver')
WHERE claim_date >= :start_date
```

Or using Molina's established pattern with `USE CATALOG`:
```sql
USE CATALOG IDENTIFIER(:env || '_catalog');

SELECT *
FROM claims_schema.claims_silver
WHERE claim_date >= :start_date
```

> **Molina-specific note:** Molina uses `USE CATALOG {{env}}_catalog` with 2-part table names. This is correct Unity Catalog usage. Do NOT flag 2-part namespaces as non-UC.

### Magic Command Conversion

**`%sql` cells → native SQL cells:**

No code change needed — the SQL content is already pure SQL. Only the notebook format changes (the `%sql` magic prefix is removed because the notebook is now a SQL notebook).

**Detect — %sql magic:**
```regex
^%sql\b
```

**Before (Python notebook with %sql magic):**
```
%sql
SELECT COUNT(*) FROM claims_silver WHERE paid_amount > 0
```

**After (SQL notebook cell):**
```sql
SELECT COUNT(*) FROM claims_silver WHERE paid_amount > 0
```

### Handling `%python` Cells that Call spark.sql()

Extract the SQL string. If the Python cell does nothing else, it converts cleanly.

**Detect — %python cell with only spark.sql:**
```regex
^%python\s*\n(?:\s*#[^\n]*\n)*\s*(?:(?:\w+)\s*=\s*)?spark\.sql\s*\(
```

**Before:**
```python
%python
# Get monthly aggregates
result = spark.sql("""
  SELECT DATE_TRUNC('month', claim_date) AS claim_month,
         SUM(paid_amount) AS total_paid
  FROM claims_silver
  GROUP BY 1
""")
display(result)
```

**After (SQL cell):**
```sql
SELECT DATE_TRUNC('month', claim_date) AS claim_month,
       SUM(paid_amount) AS total_paid
FROM claims_silver
GROUP BY 1
```

### Handling `%python` Cells that Do Anything Else

**NOT eligible for this path.** Route to Path C (Serverless General Compute).

**Detect — Python cells with non-SQL operations:**
```regex
^%python[\s\S]*?(?:\.filter\(|\.select\(|\.withColumn\(|\.join\(|\.groupBy\(|\.agg\(|\.write\.|\.read\.|import\s+|def\s+\w+|class\s+\w+|for\s+\w+\s+in|while\s+|if\s+.*:)
```

### display() Calls → Direct SELECT

In DBSQL SQL notebooks, every `SELECT` statement automatically displays its results. No `display()` wrapper needed.

**Detect:**
```regex
display\s*\(\s*spark\.sql\s*\(
```
```regex
display\s*\(\s*\w+\s*\)
```

**Before:**
```python
result = spark.sql("SELECT * FROM claims_silver LIMIT 100")
display(result)
```

**After:**
```sql
SELECT * FROM claims_silver LIMIT 100
```

### dbutils.widgets → SQL Parameters

**Detect:**
```regex
dbutils\.widgets\.(text|dropdown|combobox|multiselect|get)\s*\(
```

**Before (Python):**
```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.dropdown("run_mode", "incremental", ["full", "incremental"])

env = dbutils.widgets.get("env")
start_date = dbutils.widgets.get("start_date")
run_mode = dbutils.widgets.get("run_mode")
```

**After (SQL — using DECLARE or widget parameters):**

Option A — SQL notebook parameter widgets (set via UI or workflow parameters):
```sql
-- Parameters are passed via workflow task configuration or notebook UI widgets.
-- Reference them with :parameter_name syntax.
USE CATALOG IDENTIFIER(:env || '_catalog');

SELECT *
FROM claims_silver
WHERE claim_date >= :start_date
  AND (:run_mode = 'full' OR updated_date >= :start_date)
```

Option B — DECLARE for local variables:
```sql
DECLARE OR REPLACE env STRING DEFAULT 'dev';
DECLARE OR REPLACE start_date DATE DEFAULT '2024-01-01';

USE CATALOG IDENTIFIER(env || '_catalog');

SELECT *
FROM claims_silver
WHERE claim_date >= start_date
```

---

## 3. ANSI Mode (Always ON in DBSQL)

ANSI mode is **mandatory** in DBSQL — there is no `SET spark.sql.ansi.enabled = false` escape hatch. All SQL must be ANSI-compliant.

**Why this matters for healthcare data:** With ANSI mode off (legacy behavior), invalid casts, division by zero, and out-of-bounds access silently return `NULL`. This can mask data quality issues. With ANSI mode on, these operations throw errors — which is safer, but will break existing code that relies on silent null returns.

### 3.1 CAST to TRY_CAST

**Risk:** `CAST('abc' AS INT)` throws `NumberFormatException` in ANSI mode instead of returning `NULL`.

**Detect:**
```regex
\bCAST\s*\(\s*\S+\s+AS\s+(INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP)\s*\)
```

**Before:**
```sql
SELECT CAST(member_id AS INT) FROM eligibility_raw
```

**After:**
```sql
SELECT TRY_CAST(member_id AS INT) FROM eligibility_raw
```

**When NOT to change:** If you are 100% certain the column always contains valid values (e.g., casting a `BIGINT` to `INT` where values are always in range), `CAST` is fine and preserves the error-on-failure behavior as a safety net.

### 3.2 Division to TRY_DIVIDE

**Risk:** `x / 0` throws `ArithmeticException` in ANSI mode instead of returning `NULL`.

**Detect:**
```regex
\b(\w+)\s*/\s*(\w+)\b
```
```regex
\bSELECT\b[^;]*\b\w+\s*/\s*\w+
```

**Before:**
```sql
SELECT total_paid / member_count AS avg_paid
FROM claims_summary
```

**After (option 1 — TRY_DIVIDE):**
```sql
SELECT TRY_DIVIDE(total_paid, member_count) AS avg_paid
FROM claims_summary
```

**After (option 2 — CASE WHEN):**
```sql
SELECT CASE WHEN member_count = 0 THEN NULL
            ELSE total_paid / member_count
       END AS avg_paid
FROM claims_summary
```

### 3.3 Array Access to TRY_ELEMENT_AT

**Risk:** `array_col[5]` throws `ArrayIndexOutOfBoundsException` in ANSI mode when index is out of range.

**Detect:**
```regex
\b\w+\s*\[\s*\d+\s*\]
```

**Before:**
```sql
SELECT diagnosis_codes[0] AS primary_diagnosis
FROM claims_silver
```

**After:**
```sql
-- Note: TRY_ELEMENT_AT is 1-indexed, bracket notation is 0-indexed
SELECT TRY_ELEMENT_AT(diagnosis_codes, 1) AS primary_diagnosis
FROM claims_silver
```

> **Index shift warning:** `array[0]` (0-indexed) becomes `TRY_ELEMENT_AT(array, 1)` (1-indexed). Always add 1 to the index.

### 3.4 Map Access to TRY_ELEMENT_AT

**Risk:** `map_col['missing_key']` throws `NoSuchElementException` in ANSI mode when key does not exist.

**Detect:**
```regex
\b\w+\s*\[\s*'[^']*'\s*\]
```

**Before:**
```sql
SELECT metadata['source_system'] AS source
FROM claims_raw
```

**After:**
```sql
SELECT TRY_ELEMENT_AT(metadata, 'source_system') AS source
FROM claims_raw
```

### 3.5 Integer Overflow to BIGINT Pre-Cast

**Risk:** Integer arithmetic that exceeds `INT` range throws `ArithmeticException` in ANSI mode instead of wrapping around.

**Detect:**
```regex
\bSELECT\b[^;]*\b\w+\s*\*\s*\w+
```

**Before:**
```sql
SELECT claim_count * unit_cost AS total_cost
FROM claims_detail
-- If both are INT and product exceeds 2,147,483,647, ANSI mode throws
```

**After:**
```sql
SELECT CAST(claim_count AS BIGINT) * unit_cost AS total_cost
FROM claims_detail
```

### 3.6 Boolean Comparison — IS TRUE / IS FALSE

**Risk:** `boolean_col = 1` or `boolean_col = 0` throws a type mismatch error in ANSI mode because you cannot implicitly compare BOOLEAN to INT.

**Detect:**
```regex
\b\w+\s*=\s*1\b
```
```regex
\b\w+\s*=\s*0\b
```
```regex
\bWHERE\b[^;]*\b\w+\s*=\s*[01]\b
```

> **Note:** These regexes will match non-boolean comparisons too. Validate against the schema — only apply the fix if the column is actually BOOLEAN.

**Before:**
```sql
SELECT * FROM members WHERE is_active = 1
SELECT * FROM members WHERE is_active = 0
```

**After:**
```sql
SELECT * FROM members WHERE is_active IS TRUE
SELECT * FROM members WHERE is_active IS FALSE
```

**Alternative — explicit BOOLEAN cast:**
```sql
SELECT * FROM members WHERE is_active = TRUE
SELECT * FROM members WHERE is_active = FALSE
```

### 3.7 to_timestamp on Invalid Data → try_to_timestamp

**Risk:** `to_timestamp('not-a-date')` throws an error in ANSI mode instead of returning `NULL`.

**Detect:**
```regex
\bto_timestamp\s*\(
```

**Before:**
```sql
SELECT to_timestamp(date_string, 'yyyy-MM-dd HH:mm:ss') AS event_ts
FROM events_raw
```

**After:**
```sql
SELECT try_to_timestamp(date_string, 'yyyy-MM-dd HH:mm:ss') AS event_ts
FROM events_raw
```

### 3.8 to_date on Invalid Data → try_to_date

**Risk:** `to_date('not-a-date')` throws an error in ANSI mode instead of returning `NULL`.

**Detect:**
```regex
\bto_date\s*\(
```

**Before:**
```sql
SELECT to_date(date_string, 'yyyyMMdd') AS event_date
FROM events_raw
```

**After:**
```sql
SELECT try_to_date(date_string, 'yyyyMMdd') AS event_date
FROM events_raw
```

### 3.9 Implicit Cast Failures

ANSI mode disallows many implicit casts that silently worked before.

**Detect — string compared to numeric:**
```regex
\bWHERE\b[^;]*'[^']*'\s*[><=!]+\s*\d+
```
```regex
\bWHERE\b[^;]*\d+\s*[><=!]+\s*'[^']*'
```

**Before:**
```sql
-- Implicit string-to-int comparison (worked when ANSI was off):
SELECT * FROM claims WHERE claim_status = 0
-- If claim_status is STRING, this fails in ANSI mode
```

**After:**
```sql
SELECT * FROM claims WHERE claim_status = '0'
-- Or: WHERE TRY_CAST(claim_status AS INT) = 0
```

**Common implicit cast failures in ANSI mode:**

| Expression | ANSI OFF Result | ANSI ON Result | Fix |
|-----------|----------------|----------------|-----|
| `'abc' + 1` | `NULL` | Error | Use `TRY_CAST('abc' AS INT) + 1` |
| `WHERE string_col > 10` | Implicit cast, may work | Error | `WHERE TRY_CAST(string_col AS INT) > 10` |
| `SUM(string_col)` | Implicit cast to numeric | Error | `SUM(TRY_CAST(string_col AS DOUBLE))` |
| `UNION` of mismatched types | Implicit cast | Error | Explicit `CAST` on one side |

### 3.10 String Concatenation with NULL

**Risk:** In ANSI mode, `NULL || 'text'` returns `NULL` (standard SQL behavior). This is actually the same behavior as ANSI OFF in most Spark versions, but it catches people by surprise.

**Detect:**
```regex
\|\|
```
```regex
\bCONCAT\s*\(
```

**Before (if relying on concatenation ignoring nulls):**
```sql
SELECT first_name || ' ' || last_name AS full_name
FROM members
-- If first_name is NULL, result is NULL
```

**After (null-safe concatenation):**
```sql
-- Option 1 — CONCAT_WS (skips nulls):
SELECT CONCAT_WS(' ', first_name, last_name) AS full_name
FROM members

-- Option 2 — COALESCE each operand:
SELECT COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') AS full_name
FROM members
```

### 3.11 Summary of ANSI Fixes

| Pattern | ANSI OFF (Legacy) | ANSI ON (DBSQL) | Fix |
|---------|-------------------|-----------------|-----|
| `CAST(x AS INT)` on bad data | Returns `NULL` | Throws error | `TRY_CAST(x AS INT)` |
| `x / 0` | Returns `NULL` | Throws error | `TRY_DIVIDE(x, y)` |
| `array[out_of_bounds]` | Returns `NULL` | Throws error | `TRY_ELEMENT_AT(array, idx)` |
| `map['missing']` | Returns `NULL` | Throws error | `TRY_ELEMENT_AT(map, key)` |
| `INT * INT` overflow | Wraps around | Throws error | `CAST(x AS BIGINT) * y` |
| `bool_col = 1` | Works (implicit cast) | Type error | `bool_col IS TRUE` |
| `to_timestamp('bad')` | Returns `NULL` | Throws error | `try_to_timestamp('bad')` |
| `to_date('bad')` | Returns `NULL` | Throws error | `try_to_date('bad')` |
| Implicit string→int | Silent cast | Throws error | Explicit `TRY_CAST` |
| `NULL \|\| 'text'` | `NULL` | `NULL` | `CONCAT_WS` or `COALESCE` |

---

## 4. SQL Dialect Differences (Classic Compute vs DBSQL)

### 4.1 Functions Available in Both

The vast majority of Spark SQL functions work identically in both environments. These include:

- All standard aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`
- All window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`
- All string functions: `CONCAT`, `SUBSTRING`, `TRIM`, `UPPER`, `LOWER`, `REGEXP_REPLACE`, `REGEXP_EXTRACT`
- All date/time functions: `DATE_ADD`, `DATE_SUB`, `DATEDIFF`, `MONTHS_BETWEEN`, `DATE_TRUNC`, `DATE_FORMAT`
- All conditional functions: `CASE WHEN`, `IF`, `COALESCE`, `NULLIF`, `NVL`, `NVL2`
- Delta operations: `MERGE INTO`, `UPDATE`, `DELETE`, `INSERT INTO`, `CREATE TABLE`, `ALTER TABLE`
- JSON functions: `GET_JSON_OBJECT`, `FROM_JSON`, `TO_JSON`, `JSON_TUPLE`, `SCHEMA_OF_JSON`
- Array functions: `ARRAY`, `ARRAY_CONTAINS`, `EXPLODE`, `POSEXPLODE`, `SIZE`, `SORT_ARRAY`, `FLATTEN`
- Map functions: `MAP`, `MAP_KEYS`, `MAP_VALUES`, `MAP_FROM_ARRAYS`, `TRANSFORM_KEYS`, `TRANSFORM_VALUES`

### 4.2 Functions Available Only in DBSQL (or Newer in Current Channel)

DBSQL Current Channel runs the latest SQL engine and may have functions not available on DBR 13.3:

| Function | Purpose | Notes |
|----------|---------|-------|
| `TRY_CAST` | Safe casting | Available on 13.3 too, but more commonly needed on DBSQL |
| `TRY_DIVIDE` | Safe division | Available on 13.3 too |
| `TRY_ELEMENT_AT` | Safe array/map access | Available on 13.3 too |
| `try_to_timestamp` | Safe timestamp parse | Available on 13.3 too |
| `try_to_date` | Safe date parse | Available on 13.3 too |
| `IDENTIFIER()` | Dynamic table/column references | May not be available on 13.3 |
| `AI_GENERATE_TEXT()` | AI function | DBSQL only |
| `AI_QUERY()` | AI function | DBSQL only |
| `H3` family functions | Geospatial | Availability varies by version |

### 4.3 Functions Available Only in Classic Compute SQL

These functions are available when running SQL on a classic cluster (within PySpark runtime) but **not** in a pure DBSQL SQL notebook:

| Function/Pattern | Why Not Available | Workaround |
|------------------|-------------------|------------|
| Python UDFs called from SQL | No Python runtime in DBSQL | Convert to SQL UDFs (see Section 13) |
| Scala UDFs called from SQL | No Scala runtime in DBSQL | Convert to SQL UDFs |
| `spark.sql()` result as DataFrame | No DataFrame API | Just write the SQL directly |
| Custom Hive UDFs (JAR-based) | No JAR support | Rewrite as SQL UDFs or use serverless general compute |

### 4.4 Behavioral Differences in Shared Functions

#### Date/Time Function Precision

| Behavior | Classic Compute (13.3) | DBSQL Current Channel |
|----------|------------------------|----------------------|
| `CURRENT_TIMESTAMP()` precision | Microseconds | Microseconds |
| `DATE_FORMAT` patterns | Java SimpleDateFormat | Java SimpleDateFormat |
| Timestamp type default | `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ` |
| Calendar system | Proleptic Gregorian | Proleptic Gregorian |

No functional differences for standard patterns. Watch for:
- Pre-1582 dates: Both use Proleptic Gregorian, but verify behavior matches
- Timezone handling: DBSQL warehouse timezone may differ from cluster timezone — check `SET timezone`

#### String Function Behavior with NULLs

Identical behavior in ANSI mode. The key difference is that **ANSI is always on** in DBSQL, so:

| Function | ANSI OFF (classic default) | ANSI ON (DBSQL always) |
|----------|---------------------------|----------------------|
| `SUBSTR(NULL, 1, 5)` | `NULL` | `NULL` |
| `CONCAT(NULL, 'abc')` | `NULL` | `NULL` |
| `UPPER(NULL)` | `NULL` | `NULL` |
| `CAST(NULL AS INT)` | `NULL` | `NULL` |

No difference for null inputs. The difference is on **invalid** inputs (see Section 3).

#### Aggregate Function NULL Handling

Identical in both environments:
- `SUM`, `AVG`, `MIN`, `MAX` skip `NULL` values
- `COUNT(*)` counts all rows; `COUNT(col)` skips nulls
- `FIRST(col, true)` / `LAST(col, true)` — the `ignore nulls` parameter works the same

### 4.5 DBSQL Current Channel Specifics

- **Latest Spark SQL engine**: DBSQL Current Channel may include SQL features ahead of the latest LTS runtime
- **Photon enabled by default**: Query execution uses Photon (vectorized C++ engine) — faster for most workloads
- **ANSI always enabled**: Cannot be disabled
- **No preview features opt-in**: Current Channel is the production channel; "Preview" channel is separate
- **Serverless auto-scaling**: Warehouse scales up/down automatically based on query load

---

## 5. Spark Configuration to SQL Warehouse Configuration

### 5.1 spark.conf.set() in Notebooks — Not Applicable

**Detect:**
```regex
spark\.conf\.set\s*\(
```
```regex
spark\.sql\s*\(\s*["']SET\s
```
```regex
^SET\s+\w+
```

PySpark `spark.conf.set()` calls have no equivalent in DBSQL SQL notebooks. There is no `spark` session object.

**Before:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

**After:** Remove entirely. The warehouse manages these.

### 5.2 SET Statements in SQL — Partial Support

Some `SET` statements work in DBSQL as session-level SQL configuration.

**Configs that DO work in DBSQL via SET:**

| Config | Example | Notes |
|--------|---------|-------|
| `timezone` | `SET timezone = 'America/Chicago'` | Session timezone |
| `spark.sql.legacy.timeParserPolicy` | `SET spark.sql.legacy.timeParserPolicy = LEGACY` | Date parsing behavior |
| `spark.databricks.delta.schema.autoMerge.enabled` | `SET spark.databricks.delta.schema.autoMerge.enabled = true` | Delta schema evolution |
| `spark.sql.files.maxPartitionBytes` | `SET spark.sql.files.maxPartitionBytes = 128MB` | File read partition size |
| `spark.sql.autoBroadcastJoinThreshold` | `SET spark.sql.autoBroadcastJoinThreshold = 10485760` | Broadcast join threshold |

**Before (SET in SQL cell on classic):**
```sql
SET spark.sql.shuffle.partitions = 200;
SET spark.sql.ansi.enabled = false;
SET spark.databricks.delta.optimizeWrite.enabled = true;

SELECT * FROM claims_silver;
```

**After (DBSQL SQL notebook):**
```sql
-- Removed: spark.sql.shuffle.partitions (warehouse manages)
-- Removed: spark.sql.ansi.enabled (always true, cannot be changed)
-- Removed: spark.databricks.delta.optimizeWrite.enabled (warehouse manages)

SELECT * FROM claims_silver;
```

### 5.3 Configs That Have No Equivalent in DBSQL

| Config Category | Examples | Action |
|----------------|----------|--------|
| Shuffle/partition tuning | `spark.sql.shuffle.partitions`, `spark.sql.adaptive.coalescePartitions.*` | **Remove** — warehouse auto-manages |
| AQE configs | `spark.sql.adaptive.enabled`, `spark.sql.adaptive.skewJoin.*` | **Remove** — warehouse auto-manages (AQE always on) |
| Executor/driver configs | `spark.executor.memory`, `spark.driver.memory`, `spark.executor.cores` | **Remove** — not applicable |
| Python configs | `spark.sql.execution.pyspark.*` | **Remove** — no Python runtime |
| ANSI config | `spark.sql.ansi.enabled` | **Remove** — always true, cannot change |
| Broadcast join | `spark.sql.autoBroadcastJoinThreshold` | **Supported** — can SET in DBSQL |
| Default data source | `spark.sql.sources.default` | **Remove** — Delta is always default |

**Detect — configs to remove:**
```regex
\bSET\s+spark\.sql\.shuffle\.partitions\b
```
```regex
\bSET\s+spark\.sql\.adaptive\.\w+
```
```regex
\bSET\s+spark\.executor\.\w+
```
```regex
\bSET\s+spark\.driver\.\w+
```
```regex
\bSET\s+spark\.sql\.ansi\.enabled\b
```
```regex
\bSET\s+spark\.sql\.execution\.pyspark\.\w+
```

### 5.4 Configs That DO Work in DBSQL

| Config | Via | Notes |
|--------|-----|-------|
| Delta table properties | `ALTER TABLE SET TBLPROPERTIES` | Full support |
| Session timezone | `SET timezone` | Full support |
| SQL-level session settings | `SET <config>` for supported SQL configs | Partial — test first |
| Warehouse-level configs | SQL warehouse admin UI → Configuration tab | Admin-managed |

**Before (cluster config):**
```
spark.sql.shuffle.partitions 200
spark.databricks.delta.optimizeWrite.enabled true
spark.sql.sources.default delta
spark.sql.ansi.enabled false
```

**After:** No cluster config needed. Warehouse manages all of these. For Delta table properties, use:
```sql
ALTER TABLE claims_silver SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## 6. Removing Non-SQL Operations

### 6.1 CACHE TABLE / .persist() / .cache()

**Detect:**
```regex
\bCACHE\s+(LAZY\s+)?TABLE\b
```
```regex
\.cache\s*\(\s*\)
```
```regex
\.persist\s*\(
```
```regex
\bUNCACHE\s+TABLE\b
```

DBSQL has its own result caching layer. Manual caching is generally unnecessary and can be counterproductive.

**Before:**
```sql
CACHE TABLE claims_silver;
SELECT * FROM claims_silver WHERE claim_date >= '2024-01-01';
UNCACHE TABLE claims_silver;
```

**After:**
```sql
-- CACHE/UNCACHE removed — DBSQL manages result caching automatically.
SELECT * FROM claims_silver WHERE claim_date >= '2024-01-01';
```

> **Note:** `CACHE TABLE` does work in DBSQL but behaves differently. DBSQL caches query results at the warehouse level. Using `CACHE TABLE` forces the warehouse to read the entire table into memory, which may be slower than letting the warehouse cache naturally via query patterns.

### 6.2 REFRESH TABLE

**Detect:**
```regex
\bREFRESH\s+TABLE\b
```

**Before:**
```sql
REFRESH TABLE claims_silver;
SELECT * FROM claims_silver;
```

**After:**
```sql
-- REFRESH TABLE removed — DBSQL always reads current data from Delta.
SELECT * FROM claims_silver;
```

### 6.3 MSCK REPAIR TABLE

**Detect:**
```regex
\bMSCK\s+REPAIR\s+TABLE\b
```

**Before:**
```sql
MSCK REPAIR TABLE external_claims;
```

**After:**
```sql
-- MSCK REPAIR TABLE removed — not applicable for Unity Catalog managed/external Delta tables.
-- If needed for non-Delta external tables, run as a separate maintenance task on classic compute.
```

### 6.4 Temporary Views

Temp views work in DBSQL but are session-scoped. In a DBSQL SQL notebook, each cell runs in the same session within a single notebook run.

**Detect:**
```regex
\bCREATE\s+(OR\s+REPLACE\s+)?(TEMPORARY|TEMP)\s+VIEW\b
```
```regex
\bCREATE\s+(OR\s+REPLACE\s+)?GLOBAL\s+TEMP(ORARY)?\s+VIEW\b
```

**Before (global temp view):**
```sql
CREATE OR REPLACE GLOBAL TEMPORARY VIEW active_claims AS
SELECT * FROM claims_silver WHERE status = 'ACTIVE';

SELECT * FROM global_temp.active_claims;
```

**After (session-scoped temp view or CTE):**

Option A — Session-scoped temp view (if referenced in multiple cells):
```sql
CREATE OR REPLACE TEMPORARY VIEW active_claims AS
SELECT * FROM claims_silver WHERE status = 'ACTIVE';
```
```sql
-- In a later cell:
SELECT * FROM active_claims;
```

Option B — CTE (if only used once):
```sql
WITH active_claims AS (
  SELECT * FROM claims_silver WHERE status = 'ACTIVE'
)
SELECT * FROM active_claims;
```

> **Important:** Global temporary views (`global_temp.view_name`) are NOT supported in DBSQL. Convert to session-scoped temp views or CTEs.

### 6.5 DBFS Paths

**Detect:**
```regex
dbfs:/
```
```regex
/dbfs/
```
```regex
/mnt/
```

**Before:**
```sql
COPY INTO claims_raw
FROM 'dbfs:/mnt/landing/claims/'
FILEFORMAT = CSV;
```

**After:**
```sql
COPY INTO claims_raw
FROM '/Volumes/prod_catalog/default/landing/claims/'
FILEFORMAT = CSV;
```

Or using Unity Catalog external locations:
```sql
COPY INTO claims_raw
FROM 'abfss://landing@molinastorage.dfs.core.windows.net/claims/'
FILEFORMAT = CSV;
```

---

## 7. Job/Workflow Migration

### 7.1 Converting a Notebook Task to a SQL Task

In Databricks Workflows, the task type changes from a notebook task (running on a cluster) to a SQL task (running on a SQL warehouse).

**Before (job JSON — notebook task on cluster):**
```json
{
  "task_key": "load_claims",
  "notebook_task": {
    "notebook_path": "/Repos/prod/etl/load_claims",
    "base_parameters": {
      "env": "prod",
      "start_date": "2024-01-01"
    }
  },
  "job_cluster_key": "etl_cluster",
  "timeout_seconds": 3600
}
```

**After (job JSON — SQL notebook task on serverless warehouse):**
```json
{
  "task_key": "load_claims",
  "notebook_task": {
    "notebook_path": "/Repos/prod/etl/load_claims_sql",
    "warehouse_id": "abc123def456",
    "base_parameters": {
      "env": "prod",
      "start_date": "2024-01-01"
    }
  },
  "timeout_seconds": 3600
}
```

Or using a SQL task directly:
```json
{
  "task_key": "load_claims",
  "sql_task": {
    "warehouse_id": "abc123def456",
    "file": {
      "path": "/Repos/prod/etl/load_claims.sql"
    },
    "parameters": {
      "env": "prod",
      "start_date": "2024-01-01"
    }
  },
  "timeout_seconds": 3600
}
```

### 7.2 Warehouse Selection

For serverless SQL warehouses, set:
- `warehouse_id`: The ID of the serverless SQL warehouse
- Or omit `warehouse_id` and set `"serverless"` at the job level to use serverless SQL compute

**Warehouse sizing guidance:**
- Small (2X-Small to Small): Simple ETL, single-table operations
- Medium: Multi-table joins, moderate aggregations
- Large: Complex analytics, large MERGE operations

DBSQL Serverless auto-scales — the warehouse size is a starting point, not a ceiling.

### 7.3 Parameter Passing

Job parameters map to SQL notebook parameters:

| Job Config | SQL Notebook Access |
|-----------|-------------------|
| `base_parameters.env` | `:env` in SQL |
| `base_parameters.start_date` | `:start_date` in SQL |
| Dynamic value from upstream task | `:param` passed via task value |

**Example — accessing job parameters in SQL:**
```sql
USE CATALOG IDENTIFIER(:env || '_catalog');

SELECT *
FROM claims_silver
WHERE claim_date >= :start_date;
```

### 7.4 Multi-Task Workflow Considerations

| Consideration | Classic Compute | DBSQL Serverless |
|--------------|----------------|------------------|
| Shared cluster across tasks | Yes — `job_cluster_key` | No — each SQL task uses the warehouse independently |
| Temp views across tasks | Not shared (separate sessions) | Not shared (separate sessions) |
| Task dependencies | `depends_on` | `depends_on` (same) |
| Parameter passing between tasks | `dbutils.jobs.taskValues` | `dbutils.jobs.taskValues` (limited in SQL — use tables instead) |
| Concurrent tasks | Limited by cluster resources | Warehouse auto-scales for concurrency |

**Pattern for passing data between SQL tasks in a workflow:**
Instead of using task values (which require Python), write intermediate results to tables:

```sql
-- Task 1: Produce intermediate results
CREATE OR REPLACE TABLE staging.intermediate_claims AS
SELECT * FROM claims_raw WHERE claim_date >= :start_date;
```

```sql
-- Task 2 (depends on Task 1): Consume intermediate results
MERGE INTO claims_silver AS target
USING staging.intermediate_claims AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### 7.5 Schedule Migration

No changes needed for job scheduling. Cron expressions, trigger types, and alert configurations work identically. The only change is the task type and compute target.

---

## 8. Widget/Parameter Migration

### 8.1 Widget Types and Equivalents

| Python Widget | SQL Equivalent | Notes |
|--------------|----------------|-------|
| `dbutils.widgets.text("name", "default")` | SQL parameter `:name` with default | Parameters defined in notebook UI or workflow |
| `dbutils.widgets.dropdown("name", "default", ["a", "b"])` | SQL parameter `:name` | No dropdown UI in SQL notebooks — use text parameter |
| `dbutils.widgets.combobox(...)` | SQL parameter `:name` | No combobox UI in SQL notebooks |
| `dbutils.widgets.multiselect(...)` | Not directly supported | Split into multiple parameters or use comma-separated string |
| `dbutils.widgets.get("name")` | `:name` in SQL | Direct reference |
| `dbutils.widgets.remove("name")` | N/A | Not needed in SQL notebooks |
| `dbutils.widgets.removeAll()` | N/A | Not needed in SQL notebooks |

### 8.2 Python f-string Interpolation to SQL Parameters

**Detect:**
```regex
spark\.sql\s*\(\s*f["']{1,3}
```

**Before (Python f-string in spark.sql):**
```python
env = dbutils.widgets.get("env")
table_name = f"{env}_catalog.claims_schema.claims_silver"
cutoff_date = dbutils.widgets.get("cutoff_date")

spark.sql(f"""
  SELECT member_id, claim_date, paid_amount
  FROM {table_name}
  WHERE claim_date >= '{cutoff_date}'
  AND status IN ('PAID', 'APPROVED')
""")
```

**After (SQL with parameters):**
```sql
-- Parameters: env, cutoff_date (passed via workflow or notebook UI)
USE CATALOG IDENTIFIER(:env || '_catalog');

SELECT member_id, claim_date, paid_amount
FROM claims_schema.claims_silver
WHERE claim_date >= :cutoff_date
  AND status IN ('PAID', 'APPROVED')
```

### 8.3 Dynamic Table Names

**Detect:**
```regex
spark\.sql\s*\(\s*f["']{1,3}[^"']*\{[^}]*\}\s*\.\s*\{
```

When Python variables are used for table names, use the `IDENTIFIER()` function:

**Before:**
```python
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
spark.sql(f"SELECT * FROM {schema}.{table}")
```

**After:**
```sql
SELECT * FROM IDENTIFIER(:schema || '.' || :table)
```

Or use `USE SCHEMA` and 1-part table name:
```sql
USE SCHEMA IDENTIFIER(:schema);
SELECT * FROM IDENTIFIER(:table);
```

### 8.4 Parameter Types in DBSQL

SQL parameters in DBSQL are untyped strings by default. For type safety, cast in the query:

```sql
-- String parameter used as DATE:
WHERE claim_date >= CAST(:start_date AS DATE)

-- String parameter used as INT:
WHERE member_count > CAST(:threshold AS INT)

-- String parameter used directly (string comparison is fine):
WHERE status = :status
```

### 8.5 Default Values

In SQL notebooks, set defaults using `DECLARE`:

```sql
-- Set defaults at the top of the notebook:
DECLARE OR REPLACE env STRING DEFAULT 'dev';
DECLARE OR REPLACE start_date DATE DEFAULT CURRENT_DATE() - INTERVAL 30 DAYS;
DECLARE OR REPLACE batch_size INT DEFAULT 10000;
```

When parameters are passed via the workflow, they override `DECLARE` defaults.

---

## 9. Output and Display Differences

### 9.1 display(df) → Direct SELECT

**Detect:**
```regex
display\s*\(
```

In DBSQL SQL notebooks, every `SELECT` statement automatically renders its results in a table. No `display()` call is needed or available.

**Before:**
```python
result = spark.sql("SELECT member_id, SUM(paid_amount) AS total FROM claims GROUP BY 1")
display(result)
```

**After:**
```sql
SELECT member_id, SUM(paid_amount) AS total
FROM claims
GROUP BY 1
```

### 9.2 df.show() → SELECT with LIMIT

**Detect:**
```regex
\.show\s*\(
```

**Before:**
```python
result = spark.sql("SELECT * FROM claims_silver")
result.show(20, truncate=False)
```

**After:**
```sql
SELECT * FROM claims_silver
LIMIT 20
```

### 9.3 print() for Row Counts → SELECT COUNT(*)

**Detect:**
```regex
print\s*\(\s*(?:f?["'][^"']*count|.*\.count\s*\(\s*\))
```

**Before:**
```python
count = spark.sql("SELECT COUNT(*) FROM claims_silver").collect()[0][0]
print(f"Total rows: {count}")
```

**After:**
```sql
SELECT COUNT(*) AS total_rows FROM claims_silver
```

### 9.4 Handling Multiple Result Sets

In DBSQL SQL notebooks, each cell shows its own result. To show multiple results, use separate cells.

**Before (single Python cell with multiple displays):**
```python
total = spark.sql("SELECT COUNT(*) AS total FROM claims")
by_status = spark.sql("SELECT status, COUNT(*) AS cnt FROM claims GROUP BY 1")
display(total)
display(by_status)
```

**After (two SQL cells):**

Cell 1:
```sql
SELECT COUNT(*) AS total FROM claims
```

Cell 2:
```sql
SELECT status, COUNT(*) AS cnt FROM claims GROUP BY 1
```

---

## 10. Delta Table Operations in DBSQL

All standard Delta operations work in DBSQL Serverless. This section confirms compatibility and notes behavioral differences.

### 10.1 MERGE INTO — Works Identically

```sql
MERGE INTO claims_silver AS target
USING claims_staging AS source
ON target.claim_id = source.claim_id
WHEN MATCHED AND source.updated_date > target.updated_date
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;
```

No changes needed. MERGE performance may differ (Photon-optimized in DBSQL).

### 10.2 OPTIMIZE — Works, Warehouse May Auto-Manage

```sql
OPTIMIZE claims_silver;
OPTIMIZE claims_silver ZORDER BY (member_id, claim_date);
```

Works in DBSQL. However, **Predictive Optimization** (available on DBSQL) can automatically run OPTIMIZE. If Predictive Optimization is enabled on the table, manual OPTIMIZE calls are redundant.

**Check if Predictive Optimization is enabled:**
```sql
DESCRIBE DETAIL claims_silver;
-- Look for predictive optimization status in properties
```

### 10.3 VACUUM — Works, Different Defaults

```sql
VACUUM claims_silver;
VACUUM claims_silver RETAIN 168 HOURS;
```

Works in DBSQL. Default retention is 7 days (168 hours). For external tables, schedule regular VACUUM since Predictive Optimization is not available.

### 10.4 DESCRIBE / SHOW Commands — Work Identically

```sql
DESCRIBE DETAIL claims_silver;
DESCRIBE HISTORY claims_silver;
SHOW TABLE PROPERTIES claims_silver;
SHOW COLUMNS IN claims_silver;
DESCRIBE TABLE EXTENDED claims_silver;
```

### 10.5 ALTER TABLE — Works Identically

```sql
ALTER TABLE claims_silver SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

ALTER TABLE claims_silver ADD COLUMN new_col STRING;
ALTER TABLE claims_silver CLUSTER BY (member_id, claim_date);
```

### 10.6 Predictive Optimization — Available on DBSQL

Predictive Optimization automatically runs OPTIMIZE and VACUUM on managed tables. This is a DBSQL advantage over classic compute.

```sql
-- Enable predictive optimization on a table (requires admin):
ALTER TABLE claims_silver ENABLE PREDICTIVE OPTIMIZATION;
```

---

## 11. Performance Considerations

### 11.1 Auto-Scaling and Auto-Optimization

DBSQL Serverless warehouses automatically:
- Scale up clusters for concurrent queries
- Scale down during idle periods
- Choose optimal join strategies
- Manage shuffle partitions
- Enable AQE (Adaptive Query Execution) automatically

**Remove all manual tuning:**
```sql
-- REMOVE these — warehouse manages automatically:
-- SET spark.sql.shuffle.partitions = 200;
-- SET spark.sql.adaptive.enabled = true;
-- SET spark.sql.adaptive.coalescePartitions.enabled = true;
```

### 11.2 Result Caching

DBSQL has a multi-layered cache:
1. **Local disk cache**: Raw data cached on warehouse nodes
2. **Result cache**: Query results cached and reused for identical queries

Result cache is invalidated when underlying data changes (Delta table updates).

**Implication:** Remove manual `CACHE TABLE` / `UNCACHE TABLE` calls. The warehouse's caching is more efficient.

### 11.3 Query Profile and Query History

DBSQL provides rich query analysis tools:
- **Query Profile**: Visual DAG of query execution plan
- **Query History**: Full audit of all queries executed
- **SQL warehouse monitoring**: Cluster utilization, queue times, peak concurrency

Use these for post-migration performance analysis instead of Spark UI.

### 11.4 Photon Engine

Photon is enabled by default on DBSQL Serverless. Benefits:
- 2-8x faster for scan-heavy queries
- Vectorized execution in C++
- Particularly effective for aggregations, joins, and Delta operations

No code changes needed to leverage Photon.

### 11.5 ANALYZE TABLE for External Tables

External tables do not benefit from auto-statistics collection. Run `ANALYZE TABLE` explicitly:

```sql
ANALYZE TABLE external_claims COMPUTE STATISTICS FOR ALL COLUMNS;
```

This helps the query optimizer choose better join strategies and scan plans.

---

## 12. Things NOT Supported in DBSQL SQL Notebooks

### 12.1 Hard Blockers — Cannot Work in DBSQL SQL Notebooks

| Feature | Detection Regex | Alternative |
|---------|----------------|-------------|
| Python/Scala/R code cells | `^%python`, `^%scala`, `^%r` | Move to serverless general compute (Path C) |
| Python UDFs | `spark\.udf\.register`, `@F\.udf`, `CREATE.*FUNCTION.*LANGUAGE PYTHON` | Convert to SQL UDFs (Section 13) |
| Scala UDFs | `spark\.udf\.register` (in Scala) | Convert to SQL UDFs |
| Streaming operations | `readStream`, `writeStream`, `STREAMING`, `CREATE STREAMING` | Use Lakeflow Declarative Pipelines or classic compute |
| ML operations | `from.*ml\.`, `MLflow`, `sklearn`, `tensorflow` | Use serverless general compute |
| `%run` | `^%run\b` | Restructure as separate tasks in a workflow |
| `dbutils.notebook.run()` | `dbutils\.notebook\.run\s*\(` | Restructure as separate tasks in a workflow |
| Notebook-scoped libraries | `%pip install`, `dbutils\.library` | Not available — use SQL UDFs or warehouse libraries |
| Custom data sources | `spark\.read\.format\("custom` | Use serverless general compute |
| RDD operations | `sc\.`, `\.rdd\.`, `\.rdd\b` | Rewrite as SQL or use serverless general compute |

### 12.2 dbutils — Limited Availability

| dbutils Module | Available in DBSQL? | Notes |
|---------------|---------------------|-------|
| `dbutils.widgets` | **Partial** — use SQL parameters instead | `.get()` works; `.text()`, `.dropdown()` do not |
| `dbutils.secrets` | **Yes** | `SELECT secret('scope', 'key')` also works |
| `dbutils.fs` | **No** | Use Unity Catalog Volumes or external locations |
| `dbutils.notebook` | **No** | Use workflow task dependencies |
| `dbutils.library` | **No** | Not applicable |
| `dbutils.jobs.taskValues` | **No** | Write to tables instead |

### 12.3 %run Replacement Pattern

**Detect:**
```regex
^%run\s+
```
```regex
dbutils\.notebook\.run\s*\(
```

**Before (notebook with %run dependency):**
```python
%run ./config_notebook
# config_notebook sets variables: env, database, start_date

spark.sql(f"SELECT * FROM {database}.claims WHERE claim_date >= '{start_date}'")
```

**After (restructure as workflow):**

Workflow task 1 (config): Sets parameters at the workflow level
Workflow task 2 (SQL notebook): Receives parameters

```sql
-- SQL notebook (receives parameters from workflow):
USE CATALOG IDENTIFIER(:env || '_catalog');

SELECT *
FROM claims_schema.claims
WHERE claim_date >= :start_date;
```

---

## 13. SQL UDF Opportunities

### 13.1 When to Convert Python UDFs to SQL UDFs

If the source code had simple Python UDFs that were just SQL logic wrapped in Python, they can be converted to SQL UDFs and used in DBSQL.

**Detect — Python UDFs that are SQL-convertible:**
```regex
@F\.udf\b
```
```regex
spark\.udf\.register\s*\(
```

**Good candidates for SQL UDF conversion:**
- String manipulation (upper, lower, trim, regex)
- Date calculations
- Conditional logic (if/else that maps to CASE WHEN)
- Simple arithmetic
- Null coalescing

**Bad candidates (keep on serverless general compute):**
- Complex Python logic with loops
- External library calls
- File I/O
- HTTP requests
- Complex state management

### 13.2 Scalar SQL UDFs

**Before (Python UDF):**
```python
@F.udf(returnType=StringType())
def clean_phone(phone):
    if phone is None:
        return None
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone

spark.udf.register("clean_phone", clean_phone)
```

**After (SQL UDF):**
```sql
CREATE OR REPLACE FUNCTION clean_phone(phone STRING)
RETURNS STRING
RETURN
  CASE
    WHEN phone IS NULL THEN NULL
    WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) = 10
      THEN CONCAT(
        '(', SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 1, 3), ') ',
        SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 4, 3), '-',
        SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), 7, 4)
      )
    ELSE phone
  END;
```

Usage:
```sql
SELECT member_id, clean_phone(phone_number) AS formatted_phone
FROM members;
```

### 13.3 Table-Valued SQL UDFs

**Before (Python function returning DataFrame):**
```python
def get_member_claims(member_id_param):
    return spark.sql(f"""
        SELECT claim_id, claim_date, paid_amount
        FROM claims_silver
        WHERE member_id = '{member_id_param}'
        ORDER BY claim_date DESC
    """)
```

**After (SQL table-valued function):**
```sql
CREATE OR REPLACE FUNCTION get_member_claims(member_id_param STRING)
RETURNS TABLE (claim_id STRING, claim_date DATE, paid_amount DOUBLE)
RETURN
  SELECT claim_id, claim_date, paid_amount
  FROM claims_silver
  WHERE member_id = member_id_param
  ORDER BY claim_date DESC;
```

Usage:
```sql
SELECT * FROM get_member_claims('MEM001');

-- Or in a lateral join:
SELECT m.member_id, c.*
FROM members m,
LATERAL get_member_claims(m.member_id) c;
```

### 13.4 When NOT to Use SQL UDFs

- Performance-critical hot paths (SQL UDFs are inlined but may not optimize as well as native SQL)
- Complex string parsing that requires regex beyond Spark SQL's capabilities
- Logic that needs external data lookups (use JOINs instead)
- Anything requiring error handling beyond CASE WHEN (SQL UDFs cannot try/catch)

---

## 14. Testing and Validation

### 14.1 Archive Originals

Before making any changes:
1. Record the current Delta table version for every output table:
```sql
DESCRIBE HISTORY claims_silver LIMIT 1;
-- Record the version number (e.g., version 47)
```
2. Archive the original notebook (copy to an archive folder or rely on version control)

### 14.2 Parallel Run Strategy

Run both the original (classic compute) and migrated (DBSQL) notebooks against the same input data, writing to separate output schemas.

**Original (classic):** Writes to `prod_catalog.claims_schema.claims_silver`
**Migrated (DBSQL):** Writes to `prod_catalog.claims_schema_validation.claims_silver`

### 14.3 Compare Output Tables

For every output table, run these comparisons:

**Schema comparison:**
```sql
-- Original schema:
DESCRIBE TABLE prod_catalog.claims_schema.claims_silver;

-- Migrated schema:
DESCRIBE TABLE prod_catalog.claims_schema_validation.claims_silver;
```

**Row count comparison:**
```sql
SELECT
  (SELECT COUNT(*) FROM prod_catalog.claims_schema.claims_silver) AS original_count,
  (SELECT COUNT(*) FROM prod_catalog.claims_schema_validation.claims_silver) AS migrated_count;
```

**Data comparison (full diff):**
```sql
-- Rows in original but not in migrated:
SELECT * FROM prod_catalog.claims_schema.claims_silver
EXCEPT
SELECT * FROM prod_catalog.claims_schema_validation.claims_silver;

-- Rows in migrated but not in original:
SELECT * FROM prod_catalog.claims_schema_validation.claims_silver
EXCEPT
SELECT * FROM prod_catalog.claims_schema.claims_silver;
```

**Column-level aggregate comparison:**
```sql
SELECT
  'original' AS source,
  COUNT(*) AS row_count,
  COUNT(DISTINCT member_id) AS distinct_members,
  SUM(paid_amount) AS total_paid,
  MIN(claim_date) AS min_date,
  MAX(claim_date) AS max_date,
  SUM(CASE WHEN paid_amount IS NULL THEN 1 ELSE 0 END) AS null_paid_count
FROM prod_catalog.claims_schema.claims_silver

UNION ALL

SELECT
  'migrated' AS source,
  COUNT(*) AS row_count,
  COUNT(DISTINCT member_id) AS distinct_members,
  SUM(paid_amount) AS total_paid,
  MIN(claim_date) AS min_date,
  MAX(claim_date) AS max_date,
  SUM(CASE WHEN paid_amount IS NULL THEN 1 ELSE 0 END) AS null_paid_count
FROM prod_catalog.claims_schema_validation.claims_silver;
```

### 14.4 DBSQL-Specific Validation Checks

**Confirm the job ran on a SQL warehouse (not a cluster):**
- Check the workflow run details in the Databricks UI
- The "Compute" column should show the SQL warehouse name, not a cluster
- Query history should show the queries in the SQL warehouse query log

**Check query profile for performance:**
- Navigate to SQL > Query History
- Find the queries from the migrated notebook run
- Review the query profile for any spills-to-disk, skew, or full table scans

**Verify parameter passing:**
```sql
-- In the SQL notebook, verify parameters are received:
SELECT
  :env AS env_param,
  :start_date AS start_date_param;
```

### 14.5 ANSI-Specific Validation

Run the migrated SQL and watch specifically for these error patterns:

| Error Message Pattern | Root Cause | Fix Reference |
|----------------------|------------|---------------|
| `ArithmeticException: divide by zero` | Division by zero | Section 3.2 |
| `NumberFormatException` | Invalid CAST | Section 3.1 |
| `ArrayIndexOutOfBoundsException` | Array index out of range | Section 3.3 |
| `NoSuchElementException` | Map key not found | Section 3.4 |
| `ArithmeticException: overflow` | Integer overflow | Section 3.5 |
| `CAST_INVALID_INPUT` | Implicit cast failure | Section 3.9 |

---

## 15. Known Issues and Edge Cases

### 15.1 Query Timeout Differences

| Aspect | Classic Compute | DBSQL Serverless |
|--------|----------------|------------------|
| Default timeout | Set by job/cluster config | 172800 seconds (48 hours) for warehouse |
| Per-query timeout | No per-query limit (limited by job) | Configurable per warehouse |
| Long-running queries | Run until complete or job timeout | May be cancelled if warehouse scales down |

**Mitigation:** For very long-running queries (>1 hour), verify the warehouse timeout is sufficient. Set at the warehouse admin level.

### 15.2 Max Result Size Differences

| Aspect | Classic Compute | DBSQL Serverless |
|--------|----------------|------------------|
| `display()` limit | 10,000 rows (in notebook) | 100,000 rows (in DBSQL notebook) |
| `SELECT` without `LIMIT` | Returns all rows to driver | Returns all rows but displays up to 100K |
| Download limit | No hard limit | 1GB for CSV download |

**Mitigation:** For large result sets intended for downstream processing, write to a table instead of displaying:
```sql
CREATE OR REPLACE TABLE staging.large_result AS
SELECT * FROM complex_query;
```

### 15.3 Concurrent Query Limits

| Aspect | Classic Compute | DBSQL Serverless |
|--------|----------------|------------------|
| Concurrent queries | Limited by cluster resources | Auto-scales, but has per-warehouse concurrency limit |
| Queue behavior | Spark scheduler queues tasks | Warehouse queues queries when at capacity |
| Multi-notebook | Shared cluster, resource contention | Each notebook gets fair share of warehouse resources |

**Mitigation:** For high-concurrency workloads, consider separate warehouses or adjust warehouse size.

### 15.4 Session Isolation Differences

| Aspect | Classic Compute | DBSQL Serverless |
|--------|----------------|------------------|
| Temp views | Session-scoped | Session-scoped (same within a notebook run) |
| SET commands | Session-scoped | Session-scoped |
| Cross-notebook | Separate sessions | Separate sessions |
| Within a notebook | All cells share one session | All cells share one session |

**Key difference:** On classic compute, a Python notebook's `spark` session persists across all cells. In DBSQL, a SQL notebook's session persists across all cells within a single run. Behavior is equivalent for most use cases.

### 15.5 Temp View Scope Differences

| Behavior | Classic Compute | DBSQL SQL Notebook |
|----------|----------------|-------------------|
| `CREATE TEMPORARY VIEW` | Survives until session ends | Survives until notebook run completes |
| `CREATE GLOBAL TEMPORARY VIEW` | Accessible via `global_temp.view_name` across notebooks | **NOT SUPPORTED** |
| Temp view across cells | Accessible | Accessible |
| Temp view in different notebook | Not accessible | Not accessible |

**Detect — global temp views:**
```regex
\bglobal_temp\.\w+
```
```regex
\bCREATE\s+GLOBAL\s+TEMP
```

### 15.6 Character Set and Collation

DBSQL follows standard SQL collation rules. Potential differences:

| Behavior | Classic Compute | DBSQL |
|----------|----------------|-------|
| String comparison | Binary by default | Binary by default |
| `LIKE` pattern matching | Case-sensitive | Case-sensitive |
| `ORDER BY` on strings | Binary sort | Binary sort |
| Unicode handling | Full Unicode | Full Unicode |

Generally no differences, but verify if the original code relied on locale-specific sorting.

### 15.7 Transaction Behavior

| Behavior | Classic Compute | DBSQL |
|----------|----------------|-------|
| Auto-commit | Each statement auto-commits | Each statement auto-commits |
| Multi-statement transactions | Not supported | Not supported |
| Delta ACID | Per-operation | Per-operation |
| MERGE atomicity | Atomic | Atomic |

No differences. Delta table operations are atomic in both environments.

### 15.8 Known DBSQL Quirks

| Issue | Description | Workaround |
|-------|-------------|------------|
| `SET` result display | `SET key = value` displays a result table | This is cosmetic — ignore the output |
| `USE CATALOG` scope | `USE CATALOG` persists for the session | Set at the top of every notebook to be explicit |
| Parameter with special characters | Parameters containing single quotes or semicolons may cause parse errors | Escape or encode special characters |
| Semicolons in SQL | DBSQL requires semicolons between statements in the same cell | Add `;` between statements if combining in one cell |

---

## 16. Step-by-Step Migration Process

### Step 1: Identify Eligible Notebooks

Scan the notebook for Path D eligibility using the checklist in Section 17. Run the detection regexes from Section 2 and Section 12 to verify.

```
Scan for:
- Non-SQL cells that are not spark.sql() → INELIGIBLE
- DataFrame operations → INELIGIBLE
- Python UDFs → check if convertible to SQL UDFs
- Streaming → INELIGIBLE
- ML → INELIGIBLE
- dbutils beyond widgets/secrets → INELIGIBLE
- %run → check if restructurable as workflow tasks
```

### Step 2: Archive Original Notebooks

- Copy the original notebook to an archive location (e.g., `/Archive/pre-migration/`)
- Or rely on version control (Git integration)

### Step 3: Record Baseline Delta Table Versions

For every output table written by this notebook:

```sql
-- Run BEFORE migration:
DESCRIBE HISTORY <catalog>.<schema>.<table> LIMIT 1;
```

Record: table name, version number, timestamp, row count.

### Step 4: Extract SQL from PySpark spark.sql() Calls

For each Python cell:
1. Identify all `spark.sql("...")` calls
2. Extract the SQL string content
3. Handle f-string interpolation → convert to `:parameter` syntax (Section 8)
4. Handle `.format()` interpolation → convert to `:parameter` syntax
5. Handle `%s` interpolation → convert to `:parameter` syntax
6. Create one SQL cell per `spark.sql()` call

### Step 5: Create SQL Notebook Cells

1. Create a new SQL notebook (same name with `_sql` suffix or replace in-place)
2. First cell: `USE CATALOG` statement
3. Add one cell per extracted SQL statement
4. Preserve the order of operations
5. Convert all `display()` calls to direct `SELECT`
6. Convert all `print()` for counts to `SELECT COUNT(*)`

### Step 6: Apply ANSI Fixes to All SQL

Run every detection regex from Section 3 against all SQL cells. Apply fixes:

| Priority | Pattern | Fix |
|----------|---------|-----|
| 1 (Critical) | `CAST(x AS type)` on string/mixed data | `TRY_CAST(x AS type)` |
| 2 (Critical) | Division where denominator could be 0 | `TRY_DIVIDE(x, y)` |
| 3 (High) | `to_timestamp()` / `to_date()` on raw data | `try_to_timestamp()` / `try_to_date()` |
| 4 (High) | Array/map bracket access | `TRY_ELEMENT_AT()` |
| 5 (Medium) | `boolean_col = 1` | `boolean_col IS TRUE` |
| 6 (Medium) | Integer multiplication of large values | `CAST(x AS BIGINT) * y` |
| 7 (Low) | String concatenation with possible nulls | `CONCAT_WS()` or `COALESCE()` |

### Step 7: Migrate Parameters and Widgets

1. Replace all `dbutils.widgets.get("x")` references with `:x` parameter syntax
2. Replace all Python f-string table references with `IDENTIFIER()` or `USE CATALOG`
3. Add `DECLARE` statements for defaults if needed
4. Update workflow job JSON to pass parameters to the SQL task

### Step 8: Remove Unsupported Operations

| Operation | Action |
|-----------|--------|
| `CACHE TABLE` / `UNCACHE TABLE` | Remove |
| `REFRESH TABLE` | Remove |
| `MSCK REPAIR TABLE` | Remove |
| `spark.conf.set(...)` | Remove (or convert to `SET` for supported configs) |
| `SET spark.sql.ansi.enabled = false` | Remove (cannot disable in DBSQL) |
| `SET spark.sql.shuffle.partitions` | Remove |
| Global temp views | Convert to session temp views or CTEs |
| `dbfs:/` paths | Convert to Volumes or external locations |

### Step 9: Configure SQL Warehouse Task in Workflow

Update the workflow job definition:
1. Change task type from notebook task (cluster) to notebook task (SQL warehouse) or SQL task
2. Set `warehouse_id` to the serverless SQL warehouse
3. Map `base_parameters` to SQL parameter names
4. Remove `job_cluster_key` reference
5. Add `"queue": {"enabled": true}` if not already present

### Step 10: Run on DBSQL Serverless

1. Run the migrated SQL notebook manually first (via DBSQL notebook UI)
2. Fix any syntax errors or ANSI violations
3. Run the workflow task
4. Verify it completes without errors

### Step 11: Validate Against Baseline

Use the comparison queries from Section 14:
1. Schema comparison
2. Row count comparison
3. Full data diff (EXCEPT)
4. Column-level aggregate comparison
5. Null count comparison per column

### Step 12: Generate Comparison Report

Document for each output table:
- Row count: original vs migrated
- Schema: any differences
- Data differences: count of differing rows
- Root cause of any differences (ANSI fix changing null behavior, etc.)
- Approval status: PASS / FAIL / PASS WITH KNOWN DIFFERENCES

### Step 13: Run Parallel SIT (System Integration Testing)

Run both original and migrated versions in parallel for a defined period (e.g., 1 week):
- Same schedule
- Same input data
- Compare outputs daily
- Investigate any new differences
- Sign off when outputs match consistently

---

## 17. Eligibility Checklist

Run this checklist before beginning migration. Every item must be checked.

- [ ] All cells are SQL or `spark.sql()` only — no DataFrame operations
- [ ] No DataFrame API calls (`.filter()`, `.select()`, `.withColumn()`, `.join()`, `.groupBy()`, `.agg()`, `.write.`)
- [ ] No Python UDFs — or they can be converted to SQL UDFs (Section 13)
- [ ] No Scala UDFs
- [ ] No streaming operations (`readStream`, `writeStream`, `STREAMING`)
- [ ] No ML operations (MLflow, sklearn, tensorflow, spark.ml)
- [ ] No file system operations beyond Unity Catalog Volumes (no `dbfs:`, `/mnt/`, `dbutils.fs.`)
- [ ] No `dbutils` beyond `.widgets.get()` and `.secrets.get()`
- [ ] No `%run` dependencies — or they can be restructured as workflow tasks
- [ ] No `dbutils.notebook.run()` calls — or they can be restructured as workflow tasks
- [ ] No notebook-scoped library installs (`%pip install`)
- [ ] No RDD operations (`sc.`, `.rdd.`, `.rdd`)
- [ ] No multithreading / parallel execution in Python
- [ ] All Python variables used in SQL can be converted to SQL parameters
- [ ] No complex Python control flow (loops, conditionals) that cannot be expressed in SQL

**If any item fails:** Route to Path C (Serverless General Compute) instead.

---

## 18. Documentation Links

| Resource | URL |
|----------|-----|
| DBSQL Serverless Documentation | https://docs.databricks.com/en/compute/sql-warehouse/serverless.html |
| SQL Language Reference for DBSQL | https://docs.databricks.com/en/sql/language-manual/index.html |
| SQL Warehouse Configuration | https://docs.databricks.com/en/compute/sql-warehouse/create.html |
| SQL Task in Workflows | https://docs.databricks.com/en/jobs/sql-task.html |
| DBSQL Parameter Widget Syntax | https://docs.databricks.com/en/sql/user/queries/query-parameters.html |
| ANSI Compliance in Databricks SQL | https://docs.databricks.com/en/sql/language-manual/sql-ref-ansi-compliance.html |
| SQL UDF Reference | https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html |
| TRY_CAST Function | https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html |
| TRY_DIVIDE Function | https://docs.databricks.com/en/sql/language-manual/functions/try_divide.html |
| TRY_ELEMENT_AT Function | https://docs.databricks.com/en/sql/language-manual/functions/try_element_at.html |
| IDENTIFIER Clause | https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html |
| Unity Catalog Volumes | https://docs.databricks.com/en/connect/unity-catalog/volumes.html |
| Predictive Optimization | https://docs.databricks.com/en/optimizations/predictive-optimization.html |
| DBSQL Query Profile | https://docs.databricks.com/en/sql/user/queries/query-profile.html |
| Photon Runtime | https://docs.databricks.com/en/compute/photon.html |
