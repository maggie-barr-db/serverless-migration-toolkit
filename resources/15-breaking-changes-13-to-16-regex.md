# Breaking Changes 13.3 → 16.4: Regex Scan Patterns

Structured regex patterns for automated code scanning. Organized by severity. Designed to be loaded by Genie Code and run against notebook source code.

**Usage:** For each notebook, run all patterns in the relevant severity tier. Report matches with cell/line numbers.

---

## CRITICAL — Will cause runtime failures on 16.4 or serverless

These patterns will throw exceptions that crash the job.

### C1: CAST to numeric types (ANSI throws on invalid input)

```regex
# SQL
(?i)\bCAST\s*\(\s*\S+\s+AS\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC)\s*\)

# PySpark
\.cast\s*\(\s*["'](?:int|integer|bigint|smallint|tinyint|float|double|decimal)["']\s*\)
\.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType)\s*\(\s*\)\s*\)

# Scala
\.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType)\s*\)
```

**Fix:** Replace with `TRY_CAST` (SQL) or add `when/otherwise` guard (PySpark/Scala)

### C2: CAST to DATE or TIMESTAMP (ANSI throws on invalid date strings)

```regex
(?i)\bCAST\s*\(\s*\S+\s+AS\s+(?:DATE|TIMESTAMP)\s*\)
\.cast\s*\(\s*["'](?:date|timestamp)["']\s*\)
```

**Fix:** Replace with `TRY_CAST`

### C3: to_date / to_timestamp on potentially invalid data

```regex
(?i)\bto_date\s*\(
(?i)\bto_timestamp\s*\(
F\.to_date\s*\(
F\.to_timestamp\s*\(
```

**Fix:** Replace with `try_to_date` / `try_to_timestamp`

### C4: Division (ANSI throws ArithmeticException on divide-by-zero)

```regex
# SQL division
(?i)\b\w+\s*/\s*\w+

# PySpark/Scala column division (more targeted)
F\.col\([^)]+\)\s*/\s*F\.col
\$"[^"]+"\s*/\s*\$"
```

**Fix:** `TRY_DIVIDE(a, b)` or `CASE WHEN b = 0 THEN NULL ELSE a/b END`

### C5: BOOLEAN compared to INT

```regex
# SQL patterns
(?i)\b\w+\s*=\s*1\b(?![\.\d])
(?i)\b\w+\s*=\s*0\b(?![\.\d])

# Requires cross-reference with schema to confirm column is BOOLEAN
```

**Fix:** Replace `col = 1` with `col IS TRUE`, `col = 0` with `col IS NOT TRUE`

### C6: Array bracket access (ANSI throws on out-of-bounds)

```regex
# SQL
\w+\[\d+\]

# PySpark
\.getItem\s*\(\d+\)
F\.split\s*\([^)]+\)\s*\[\d+\]
F\.split\s*\([^)]+\)\.getItem\s*\(\d+\)
```

**Fix:** `TRY_ELEMENT_AT(array, index+1)` (1-indexed) or bounds check

### C7: Map bracket access (ANSI throws on missing key)

```regex
# SQL
\w+\['[^']+'\]
\w+\["[^"]+"\]

# PySpark
\.getItem\s*\(\s*["']
```

**Fix:** `TRY_ELEMENT_AT(map, 'key')` or `map_contains_key` guard

### C8: Unsupported Spark configs (serverless throws CONFIG_NOT_AVAILABLE)

```regex
# Configs known to fail on serverless (from the customer's issue log)
spark\.databricks\.delta\.retentionDurationCheck\.enabled
spark\.databricks\.delta\.schema\.autoMerge\.enabled
spark\.databricks\.delta\.optimizeWrite\.enabled
spark\.databricks\.delta\.autoCompact\.enabled
spark\.sql\.broadcastTimeout
spark\.sql\.streaming\.stateStore\.stateSchemaCheck
spark\.sql\.caseSensitive
spark\.executor\.
spark\.driver\.extra
spark\.dynamicAllocation\.
spark\.shuffle\.service\.
spark\.serializer
spark\.sql\.warehouse\.dir
spark\.hadoop\.
fs\.azure\.
```

**Fix:** See config reference (05) for per-config replacement

### C9: REFRESH TABLE (not supported on serverless)

```regex
(?i)\bREFRESH\s+TABLE\b
```

**Fix:** Remove. Serverless auto-refreshes.

### C10: MSCK REPAIR TABLE (not supported on serverless)

```regex
(?i)\bMSCK\s+REPAIR\s+TABLE\b
```

**Fix:** Remove.

---

## HIGH — Likely to cause failures or incorrect behavior

### H1: .persist() / .cache() (not supported on serverless)

```regex
\.persist\s*\(
\.cache\s*\(
\.unpersist\s*\(
(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b
(?i)\bUNCACHE\s+TABLE\b
```

**Fix:** Remove. Serverless auto-manages caching.

### H2: RDD APIs (not available on serverless)

```regex
sc\.textFile\s*\(
sc\.parallelize\s*\(
sc\.wholeTextFiles\s*\(
\.rdd\.
rdd\.map\s*\(
rdd\.filter\s*\(
rdd\.flatMap\s*\(
rdd\.reduce\s*\(
```

**Fix:** Rewrite as DataFrame operations

### H3: Environment variables (not available on serverless)

```regex
os\.environ\.get\s*\(
os\.environ\[
sys\.argv
```

**Fix:** Use `dbutils.widgets.get()`

### H4: dbutils.library (deprecated, fails on serverless)

```regex
dbutils\.library\.install
dbutils\.library\.restartPython
```

**Fix:** Move to requirements.txt

### H5: CREATE MATERIALIZED VIEW (requires SQL Warehouse)

```regex
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b
```

**Fix:** Must use SQL Warehouse, not serverless general compute

### H6: Global temp views

```regex
(?i)createOrReplaceGlobalTempView
(?i)global_temp\.
```

**Fix:** Convert to session-scoped temp views or persistent tables

### H7: com.crealytics.spark.excel

```regex
com\.crealytics\.spark\.excel
```

**Fix:** Replace with pandas + openpyxl pattern (see package guide 06)

### H8: spark.sql.ansi.enabled set to false

```regex
(?i)spark\.sql\.ansi\.enabled.*false
(?i)SET\s+spark\.sql\.ansi\.enabled\s*=\s*false
```

**Fix:** Remove the setting. Fix code to be ANSI-safe instead. This config is a no-op on serverless.

### H9: f.lit() wrapping format strings (real the customer issue)

```regex
# Wrong: f.lit("format") as format argument
F\.to_date\s*\([^,]+,\s*F\.lit\s*\(
F\.to_timestamp\s*\([^,]+,\s*F\.lit\s*\(
f\.to_date\s*\([^,]+,\s*f\.lit\s*\(
f\.to_timestamp\s*\([^,]+,\s*f\.lit\s*\(
```

**Fix:** Remove `F.lit()` wrapper. Format strings must be plain strings, not Column expressions.

### H10: Integer overflow risk (ANSI throws on overflow)

```regex
# Multiplication of INT columns that could overflow
(?i)\bSUM\s*\(.*?\*
(?i)\bCAST\s*\(.*?\bAS\s+INT\s*\)\s*\*
```

**Fix:** Cast to BIGINT before multiplication

---

## MEDIUM — May cause issues depending on data

### M1: SELECT * (execution plan differences on serverless)

```regex
(?i)\bSELECT\s+\*\s+FROM\b
```

**Note:** On serverless, `SELECT *` with row filters can cause `MISSING_ATTRIBUTES` errors. Use explicit column lists.

### M2: ThreadPoolExecutor / concurrent.futures

```regex
concurrent\.futures
ThreadPoolExecutor
multiprocessing\.Pool
multiprocessing\.Process
```

**Note:** Performance typically degrades on serverless. Warn developer.

### M3: /tmp file path

```regex
["']/tmp/[^"']*["']
open\s*\(\s*["']/tmp/
```

**Fix:** Use `/local_disk0/tmp/` instead

### M4: spark.createDataFrame without schema

```regex
spark\.createDataFrame\s*\([^,)]+\)(?!\s*,)
```

**Note:** Schema inference may differ on serverless. Provide explicit StructType for complex/nested data.

### M5: Multiple chained withColumn (RecursionError risk)

```regex
# Heuristic: count .withColumn occurrences in a cell
# Flag if >20 in a single cell
\.withColumn\s*\(
```

**Fix:** Replace with single `.withColumns()` call

### M6: String concatenation with || (null propagation in ANSI)

```regex
(?i)\|\|(?!\|)
```

**Fix:** Use `CONCAT_WS` or `COALESCE` for null safety

### M7: ROUND with negative scale

```regex
(?i)\bROUND\s*\([^,]+,\s*-\d+\)
```

**Note:** Verify behavior manually

### M8: CSV write null handling

```regex
\.write.*\.format\s*\(\s*["']csv["']\)
\.write\.csv\s*\(
```

**Note:** Serverless may handle null values differently in CSV output. Use `.option("nullValue","").option("quote","")` if needed.

---

## LOW — Performance or informational

### L1: .count() for existence checks

```regex
\.count\s*\(\s*\)\s*>\s*0
\.count\s*\(\s*\)\s*==\s*0
\.count\s*\(\s*\)\s*!=\s*0
if\s+.*\.count\s*\(\s*\)
```

**Fix:** Replace with `.first() is not None` or `.isEmpty()`

### L2: ZORDER (Liquid Clustering available)

```regex
(?i)\bZORDER\s+BY\b
```

**Note:** Liquid Clustering is available in 16.4+. Recommend for new tables. Don't change existing in first pass.

### L3: Manual shuffle partitions

```regex
spark\.sql\.shuffle\.partitions
\.repartition\s*\(\d+\)
\.coalesce\s*\(\d+\)
```

**Note:** Serverless auto-tunes. Remove unless partition count matters for downstream consumers.

### L4: PartitionBy on write

```regex
\.partitionBy\s*\(
```

**Note:** Evaluate if needed. Liquid Clustering is often better. Causes small file issues.

### L5: display(df.count())

```regex
display\s*\(.*\.count\s*\(\s*\)
print\s*\(.*\.count\s*\(\s*\)
```

**Note:** Unnecessary full table scan for logging. Remove or use approximation.

---

## Summary Table

| Severity | Count | Category |
|----------|-------|----------|
| CRITICAL | 10 | ANSI casts, division, boolean, array/map access, unsupported configs, REFRESH/MSCK |
| HIGH | 10 | persist/cache, RDD, env vars, libraries, materialized views, global temp views, format string bug |
| MEDIUM | 8 | SELECT *, threading, /tmp path, schema inference, chained withColumn, null concatenation |
| LOW | 5 | count anti-patterns, ZORDER, manual partitioning, PartitionBy |

**Total: 33 scan patterns**

---

## Machine-Readable Pattern List

For automated scanning tools, all patterns in a flat structure:

```json
{
  "patterns": [
    {"id": "C1", "severity": "CRITICAL", "name": "CAST to numeric", "regex": "(?i)\\bCAST\\s*\\(\\s*\\S+\\s+AS\\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC)\\s*\\)", "fix_ref": "07-ansi-compliance-reference.md#pattern-1"},
    {"id": "C2", "severity": "CRITICAL", "name": "CAST to date/timestamp", "regex": "(?i)\\bCAST\\s*\\(\\s*\\S+\\s+AS\\s+(?:DATE|TIMESTAMP)\\s*\\)", "fix_ref": "07-ansi-compliance-reference.md#pattern-1"},
    {"id": "C3", "severity": "CRITICAL", "name": "to_date/to_timestamp", "regex": "(?i)\\bto_(?:date|timestamp)\\s*\\(", "fix_ref": "07-ansi-compliance-reference.md#pattern-8"},
    {"id": "C4", "severity": "CRITICAL", "name": "Division", "regex": "F\\.col\\([^)]+\\)\\s*/\\s*F\\.col", "fix_ref": "07-ansi-compliance-reference.md#pattern-2"},
    {"id": "C5", "severity": "CRITICAL", "name": "Boolean = INT", "regex": "(?i)\\b\\w+\\s*=\\s*[01]\\b(?![\\d\\.])", "fix_ref": "07-ansi-compliance-reference.md#pattern-6"},
    {"id": "C6", "severity": "CRITICAL", "name": "Array access", "regex": "\\w+\\[\\d+\\]", "fix_ref": "07-ansi-compliance-reference.md#pattern-4"},
    {"id": "C7", "severity": "CRITICAL", "name": "Map access", "regex": "\\w+\\['[^']+'\\]", "fix_ref": "07-ansi-compliance-reference.md#pattern-5"},
    {"id": "C8", "severity": "CRITICAL", "name": "Unsupported config", "regex": "spark\\.databricks\\.delta\\.retentionDurationCheck\\.enabled|spark\\.sql\\.broadcastTimeout|spark\\.sql\\.caseSensitive", "fix_ref": "05-spark-config-classic-to-serverless.md"},
    {"id": "C9", "severity": "CRITICAL", "name": "REFRESH TABLE", "regex": "(?i)\\bREFRESH\\s+TABLE\\b", "fix_ref": "remove"},
    {"id": "C10", "severity": "CRITICAL", "name": "MSCK REPAIR TABLE", "regex": "(?i)\\bMSCK\\s+REPAIR\\s+TABLE\\b", "fix_ref": "remove"}
  ]
}
```
