# Data Compatibility Checks — Pre-Migration Table Analysis

Pre-migration data-level checks that must be run against every output table for every job being migrated. These checks identify data-level risks **before** the runtime or compute changes, so that issues are caught and remediated proactively rather than discovered as silent data corruption in production.

**Context:** Molina Healthcare operates regulated healthcare pipelines (claims, eligibility, encounters, pharmacy). Silent data changes — a date that shifts, a boolean that flips, a row that drops — are unacceptable. CMS reporting errors carry financial penalties. These checks exist because the Bright Health DBR 7.3 EOL migration surfaced real data issues that would have gone undetected without pre-migration analysis.

**Relationship to other toolkit components:**
- These checks run **BEFORE** migration. They inform migration planning and identify tables needing pre-work.
- The `conversion_validator` skill (resource 08) runs **AFTER** migration to verify output equivalence.
- The `07-ansi-compliance-reference` covers code-level ANSI issues. This document covers data-level issues that interact with ANSI mode and DBR changes.

**Design principle:** Every check must be runnable as-is in a Databricks notebook cell. No check should modify data — these are read-only diagnostics.

---

## 1. Purpose

When a Databricks job moves from DBR 13.3 (classic compute) to DBR 16.4 or serverless, the runtime changes beneath the data. Even if the code is identical, the new runtime may:

- Auto-upgrade Delta protocol versions on the next write (irreversible)
- Reject previously-tolerated invalid data under ANSI mode
- Handle datetime edge cases differently (Proleptic Gregorian calendar)
- Interpret boolean comparisons strictly
- Read old parquet files with different rebase behavior
- Interact with table properties (like row tracking) in unexpected ways

**Every output table for every job must be checked before migration begins.** The checks produce a per-table risk classification (HIGH / MEDIUM / LOW) that feeds into the assessment skill's migration plan.

### What This Document Covers

| Section | What It Checks | Why It Matters |
|---------|---------------|----------------|
| Table Type Classification | Managed vs external | External tables miss auto-optimization features |
| Protocol Version | Delta reader/writer protocol | Protocol upgrades are irreversible |
| Table Properties | Row tracking, retention, auto-optimize | Properties can conflict with code or change behavior |
| Datetime Analysis | Invalid dates, pre-1582 dates, string dates | ANSI mode rejects invalid dates; calendar edge cases exist |
| Boolean Detection | BOOLEAN columns compared to INT | ANSI mode rejects BOOLEAN = INT comparisons |
| Parquet Metadata | Writer version, rebase mode needs | Old Spark 2.x files need special datetime handling |
| Partition Scheme | Partitioning, ZORDER, AQE interaction | Performance regressions from layout changes |
| File Layout Health | File count, file sizes | Small files cause catastrophic performance on external tables |
| Risk Classification | Composite rating per table | Prioritize which tables need pre-work |

---

## 2. Table Type Classification

Managed and external tables behave differently in Unity Catalog. External tables do not benefit from Predictive Optimization (auto-OPTIMIZE, auto-VACUUM, auto-ANALYZE). They also may contain parquet files written by legacy systems outside of Delta, or by older Spark versions.

### Why This Matters for Migration

| Feature | Managed Table | External Table |
|---------|--------------|----------------|
| Predictive Optimization | Automatic | Not available |
| OPTIMIZE | Auto or manual | Manual only |
| VACUUM | Auto or manual | Manual only |
| ANALYZE TABLE | Auto or manual | Manual only |
| File layout quality | Generally good | Often degraded |
| Protocol upgrades | Controlled | May be uncontrolled |

### Detection SQL

```sql
-- Classify a single table
DESCRIBE DETAIL catalog_name.schema_name.table_name;
```

Key columns in the output:

| Column | What to Check |
|--------|---------------|
| `format` | Should be `delta` — non-delta tables need separate handling |
| `location` | If starts with `dbfs:/` or `s3://` or `abfss://` — indicates external storage |
| `isManaged` | Direct boolean — but only present in some DBR versions |
| `provider` | Should be `delta` |

### Batch Classification Query

```sql
-- Classify all tables in a schema
-- Run this for each schema that contains output tables for the job being migrated
WITH table_details AS (
  SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    t.data_source_format,
    t.storage_path
  FROM system.information_schema.tables t
  WHERE t.table_catalog = '${catalog}'
    AND t.table_schema = '${schema}'
    AND t.table_type IN ('MANAGED', 'EXTERNAL')
    AND t.data_source_format = 'DELTA'
)
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type,
  storage_path,
  CASE
    WHEN table_type = 'EXTERNAL' THEN 'REQUIRES_MANUAL_MAINTENANCE'
    ELSE 'AUTO_OPTIMIZED'
  END AS optimization_status
FROM table_details
ORDER BY table_type DESC, table_name;
```

### External Table Action Items

For every external table identified:

1. **OPTIMIZE** must be scheduled manually before and after migration
2. **VACUUM** must be run manually (check retention settings first)
3. **ANALYZE TABLE COMPUTE STATISTICS** must be run to ensure the query optimizer has current stats
4. File layout health check (Section 9) is especially critical

---

## 3. Delta Protocol Version Check

Delta Lake protocol versions control which features are available and which readers/writers can access the table. When a job migrates to a newer DBR, the first write operation may **auto-upgrade the protocol version**. This upgrade is **irreversible** — once upgraded, older DBR versions and older external readers cannot read the table.

### Protocol Version Reference

| Reader Version | Writer Version | Key Features Enabled | Minimum DBR |
|:-:|:-:|---|---|
| 1 | 1 | Basic Delta (parquet + transaction log) | Any |
| 1 | 2 | Append-only tables (column `appendOnly`) | 4.2+ |
| 1 | 3 | Check constraints, generated columns | 8.1+ |
| 1 | 4 | Change Data Feed (CDF) | 8.4+ |
| 1 | 5 | Column mapping (rename/drop columns) | 10.2+ |
| 2 | 5 | Column mapping (reader-side) | 10.2+ |
| 2 | 6 | Row tracking, identity columns | 13.3+ |
| 2 | 7 | Managed commits (coordinated commits) | 14.0+ |
| 3 | 7 | Deletion vectors, v2 checkpoints | 14.0+ |

### Risk: Auto-Upgrade on Write

DBR 16.4 and serverless may auto-upgrade protocol when:

- **Row tracking** is enabled (requires writer version 6)
- **Deletion vectors** are enabled (requires reader version 3, writer version 7)
- **Liquid clustering** is used (requires reader version 3, writer version 7)
- A table property is set that requires a higher protocol version

**After upgrade, any system still on DBR 13.3 or below that reads this table will fail.**

### Detection SQL

```sql
-- Check protocol versions for a single table
SELECT
  '${catalog}.${schema}.${table}' AS table_name,
  d.minReaderVersion,
  d.minWriterVersion
FROM (DESCRIBE DETAIL ${catalog}.${schema}.${table}) d;
```

### Batch Protocol Check

```sql
-- Check protocol versions across all tables in a schema
-- This uses INFORMATION_SCHEMA which is available in Unity Catalog
-- For the actual protocol versions, you must check each table individually

-- Step 1: Get table list
CREATE OR REPLACE TEMP VIEW target_tables AS
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE table_catalog = '${catalog}'
  AND table_schema = '${schema}'
  AND data_source_format = 'DELTA';

-- Step 2: For each table, run this (must be done in a loop — see notebook template in Section 11)
DESCRIBE DETAIL ${catalog}.${schema}.${table_name};
```

### Protocol Risk Assessment

```python
# Python helper to assess protocol risk for a list of tables
def check_protocol_risk(spark, tables: list[str]) -> list[dict]:
    """
    Check Delta protocol versions for a list of fully-qualified table names.
    Returns risk assessment per table.
    """
    results = []
    for table_fqn in tables:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
            reader_v = detail["minReaderVersion"]
            writer_v = detail["minWriterVersion"]

            risk = "LOW"
            notes = []

            # Tables at protocol (1,2) or (1,3) will likely be upgraded on first write
            if writer_v < 4:
                risk = "MEDIUM"
                notes.append(f"Writer v{writer_v} may auto-upgrade on new DBR write")

            # Tables already at high protocol — check if readers exist on older DBR
            if reader_v >= 3:
                notes.append("Reader v3 — requires DBR 14.0+ for all readers")

            # Tables at (1,1) are the most likely to get upgraded
            if reader_v == 1 and writer_v <= 2:
                risk = "HIGH"
                notes.append("Very old protocol — high likelihood of auto-upgrade on new DBR")

            results.append({
                "table": table_fqn,
                "minReaderVersion": reader_v,
                "minWriterVersion": writer_v,
                "risk": risk,
                "notes": "; ".join(notes) if notes else "Current protocol is compatible"
            })
        except Exception as e:
            results.append({
                "table": table_fqn,
                "minReaderVersion": None,
                "minWriterVersion": None,
                "risk": "ERROR",
                "notes": str(e)
            })
    return results
```

### Mitigation

If protocol upgrade risk is HIGH:

1. **Before migration:** Record current protocol versions in the manifest
2. **During migration:** Set `spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false` to prevent auto-compaction from triggering protocol upgrades (note: not available on serverless — must test in classic first)
3. **After migration:** Verify protocol versions haven't changed unexpectedly
4. **If shared tables:** Ensure all downstream consumers are on a DBR that supports the new protocol before allowing the upgraded writer to run

---

## 4. Table Properties Audit

Table properties control Delta behavior for optimization, retention, row tracking, and more. Some properties interact with migration in ways that cause failures or silent behavioral changes.

### Detection SQL — Full Properties Dump

```sql
-- Get all properties for a table
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name};
```

### Key Properties to Check

#### 4a. Row Tracking (Issue #14)

Row tracking adds a hidden `_metadata` column to the table. If any notebook code references `_metadata` (which is a common column name in healthcare data for audit trails), it will conflict with the Delta row tracking metadata column.

**This was a real issue at Molina (issue #14 in their migration log).** A notebook had a column called `_metadata` in a claims table that conflicted with Delta's row tracking `_metadata` struct.

```sql
-- Check if row tracking is enabled
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.enableRowTracking');

-- If the above errors with "not found", row tracking is not set.
-- If it returns 'true', this table has row tracking enabled.
```

**Cross-reference with code:** Search all notebooks for the job for references to `_metadata`:

```python
# In the assessment skill, scan for _metadata references
import re

metadata_pattern = re.compile(
    r"""(?:['"]_metadata['"]|"""          # String literal '_metadata'
    r"""col\s*\(\s*['"]_metadata['"]|"""  # F.col("_metadata")
    r"""\._metadata\b|"""                 # df._metadata
    r"""`_metadata`|"""                   # SQL backtick-quoted
    r"""_metadata\s+(?:STRING|STRUCT))""", # DDL type definition
    re.IGNORECASE
)
```

**Risk classification:** If row tracking is enabled AND code references `_metadata` --> **HIGH** risk.

#### 4b. Retention Duration

```sql
-- Check deletion retention (affects VACUUM)
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.deletedFileRetentionDuration');

-- Check log retention (affects time travel)
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.logRetentionDuration');
```

| Property | Default | Risk |
|----------|---------|------|
| `delta.deletedFileRetentionDuration` | `interval 7 days` | Short retention + VACUUM = lost time travel |
| `delta.logRetentionDuration` | `interval 30 days` | Short retention = limited rollback window |

**Migration concern:** If retention is very short (e.g., `interval 1 day`), running VACUUM before migration could destroy the ability to compare pre/post migration data via time travel.

#### 4c. Auto-Optimization Settings

```sql
-- Check auto-optimization
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.autoOptimize.optimizeWrite');
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.autoOptimize.autoCompact');
```

**Migration concern:** On serverless, auto-optimization behavior may differ. Tables with explicit `autoOptimize.optimizeWrite = false` may have been set that way intentionally (e.g., for streaming ingest patterns). Migrating to serverless where optimizeWrite is the default can change write behavior.

#### 4d. Partition Columns

```sql
-- Check partition columns
DESCRIBE DETAIL ${catalog}.${schema}.${table_name};
-- Look at the 'partitionColumns' field in the output
```

### Batch Properties Audit

```python
def audit_table_properties(spark, tables: list[str]) -> list[dict]:
    """
    Audit key table properties for migration risk.
    """
    critical_properties = [
        "delta.enableRowTracking",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.autoOptimize.optimizeWrite",
        "delta.autoOptimize.autoCompact",
        "delta.enableDeletionVectors",
        "delta.columnMapping.mode",
        "delta.minReaderVersion",
        "delta.minWriterVersion",
        "delta.enableChangeDataFeed",
    ]

    results = []
    for table_fqn in tables:
        try:
            props_df = spark.sql(f"SHOW TBLPROPERTIES {table_fqn}")
            props = {row["key"]: row["value"] for row in props_df.collect()}

            flags = []

            # Row tracking check
            if props.get("delta.enableRowTracking", "false").lower() == "true":
                flags.append("ROW_TRACKING_ENABLED — check for _metadata column conflicts")

            # Short retention
            retention = props.get("delta.deletedFileRetentionDuration", "interval 7 days")
            if "1 day" in retention or "hours" in retention:
                flags.append(f"SHORT_RETENTION ({retention}) — VACUUM may destroy time travel data")

            # Deletion vectors
            if props.get("delta.enableDeletionVectors", "false").lower() == "true":
                flags.append("DELETION_VECTORS_ENABLED — requires reader v3")

            # Column mapping
            cm_mode = props.get("delta.columnMapping.mode", "none")
            if cm_mode != "none":
                flags.append(f"COLUMN_MAPPING={cm_mode} — verify column references in code")

            results.append({
                "table": table_fqn,
                "properties": {k: v for k, v in props.items() if k in critical_properties},
                "flags": flags,
                "risk": "HIGH" if any("ROW_TRACKING" in f or "SHORT_RETENTION" in f for f in flags) else "LOW"
            })
        except Exception as e:
            results.append({
                "table": table_fqn,
                "properties": {},
                "flags": [f"ERROR: {str(e)}"],
                "risk": "ERROR"
            })
    return results
```

---

## 5. Datetime / Timestamp Column Analysis

Datetime handling is one of the highest-risk areas in a DBR migration. Three distinct issues arise:

1. **ANSI mode rejects invalid date strings** — values like `'00000000'`, `'9999-99-99'`, `'2299-12-34'` that silently became `null` under ANSI-off will throw `DateTimeException` under ANSI-on.
2. **Pre-1582 dates** — the Proleptic Gregorian calendar change in Spark 3.0+ means dates before October 15, 1582 may shift by several days when read from old parquet files.
3. **String-typed date columns** — dates stored as strings that are cast in notebook code will fail under ANSI mode if any value is invalid.

**All three of these have been observed at Molina.** The `'00000000'` date placeholder is used extensively in legacy claims data from Bright Health.

### 5a. Identify Date/Timestamp Columns

```sql
-- Find all date and timestamp columns in a table
SELECT
  column_name,
  data_type,
  is_nullable
FROM system.information_schema.columns
WHERE table_catalog = '${catalog}'
  AND table_schema = '${schema}'
  AND table_name = '${table}'
  AND data_type IN ('date', 'timestamp', 'timestamp_ntz')
ORDER BY ordinal_position;
```

### 5b. Identify String Columns That May Contain Dates

```sql
-- Find string columns with date-like names
-- These are candidates for implicit date casting in code
SELECT
  column_name,
  data_type
FROM system.information_schema.columns
WHERE table_catalog = '${catalog}'
  AND table_schema = '${schema}'
  AND table_name = '${table}'
  AND data_type = 'string'
  AND (
    column_name LIKE '%date%'
    OR column_name LIKE '%dt%'
    OR column_name LIKE '%_dt'
    OR column_name LIKE '%time%'
    OR column_name LIKE '%timestamp%'
    OR column_name LIKE '%dob%'
    OR column_name LIKE '%birth%'
    OR column_name LIKE '%admit%'
    OR column_name LIKE '%discharge%'
    OR column_name LIKE '%effective%'
    OR column_name LIKE '%expir%'
    OR column_name LIKE '%service%'
    OR column_name LIKE '%incur%'
    OR column_name LIKE '%paid%'
    OR column_name LIKE '%received%'
    OR column_name LIKE '%created%'
    OR column_name LIKE '%modified%'
    OR column_name LIKE '%updated%'
  )
ORDER BY ordinal_position;
```

### 5c. Sample Date Columns for Invalid Values

```sql
-- Check a DATE column for values outside valid range or with edge-case issues
-- Replace ${date_col} with the actual column name
SELECT
  '${table}' AS table_name,
  '${date_col}' AS column_name,
  COUNT(*) AS total_rows,
  COUNT(${date_col}) AS non_null_rows,
  SUM(CASE WHEN ${date_col} IS NULL THEN 1 ELSE 0 END) AS null_count,
  MIN(${date_col}) AS min_date,
  MAX(${date_col}) AS max_date,
  SUM(CASE WHEN ${date_col} < DATE'1582-10-15' THEN 1 ELSE 0 END) AS pre_1582_count,
  SUM(CASE WHEN ${date_col} > DATE'2100-01-01' THEN 1 ELSE 0 END) AS far_future_count,
  SUM(CASE WHEN ${date_col} = DATE'1900-01-01' THEN 1 ELSE 0 END) AS sentinel_1900_count,
  SUM(CASE WHEN ${date_col} = DATE'9999-12-31' THEN 1 ELSE 0 END) AS sentinel_9999_count
FROM ${catalog}.${schema}.${table};
```

### 5d. Sample String Columns for Invalid Date Patterns

This is the most critical check. These are real patterns found in Molina data:

```sql
-- Check a STRING column that may contain date values
-- This catches the exact invalid patterns found in Molina's Bright Health data
SELECT
  '${table}' AS table_name,
  '${string_date_col}' AS column_name,
  COUNT(*) AS total_rows,

  -- Empty / null
  SUM(CASE WHEN ${string_date_col} IS NULL THEN 1 ELSE 0 END) AS null_count,
  SUM(CASE WHEN TRIM(${string_date_col}) = '' THEN 1 ELSE 0 END) AS empty_string_count,

  -- Known bad patterns (all found in Molina data)
  SUM(CASE WHEN ${string_date_col} = '00000000' THEN 1 ELSE 0 END) AS zeros_count,
  SUM(CASE WHEN ${string_date_col} = '99999999' THEN 1 ELSE 0 END) AS nines_count,
  SUM(CASE WHEN ${string_date_col} RLIKE '^\\d{4}-\\d{2}-\\d{2}$'
            AND TRY_TO_DATE(${string_date_col}, 'yyyy-MM-dd') IS NULL
            THEN 1 ELSE 0 END) AS invalid_yyyy_mm_dd_count,
  SUM(CASE WHEN ${string_date_col} RLIKE '^\\d{8}$'
            AND ${string_date_col} != '00000000'
            AND TRY_TO_DATE(${string_date_col}, 'yyyyMMdd') IS NULL
            THEN 1 ELSE 0 END) AS invalid_yyyymmdd_count,

  -- Dates that parse but are suspicious
  SUM(CASE WHEN TRY_TO_DATE(${string_date_col}, 'yyyy-MM-dd') < DATE'1582-10-15'
            THEN 1 ELSE 0 END) AS pre_1582_parseable,
  SUM(CASE WHEN TRY_TO_DATE(${string_date_col}, 'yyyy-MM-dd') > DATE'2100-01-01'
            THEN 1 ELSE 0 END) AS far_future_parseable,

  -- Sample distinct bad values (for manual review)
  COLLECT_SET(
    CASE WHEN TRY_TO_DATE(${string_date_col}, 'yyyy-MM-dd') IS NULL
          AND TRIM(COALESCE(${string_date_col}, '')) != ''
         THEN ${string_date_col}
    END
  ) AS sample_unparseable_values

FROM ${catalog}.${schema}.${table}
WHERE ${string_date_col} IS NOT NULL;
```

### 5e. Comprehensive String-to-Date Safety Check

```python
def check_string_date_columns(spark, table_fqn: str) -> list[dict]:
    """
    Find all string columns with date-like names and check for invalid values.
    Returns a list of findings per column.
    """
    catalog, schema, table = table_fqn.split(".")

    # Get string columns with date-like names
    date_name_patterns = [
        "%date%", "%_dt", "%dt_%", "%time%", "%dob%", "%birth%",
        "%admit%", "%discharge%", "%effective%", "%expir%",
        "%service%", "%incur%", "%paid%", "%received%",
        "%created%", "%modified%", "%updated%"
    ]

    like_clauses = " OR ".join([f"LOWER(column_name) LIKE '{p}'" for p in date_name_patterns])

    string_date_cols = spark.sql(f"""
        SELECT column_name
        FROM system.information_schema.columns
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
          AND data_type = 'string'
          AND ({like_clauses})
    """).collect()

    findings = []
    for row in string_date_cols:
        col = row["column_name"]
        try:
            result = spark.sql(f"""
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN TRIM(COALESCE(`{col}`, '')) = '' THEN 1 ELSE 0 END) AS empty_or_null,
                  SUM(CASE WHEN `{col}` = '00000000' THEN 1 ELSE 0 END) AS zeros,
                  SUM(CASE WHEN `{col}` RLIKE '^[0-9]{{8}}$'
                            AND TRY_TO_DATE(`{col}`, 'yyyyMMdd') IS NULL
                            AND `{col}` != '00000000'
                       THEN 1 ELSE 0 END) AS invalid_8digit,
                  SUM(CASE WHEN `{col}` RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            AND TRY_TO_DATE(`{col}`, 'yyyy-MM-dd') IS NULL
                       THEN 1 ELSE 0 END) AS invalid_iso
                FROM {table_fqn}
            """).collect()[0]

            risk = "LOW"
            notes = []

            if result["zeros"] > 0:
                risk = "HIGH"
                notes.append(f"{result['zeros']} rows with '00000000' — will fail CAST to DATE under ANSI")
            if result["invalid_8digit"] > 0:
                risk = "HIGH"
                notes.append(f"{result['invalid_8digit']} rows with invalid 8-digit date strings")
            if result["invalid_iso"] > 0:
                risk = "HIGH"
                notes.append(f"{result['invalid_iso']} rows with invalid ISO date strings")
            if result["empty_or_null"] > 0:
                notes.append(f"{result['empty_or_null']} empty/null values — safe if code uses TRY_CAST")

            findings.append({
                "column": col,
                "total_rows": result["total"],
                "risk": risk,
                "notes": "; ".join(notes) if notes else "Clean — all values parseable"
            })
        except Exception as e:
            findings.append({
                "column": col,
                "total_rows": None,
                "risk": "ERROR",
                "notes": str(e)
            })

    return findings
```

---

## 6. BOOLEAN Column Detection

ANSI mode strictly enforces type safety for boolean comparisons. Code like `WHERE active_flag = 1` or `CASE WHEN boolean_col = 0` that worked on DBR 13.3 (ANSI off) will throw an error on DBR 16.4 / serverless (ANSI on, mandatory).

### The Problem

```sql
-- This works on DBR 13.3 with ANSI OFF:
SELECT * FROM members WHERE is_active = 1;
-- Spark implicitly converts: BOOLEAN 'true' == INT 1 → true

-- This FAILS on DBR 16.4 / Serverless with ANSI ON:
-- Error: cannot resolve '(is_active = 1)' due to data type mismatch
```

### 6a. Identify BOOLEAN Columns

```sql
-- Find all BOOLEAN columns in a table
SELECT
  column_name,
  data_type,
  is_nullable
FROM system.information_schema.columns
WHERE table_catalog = '${catalog}'
  AND table_schema = '${schema}'
  AND table_name = '${table}'
  AND data_type = 'boolean'
ORDER BY ordinal_position;
```

### 6b. Batch BOOLEAN Detection Across Schema

```sql
-- Find all BOOLEAN columns across all tables in a schema
-- These columns need cross-referencing with notebook code
SELECT
  table_name,
  column_name
FROM system.information_schema.columns
WHERE table_catalog = '${catalog}'
  AND table_schema = '${schema}'
  AND data_type = 'boolean'
ORDER BY table_name, ordinal_position;
```

### 6c. Cross-Reference with Code

The data check alone is not enough — you must cross-reference BOOLEAN column names with the notebook code to find comparisons to INT literals. This is done at the code scanning layer, not the data layer, but the data check provides the input list.

```python
def find_boolean_int_risks(spark, tables: list[str], notebook_code: str) -> list[dict]:
    """
    Find BOOLEAN columns and check if they appear in INT comparisons in code.

    Parameters:
        spark: SparkSession
        tables: List of fully-qualified table names
        notebook_code: Combined text of all notebook cells for the job
    """
    import re

    # Collect all boolean column names across all tables
    boolean_columns = set()
    for table_fqn in tables:
        catalog, schema, table = table_fqn.split(".")
        cols = spark.sql(f"""
            SELECT column_name
            FROM system.information_schema.columns
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
              AND data_type = 'boolean'
        """).collect()
        for row in cols:
            boolean_columns.add(row["column_name"].lower())

    # Scan code for boolean columns compared to INT literals
    findings = []
    for col_name in boolean_columns:
        # Pattern: col_name = 0, col_name = 1, col_name == 0, col_name == 1
        # Also: col_name != 0, col_name <> 1
        patterns = [
            rf"(?i)\b{re.escape(col_name)}\b\s*(?:=|==|!=|<>)\s*[01]\b",
            rf"(?i)[`\"']{re.escape(col_name)}[`\"']\s*(?:=|==|!=|<>)\s*[01]\b",
            rf"(?i)F\.col\s*\(\s*[\"']{re.escape(col_name)}[\"']\s*\)\s*==\s*[01]",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, notebook_code)
            if matches:
                findings.append({
                    "column": col_name,
                    "pattern_found": matches[0],
                    "match_count": len(matches),
                    "risk": "MEDIUM",
                    "fix": f"Replace `{col_name} = 1` with `{col_name} = TRUE` "
                           f"or `{col_name} = true`; replace `{col_name} = 0` "
                           f"with `{col_name} = FALSE` or `NOT {col_name}`"
                })

    return findings
```

### 6d. Sample BOOLEAN Column Value Distribution

Even without code access, knowing the value distribution helps assess risk:

```sql
-- Check BOOLEAN column value distribution
-- Useful for understanding if the column is actually boolean-like
SELECT
  '${table}' AS table_name,
  '${bool_col}' AS column_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN ${bool_col} = TRUE THEN 1 ELSE 0 END) AS true_count,
  SUM(CASE WHEN ${bool_col} = FALSE THEN 1 ELSE 0 END) AS false_count,
  SUM(CASE WHEN ${bool_col} IS NULL THEN 1 ELSE 0 END) AS null_count
FROM ${catalog}.${schema}.${table};
```

---

## 7. Parquet File Metadata Check

Tables written by older Spark versions (especially Spark 2.x on the Bright Health DBR 7.3 jobs) may contain parquet files with datetime values that were written using the legacy Julian calendar. When these files are read by Spark 3.0+, the datetime values can shift by several days for dates before 1900.

### Why This Matters

| Writer Version | Calendar Behavior | Risk |
|---|---|---|
| Spark 2.x (DBR 7.x) | Legacy Julian/hybrid calendar for dates/timestamps | Dates before 1900 may shift when read by Spark 3.0+ |
| Spark 3.0-3.1 (DBR 8.x-9.x) | Proleptic Gregorian, but `rebase` mode configurable | Generally safe if rebase mode was set |
| Spark 3.2+ (DBR 10.x+) | Proleptic Gregorian by default | Safe for new files; old files still need rebase |

### 7a. Check Table History for Writer Versions

```sql
-- Check who wrote to this table and when
-- Look for operations from old DBR versions
SELECT
  version,
  timestamp,
  operation,
  operationParameters,
  userIdentity.email AS writer,
  engineInfo
FROM (DESCRIBE HISTORY ${catalog}.${schema}.${table})
WHERE operation IN ('WRITE', 'MERGE', 'CREATE TABLE AS SELECT', 'CREATE OR REPLACE TABLE AS SELECT')
ORDER BY version DESC
LIMIT 50;
```

The `engineInfo` column contains the Spark/DBR version that performed the write. Look for entries containing:

- `Databricks-Runtime/7.` — DBR 7.x (Spark 2.4) — **HIGH risk** for datetime rebase
- `Databricks-Runtime/8.` or `Databricks-Runtime/9.` — Check for rebase config
- `Apache-Spark/2.` — Non-Databricks Spark 2.x writer — **HIGH risk**

### 7b. Check Rebase Configuration

```sql
-- Check table-level rebase mode setting
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.parquet.datetimeRebaseModeInRead');
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.parquet.datetimeRebaseModeInWrite');
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.parquet.int96RebaseModeInRead');
SHOW TBLPROPERTIES ${catalog}.${schema}.${table_name} ('delta.parquet.int96RebaseModeInWrite');
```

| Mode Value | Meaning |
|---|---|
| `EXCEPTION` | Throws error if legacy dates encountered (safe but may break reads) |
| `CORRECTED` | Assumes dates were written in Proleptic Gregorian (may shift old dates) |
| `LEGACY` | Assumes dates were written in Julian calendar (correct for Spark 2.x files) |

### 7c. Python Helper for Parquet Writer Analysis

```python
def check_parquet_writers(spark, tables: list[str]) -> list[dict]:
    """
    Analyze table history to find tables with old Spark 2.x writers.
    These tables need datetime rebase mode verification.
    """
    results = []
    for table_fqn in tables:
        try:
            history = spark.sql(f"""
                SELECT version, timestamp, operation, engineInfo
                FROM (DESCRIBE HISTORY {table_fqn})
                WHERE operation IN ('WRITE', 'MERGE',
                                    'CREATE TABLE AS SELECT',
                                    'CREATE OR REPLACE TABLE AS SELECT')
                ORDER BY version ASC
                LIMIT 100
            """).collect()

            has_spark2_writes = False
            oldest_engine = None
            for row in history:
                engine = row["engineInfo"] or ""
                if "Runtime/7." in engine or "Spark/2." in engine:
                    has_spark2_writes = True
                    if oldest_engine is None:
                        oldest_engine = engine

            # Check rebase mode
            rebase_mode = None
            try:
                rebase_row = spark.sql(
                    f"SHOW TBLPROPERTIES {table_fqn} ('delta.parquet.datetimeRebaseModeInRead')"
                ).collect()
                if rebase_row:
                    rebase_mode = rebase_row[0]["value"]
            except Exception:
                pass

            risk = "LOW"
            notes = []
            if has_spark2_writes:
                if rebase_mode is None or rebase_mode == "CORRECTED":
                    risk = "HIGH"
                    notes.append(
                        f"Table has Spark 2.x writes (engine: {oldest_engine}) "
                        f"and rebase mode is '{rebase_mode or 'not set'}' — "
                        f"dates before 1900 may be incorrect"
                    )
                elif rebase_mode == "LEGACY":
                    risk = "MEDIUM"
                    notes.append(
                        f"Spark 2.x writes detected but rebase mode is LEGACY — verify dates manually"
                    )
            else:
                notes.append("No Spark 2.x writers found in recent history")

            results.append({
                "table": table_fqn,
                "has_spark2_writes": has_spark2_writes,
                "oldest_engine": oldest_engine,
                "rebase_mode": rebase_mode,
                "risk": risk,
                "notes": "; ".join(notes)
            })
        except Exception as e:
            results.append({
                "table": table_fqn,
                "has_spark2_writes": None,
                "oldest_engine": None,
                "rebase_mode": None,
                "risk": "ERROR",
                "notes": str(e)
            })
    return results
```

---

## 8. Partition Scheme Analysis

Partitioning and ZORDER interact with Adaptive Query Execution (AQE) differently across DBR versions. A table partitioned by a high-cardinality column may behave differently when AQE is enabled by default on newer DBR. Additionally, tables using ZORDER may benefit from migration to Liquid Clustering — but **not in the first migration pass**.

### 8a. Check Partition Columns

```sql
-- Get partition information
SELECT
  name,
  partitionColumns,
  numFiles,
  sizeInBytes
FROM (DESCRIBE DETAIL ${catalog}.${schema}.${table});
```

### 8b. Assess Partition Cardinality

```sql
-- If the table is partitioned, check the cardinality of partition columns
-- High cardinality partitions (>10,000 distinct values) can cause issues with file listing
-- and may interact poorly with AQE's dynamic partition pruning

-- Replace ${partition_col} with the actual partition column name
SELECT
  '${table}' AS table_name,
  '${partition_col}' AS partition_column,
  COUNT(DISTINCT ${partition_col}) AS distinct_values,
  MIN(${partition_col}) AS min_value,
  MAX(${partition_col}) AS max_value,
  CASE
    WHEN COUNT(DISTINCT ${partition_col}) > 10000 THEN 'HIGH_CARDINALITY'
    WHEN COUNT(DISTINCT ${partition_col}) > 1000 THEN 'MEDIUM_CARDINALITY'
    ELSE 'OK'
  END AS cardinality_assessment
FROM ${catalog}.${schema}.${table};
```

### 8c. Check for ZORDER History

```sql
-- Check if OPTIMIZE with ZORDER has been run on this table
SELECT
  version,
  timestamp,
  operation,
  operationParameters
FROM (DESCRIBE HISTORY ${catalog}.${schema}.${table})
WHERE operation = 'OPTIMIZE'
ORDER BY version DESC
LIMIT 10;
```

Look at `operationParameters` for `zOrderBy` — this tells you which columns are ZORDER'd.

### 8d. Partition Risk Assessment

```python
def check_partition_risk(spark, tables: list[str]) -> list[dict]:
    """
    Assess partition scheme risks for migration.
    """
    results = []
    for table_fqn in tables:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
            partition_cols = detail["partitionColumns"]
            num_files = detail["numFiles"]

            notes = []
            risk = "LOW"

            if partition_cols and len(partition_cols) > 0:
                # Check cardinality of first partition column
                first_col = partition_cols[0]
                cardinality = spark.sql(f"""
                    SELECT COUNT(DISTINCT `{first_col}`) AS cnt
                    FROM {table_fqn}
                """).collect()[0]["cnt"]

                if cardinality > 10000:
                    risk = "MEDIUM"
                    notes.append(
                        f"High cardinality partition on '{first_col}' "
                        f"({cardinality} distinct values) — "
                        f"AQE may change scan behavior on new DBR"
                    )

                if len(partition_cols) > 2:
                    risk = "MEDIUM"
                    notes.append(
                        f"Multi-level partitioning ({partition_cols}) — "
                        f"consider Liquid Clustering in future pass"
                    )
            else:
                notes.append("No partitioning — no partition-related risks")

            # Check for ZORDER history
            zorder_history = spark.sql(f"""
                SELECT operationParameters
                FROM (DESCRIBE HISTORY {table_fqn})
                WHERE operation = 'OPTIMIZE'
                ORDER BY version DESC
                LIMIT 5
            """).collect()

            has_zorder = any(
                "zOrderBy" in str(row["operationParameters"])
                for row in zorder_history
            )
            if has_zorder:
                notes.append(
                    "ZORDER in use — do NOT change to Liquid Clustering in first migration pass. "
                    "Recommend as Phase 2 optimization."
                )

            results.append({
                "table": table_fqn,
                "partition_columns": partition_cols,
                "num_files": num_files,
                "has_zorder": has_zorder,
                "risk": risk,
                "notes": "; ".join(notes) if notes else "Clean partition scheme"
            })
        except Exception as e:
            results.append({
                "table": table_fqn,
                "partition_columns": None,
                "num_files": None,
                "has_zorder": None,
                "risk": "ERROR",
                "notes": str(e)
            })
    return results
```

### Recommendations

| Finding | First Migration Pass | Future Optimization |
|---------|---------------------|---------------------|
| ZORDER in use | Keep as-is | Migrate to Liquid Clustering |
| High cardinality partition | Keep as-is, monitor performance | Consider re-partitioning or Liquid Clustering |
| Multi-level partitioning | Keep as-is | Evaluate Liquid Clustering |
| No partitioning, large table | Keep as-is | Add Liquid Clustering |

---

## 9. File Layout Health

Poor file layout — many small files — is one of the most common causes of performance regression after migration. External tables are especially at risk because they do not receive Predictive Optimization (auto-compaction).

**This was a real issue at Molina.** The POP (Population Health) job went from a 5-minute runtime to over 3 hours after migration because the external tables it read had accumulated thousands of small files from legacy write patterns. Running `OPTIMIZE` before migration resolved the issue.

### 9a. File Count and Size Check

```sql
-- Check file count and total size
SELECT
  name AS table_name,
  numFiles,
  sizeInBytes,
  ROUND(sizeInBytes / numFiles, 0) AS avg_file_size_bytes,
  ROUND(sizeInBytes / (1024 * 1024 * 1024), 2) AS total_size_gb,
  ROUND(sizeInBytes / numFiles / (1024 * 1024), 2) AS avg_file_size_mb,
  CASE
    WHEN numFiles > 1000 AND (sizeInBytes / numFiles) < (32 * 1024 * 1024)
      THEN 'CRITICAL — many small files, OPTIMIZE required before migration'
    WHEN numFiles > 500 AND (sizeInBytes / numFiles) < (32 * 1024 * 1024)
      THEN 'WARNING — moderate small file issue, OPTIMIZE recommended'
    WHEN numFiles > 100 AND (sizeInBytes / numFiles) < (8 * 1024 * 1024)
      THEN 'WARNING — very small files detected'
    ELSE 'OK'
  END AS file_layout_assessment
FROM (DESCRIBE DETAIL ${catalog}.${schema}.${table});
```

### 9b. Batch File Layout Health Check

```python
def check_file_layout(spark, tables: list[str]) -> list[dict]:
    """
    Check file layout health for all tables.
    Flag tables with many small files that need OPTIMIZE before migration.
    """
    SMALL_FILE_THRESHOLD_BYTES = 32 * 1024 * 1024  # 32MB
    CRITICAL_FILE_COUNT = 1000
    WARNING_FILE_COUNT = 500

    results = []
    for table_fqn in tables:
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
            num_files = detail["numFiles"]
            size_bytes = detail["sizeInBytes"]

            avg_file_size = size_bytes / max(num_files, 1)
            is_external = detail.get("location", "").startswith(("s3://", "abfss://", "gs://"))

            risk = "LOW"
            notes = []

            if num_files > CRITICAL_FILE_COUNT and avg_file_size < SMALL_FILE_THRESHOLD_BYTES:
                risk = "HIGH"
                notes.append(
                    f"CRITICAL: {num_files} files, avg {avg_file_size / (1024*1024):.1f}MB — "
                    f"OPTIMIZE required before migration"
                )
            elif num_files > WARNING_FILE_COUNT and avg_file_size < SMALL_FILE_THRESHOLD_BYTES:
                risk = "MEDIUM"
                notes.append(
                    f"WARNING: {num_files} files, avg {avg_file_size / (1024*1024):.1f}MB — "
                    f"OPTIMIZE recommended"
                )

            if is_external and risk != "LOW":
                risk = "HIGH"  # Elevate risk for external tables
                notes.append(
                    "External table — no auto-compaction. "
                    "Manual OPTIMIZE is the only remediation."
                )

            if num_files == 0:
                notes.append("Table is empty (0 files)")

            results.append({
                "table": table_fqn,
                "num_files": num_files,
                "total_size_bytes": size_bytes,
                "avg_file_size_mb": round(avg_file_size / (1024 * 1024), 2),
                "is_external": is_external,
                "risk": risk,
                "notes": "; ".join(notes) if notes else "Good file layout"
            })
        except Exception as e:
            results.append({
                "table": table_fqn,
                "num_files": None,
                "total_size_bytes": None,
                "avg_file_size_mb": None,
                "is_external": None,
                "risk": "ERROR",
                "notes": str(e)
            })
    return results
```

### 9c. Target File Sizes

| Table Size | Target File Size | Target File Count |
|---|---|---|
| < 1 GB | 32-64 MB | 1-32 |
| 1-10 GB | 64-128 MB | 8-160 |
| 10-100 GB | 128-256 MB | 40-800 |
| > 100 GB | 256 MB - 1 GB | 100-1000 |

### 9d. OPTIMIZE Command

```sql
-- Run OPTIMIZE on a table with small files
-- This should be done BEFORE migration, while still on the current compute
OPTIMIZE ${catalog}.${schema}.${table};

-- For tables with ZORDER, preserve the existing ZORDER columns
OPTIMIZE ${catalog}.${schema}.${table}
ZORDER BY (${zorder_col1}, ${zorder_col2});

-- Verify after optimization
DESCRIBE DETAIL ${catalog}.${schema}.${table};
```

---

## 10. Output Risk Classification

Each table receives a composite risk score based on findings from all checks. This score determines whether the table needs pre-migration remediation, special attention during migration, or can proceed normally.

### Scoring Logic

| Category | HIGH Triggers | MEDIUM Triggers | LOW Criteria |
|----------|--------------|----------------|-------------|
| **Table Type** | External table with no recent OPTIMIZE | External table with recent OPTIMIZE | Managed table |
| **Protocol** | Writer v1-2 (likely auto-upgrade) | Reader v3 (limits downstream compatibility) | Current protocol compatible |
| **Properties** | Row tracking + _metadata in code | Column mapping enabled | No risky properties |
| **Datetime** | Invalid date strings found, `'00000000'` values | Pre-1582 dates, sentinel values | Clean date columns |
| **Boolean** | BOOLEAN columns with INT comparison in code | BOOLEAN columns present (unverified code) | No BOOLEAN columns |
| **Parquet** | Spark 2.x writes, no rebase mode set | Spark 2.x writes with LEGACY rebase | No old writers |
| **Partitions** | — | High cardinality, multi-level | Clean or no partitioning |
| **File Layout** | >1000 files < 32MB (especially external) | >500 files < 32MB | Good file layout |

### Composite Risk Calculation

```python
def classify_table_risk(checks: dict) -> dict:
    """
    Produce a composite risk classification from individual check results.

    Parameters:
        checks: dict with keys matching check names, each containing a 'risk' field
                Expected keys: protocol, properties, datetime, boolean, parquet,
                               partition, file_layout, table_type

    Returns:
        dict with overall_risk, category breakdown, and recommended actions
    """
    risk_weights = {
        "HIGH": 3,
        "MEDIUM": 2,
        "LOW": 1,
        "ERROR": 3  # Treat errors as HIGH — unknown state is dangerous
    }

    total_score = 0
    high_count = 0
    findings = []

    for check_name, check_result in checks.items():
        risk = check_result.get("risk", "LOW")
        score = risk_weights.get(risk, 1)
        total_score += score

        if risk == "HIGH":
            high_count += 1
            findings.append(f"HIGH [{check_name}]: {check_result.get('notes', 'No details')}")
        elif risk == "MEDIUM":
            findings.append(f"MEDIUM [{check_name}]: {check_result.get('notes', 'No details')}")

    # Overall classification
    if high_count >= 2 or total_score >= 18:
        overall = "HIGH"
    elif high_count >= 1 or total_score >= 12:
        overall = "MEDIUM"
    else:
        overall = "LOW"

    # Recommended actions based on findings
    actions = []
    for check_name, check_result in checks.items():
        risk = check_result.get("risk", "LOW")
        if risk in ("HIGH", "MEDIUM"):
            if check_name == "file_layout":
                actions.append(f"RUN: OPTIMIZE {check_result.get('table', 'table_name')}")
            elif check_name == "properties" and "ROW_TRACKING" in str(check_result.get("flags", [])):
                actions.append(f"DISABLE row tracking or rename _metadata column in code")
            elif check_name == "datetime":
                actions.append(f"REVIEW: Invalid date values in {check_result.get('column', 'date columns')} — ensure code uses TRY_CAST")
            elif check_name == "protocol":
                actions.append(f"RECORD: Current protocol versions before migration for rollback verification")
            elif check_name == "parquet":
                actions.append(f"SET: delta.parquet.datetimeRebaseModeInRead = LEGACY if dates before 1900 exist")
            elif check_name == "boolean":
                actions.append(f"FIX: Replace BOOLEAN = INT comparisons with BOOLEAN = TRUE/FALSE in code")

    return {
        "overall_risk": overall,
        "total_score": total_score,
        "high_count": high_count,
        "findings": findings,
        "recommended_actions": actions
    }
```

### Risk Report Output Format

```
TABLE RISK ASSESSMENT: prod_catalog.claims.claims_silver
=========================================================
Overall Risk: HIGH
Score: 19/24 | HIGH findings: 2

Findings:
  HIGH [datetime]: 4,521 rows with '00000000' in service_date — will fail CAST to DATE under ANSI
  HIGH [file_layout]: CRITICAL: 2,847 files, avg 4.2MB — OPTIMIZE required before migration
  MEDIUM [protocol]: Writer v2 may auto-upgrade on new DBR write
  MEDIUM [boolean]: BOOLEAN column 'is_active' found — verify no INT comparisons in code

Recommended Actions:
  1. RUN: OPTIMIZE prod_catalog.claims.claims_silver
  2. REVIEW: Invalid date values in service_date — ensure code uses TRY_CAST
  3. RECORD: Current protocol versions before migration for rollback verification
  4. FIX: Replace BOOLEAN = INT comparisons with BOOLEAN = TRUE/FALSE in code
```

---

## 11. Comprehensive Check Notebook Template

This notebook template runs all checks against a parameterized list of tables and produces a structured report. Copy this into a Databricks notebook and execute it.

### Cell 1: Parameters

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Pre-Migration Data Compatibility Check
# MAGIC
# MAGIC Runs all data-level compatibility checks against specified tables.
# MAGIC Produces a risk classification report for migration planning.

# COMMAND ----------

# Parameters — set these via job parameters or widgets
dbutils.widgets.text("tables", "", "Comma-separated fully-qualified table names")
dbutils.widgets.text("catalog", "", "Default catalog (used for schema-level scans)")
dbutils.widgets.text("schema", "", "Default schema (used for schema-level scans)")
dbutils.widgets.text("job_name", "", "Job name for the report header")

tables_param = dbutils.widgets.get("tables")
default_catalog = dbutils.widgets.get("catalog")
default_schema = dbutils.widgets.get("schema")
job_name = dbutils.widgets.get("job_name")

# Parse table list
if tables_param:
    tables = [t.strip() for t in tables_param.split(",") if t.strip()]
else:
    # If no explicit tables, discover all delta tables in the schema
    tables_df = spark.sql(f"""
        SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS fqn
        FROM system.information_schema.tables
        WHERE table_catalog = '{default_catalog}'
          AND table_schema = '{default_schema}'
          AND data_source_format = 'DELTA'
    """)
    tables = [row["fqn"] for row in tables_df.collect()]

print(f"Checking {len(tables)} tables for job: {job_name}")
for t in tables:
    print(f"  - {t}")
```

### Cell 2: Check Functions

```python
# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json

# ---- Table Type Classification ----
def check_table_type(spark, table_fqn: str) -> dict:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        location = detail.get("location", "")
        is_external = location.startswith(("s3://", "abfss://", "gs://", "wasbs://"))

        return {
            "table": table_fqn,
            "check": "table_type",
            "is_external": is_external,
            "location": location,
            "format": detail.get("format", "unknown"),
            "risk": "MEDIUM" if is_external else "LOW",
            "notes": "External table — no Predictive Optimization" if is_external else "Managed table"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "table_type", "risk": "ERROR", "notes": str(e)}


# ---- Protocol Version ----
def check_protocol(spark, table_fqn: str) -> dict:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        reader_v = detail["minReaderVersion"]
        writer_v = detail["minWriterVersion"]

        risk = "LOW"
        notes = []
        if writer_v <= 2:
            risk = "HIGH"
            notes.append(f"Writer v{writer_v} — high likelihood of auto-upgrade on new DBR")
        elif writer_v <= 4:
            risk = "MEDIUM"
            notes.append(f"Writer v{writer_v} — may auto-upgrade if new features are enabled")
        if reader_v >= 3:
            notes.append("Reader v3+ — older DBR readers will fail if they access this table")

        return {
            "table": table_fqn,
            "check": "protocol",
            "minReaderVersion": reader_v,
            "minWriterVersion": writer_v,
            "risk": risk,
            "notes": "; ".join(notes) if notes else "Protocol is compatible"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "protocol", "risk": "ERROR", "notes": str(e)}


# ---- Table Properties ----
def check_properties(spark, table_fqn: str) -> dict:
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {table_fqn}")
        props = {row["key"]: row["value"] for row in props_df.collect()}

        flags = []
        risk = "LOW"

        if props.get("delta.enableRowTracking", "false").lower() == "true":
            flags.append("ROW_TRACKING_ENABLED")
            risk = "HIGH"

        retention = props.get("delta.deletedFileRetentionDuration", "interval 7 days")
        if "1 day" in retention or "hours" in retention:
            flags.append(f"SHORT_RETENTION ({retention})")
            risk = max(risk, "MEDIUM", key=lambda x: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}[x])

        if props.get("delta.enableDeletionVectors", "false").lower() == "true":
            flags.append("DELETION_VECTORS_ENABLED")

        if props.get("delta.columnMapping.mode", "none") != "none":
            flags.append(f"COLUMN_MAPPING={props['delta.columnMapping.mode']}")

        return {
            "table": table_fqn,
            "check": "properties",
            "flags": flags,
            "risk": risk,
            "notes": "; ".join(flags) if flags else "No risky properties"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "properties", "risk": "ERROR", "notes": str(e)}


# ---- Datetime/Timestamp Analysis ----
def check_datetime(spark, table_fqn: str) -> dict:
    try:
        catalog, schema, table = table_fqn.split(".")

        # Find string columns with date-like names
        date_name_patterns = [
            "%date%", "%_dt", "%dt_%", "%time%", "%dob%", "%birth%",
            "%admit%", "%discharge%", "%effective%", "%expir%",
            "%service%", "%incur%", "%paid%", "%received%"
        ]
        like_clauses = " OR ".join(
            [f"LOWER(column_name) LIKE '{p}'" for p in date_name_patterns]
        )

        # Get both typed date columns and string date-like columns
        cols_df = spark.sql(f"""
            SELECT column_name, data_type
            FROM system.information_schema.columns
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
              AND (
                data_type IN ('date', 'timestamp', 'timestamp_ntz')
                OR (data_type = 'string' AND ({like_clauses}))
              )
        """)
        cols = cols_df.collect()

        if not cols:
            return {
                "table": table_fqn,
                "check": "datetime",
                "risk": "LOW",
                "columns_checked": 0,
                "notes": "No date/timestamp or date-like string columns found"
            }

        findings = []
        overall_risk = "LOW"

        for col_row in cols:
            col_name = col_row["column_name"]
            col_type = col_row["data_type"]

            if col_type == "string":
                result = spark.sql(f"""
                    SELECT
                      SUM(CASE WHEN `{col_name}` = '00000000' THEN 1 ELSE 0 END) AS zeros,
                      SUM(CASE WHEN TRIM(COALESCE(`{col_name}`, '')) = '' THEN 1 ELSE 0 END) AS empty,
                      SUM(CASE WHEN `{col_name}` RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                                AND TRY_TO_DATE(`{col_name}`, 'yyyy-MM-dd') IS NULL
                           THEN 1 ELSE 0 END) AS invalid_iso,
                      SUM(CASE WHEN `{col_name}` RLIKE '^[0-9]{{8}}$'
                                AND `{col_name}` != '00000000'
                                AND TRY_TO_DATE(`{col_name}`, 'yyyyMMdd') IS NULL
                           THEN 1 ELSE 0 END) AS invalid_compact
                    FROM {table_fqn}
                    WHERE `{col_name}` IS NOT NULL
                """).collect()[0]

                if result["zeros"] > 0 or result["invalid_iso"] > 0 or result["invalid_compact"] > 0:
                    overall_risk = "HIGH"
                    findings.append(
                        f"STRING column '{col_name}': "
                        f"zeros={result['zeros']}, "
                        f"invalid_iso={result['invalid_iso']}, "
                        f"invalid_compact={result['invalid_compact']}"
                    )
            else:
                # Typed date/timestamp column — check for edge cases
                result = spark.sql(f"""
                    SELECT
                      SUM(CASE WHEN `{col_name}` < DATE'1582-10-15' THEN 1 ELSE 0 END) AS pre_1582,
                      SUM(CASE WHEN `{col_name}` > DATE'2100-01-01' THEN 1 ELSE 0 END) AS far_future
                    FROM {table_fqn}
                    WHERE `{col_name}` IS NOT NULL
                """).collect()[0]

                if result["pre_1582"] > 0:
                    overall_risk = max(overall_risk, "HIGH",
                                       key=lambda x: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}[x])
                    findings.append(f"DATE column '{col_name}': {result['pre_1582']} pre-1582 dates")
                if result["far_future"] > 0:
                    findings.append(f"DATE column '{col_name}': {result['far_future']} far-future dates")

        return {
            "table": table_fqn,
            "check": "datetime",
            "columns_checked": len(cols),
            "risk": overall_risk,
            "findings": findings,
            "notes": "; ".join(findings) if findings else "All date columns clean"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "datetime", "risk": "ERROR", "notes": str(e)}


# ---- Boolean Detection ----
def check_boolean(spark, table_fqn: str) -> dict:
    try:
        catalog, schema, table = table_fqn.split(".")
        bool_cols = spark.sql(f"""
            SELECT column_name
            FROM system.information_schema.columns
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
              AND data_type = 'boolean'
        """).collect()

        if not bool_cols:
            return {
                "table": table_fqn,
                "check": "boolean",
                "boolean_columns": [],
                "risk": "LOW",
                "notes": "No BOOLEAN columns"
            }

        col_names = [r["column_name"] for r in bool_cols]
        return {
            "table": table_fqn,
            "check": "boolean",
            "boolean_columns": col_names,
            "risk": "MEDIUM",
            "notes": (
                f"BOOLEAN columns found: {col_names} — "
                f"cross-reference with notebook code for INT comparisons"
            )
        }
    except Exception as e:
        return {"table": table_fqn, "check": "boolean", "risk": "ERROR", "notes": str(e)}


# ---- Parquet Writer History ----
def check_parquet(spark, table_fqn: str) -> dict:
    try:
        history = spark.sql(f"""
            SELECT engineInfo
            FROM (DESCRIBE HISTORY {table_fqn})
            WHERE operation IN ('WRITE', 'MERGE',
                                'CREATE TABLE AS SELECT',
                                'CREATE OR REPLACE TABLE AS SELECT')
            ORDER BY version ASC
            LIMIT 100
        """).collect()

        has_spark2 = any(
            ("Runtime/7." in str(r["engineInfo"]) or "Spark/2." in str(r["engineInfo"]))
            for r in history
        )

        if has_spark2:
            # Check rebase mode
            rebase = None
            try:
                rebase_row = spark.sql(
                    f"SHOW TBLPROPERTIES {table_fqn} ('delta.parquet.datetimeRebaseModeInRead')"
                ).collect()
                if rebase_row:
                    rebase = rebase_row[0]["value"]
            except Exception:
                pass

            risk = "HIGH" if rebase is None or rebase == "CORRECTED" else "MEDIUM"
            return {
                "table": table_fqn,
                "check": "parquet",
                "has_spark2_writes": True,
                "rebase_mode": rebase,
                "risk": risk,
                "notes": f"Spark 2.x writes detected; rebase mode = {rebase or 'NOT SET'}"
            }

        return {
            "table": table_fqn,
            "check": "parquet",
            "has_spark2_writes": False,
            "rebase_mode": None,
            "risk": "LOW",
            "notes": "No Spark 2.x writers in history"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "parquet", "risk": "ERROR", "notes": str(e)}


# ---- File Layout ----
def check_file_layout(spark, table_fqn: str) -> dict:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        num_files = detail["numFiles"]
        size_bytes = detail["sizeInBytes"]
        avg_size = size_bytes / max(num_files, 1)
        location = detail.get("location", "")
        is_external = location.startswith(("s3://", "abfss://", "gs://", "wasbs://"))

        risk = "LOW"
        notes = []

        if num_files > 1000 and avg_size < 32 * 1024 * 1024:
            risk = "HIGH"
            notes.append(
                f"{num_files} files, avg {avg_size / (1024*1024):.1f}MB — "
                f"OPTIMIZE required"
            )
        elif num_files > 500 and avg_size < 32 * 1024 * 1024:
            risk = "MEDIUM"
            notes.append(
                f"{num_files} files, avg {avg_size / (1024*1024):.1f}MB — "
                f"OPTIMIZE recommended"
            )

        if is_external and risk != "LOW":
            risk = "HIGH"
            notes.append("External table — no auto-compaction available")

        return {
            "table": table_fqn,
            "check": "file_layout",
            "num_files": num_files,
            "total_size_bytes": size_bytes,
            "avg_file_size_mb": round(avg_size / (1024 * 1024), 2),
            "is_external": is_external,
            "risk": risk,
            "notes": "; ".join(notes) if notes else "Good file layout"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "file_layout", "risk": "ERROR", "notes": str(e)}


# ---- Partition Check ----
def check_partitions(spark, table_fqn: str) -> dict:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        partition_cols = detail["partitionColumns"] or []

        notes = []
        risk = "LOW"

        if len(partition_cols) > 0:
            first_col = partition_cols[0]
            cardinality = spark.sql(f"""
                SELECT COUNT(DISTINCT `{first_col}`) AS cnt FROM {table_fqn}
            """).collect()[0]["cnt"]

            if cardinality > 10000:
                risk = "MEDIUM"
                notes.append(
                    f"High cardinality partition on '{first_col}' ({cardinality} distinct values)"
                )
            if len(partition_cols) > 2:
                risk = "MEDIUM"
                notes.append(f"Multi-level partitioning: {partition_cols}")

        # Check ZORDER
        zorder_history = spark.sql(f"""
            SELECT operationParameters
            FROM (DESCRIBE HISTORY {table_fqn})
            WHERE operation = 'OPTIMIZE'
            ORDER BY version DESC LIMIT 5
        """).collect()

        has_zorder = any("zOrderBy" in str(r["operationParameters"]) for r in zorder_history)
        if has_zorder:
            notes.append("ZORDER in use — preserve during first migration pass")

        return {
            "table": table_fqn,
            "check": "partitions",
            "partition_columns": partition_cols,
            "has_zorder": has_zorder,
            "risk": risk,
            "notes": "; ".join(notes) if notes else "Clean partition scheme"
        }
    except Exception as e:
        return {"table": table_fqn, "check": "partitions", "risk": "ERROR", "notes": str(e)}
```

### Cell 3: Run All Checks

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Checks

# COMMAND ----------

from collections import defaultdict

all_results = []
table_summaries = []

for table_fqn in tables:
    print(f"\n{'='*70}")
    print(f"Checking: {table_fqn}")
    print(f"{'='*70}")

    checks = {}
    check_functions = {
        "table_type": check_table_type,
        "protocol": check_protocol,
        "properties": check_properties,
        "datetime": check_datetime,
        "boolean": check_boolean,
        "parquet": check_parquet,
        "file_layout": check_file_layout,
        "partitions": check_partitions,
    }

    for check_name, check_fn in check_functions.items():
        result = check_fn(spark, table_fqn)
        checks[check_name] = result
        all_results.append(result)

        risk_indicator = {
            "HIGH": "[!!!]", "MEDIUM": "[!!]", "LOW": "[ok]", "ERROR": "[ERR]"
        }.get(result.get("risk", "LOW"), "[??]")

        print(f"  {risk_indicator} {check_name}: {result.get('notes', 'No details')}")

    # Compute composite risk
    risk_weights = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "ERROR": 3}
    total_score = sum(risk_weights.get(c.get("risk", "LOW"), 1) for c in checks.values())
    high_count = sum(1 for c in checks.values() if c.get("risk") == "HIGH")

    if high_count >= 2 or total_score >= 18:
        overall = "HIGH"
    elif high_count >= 1 or total_score >= 12:
        overall = "MEDIUM"
    else:
        overall = "LOW"

    summary = {
        "table": table_fqn,
        "overall_risk": overall,
        "total_score": total_score,
        "high_count": high_count,
        "checks": {k: v.get("risk", "LOW") for k, v in checks.items()}
    }
    table_summaries.append(summary)

    print(f"\n  >>> OVERALL RISK: {overall} (score: {total_score}, HIGH findings: {high_count})")
```

### Cell 4: Summary Report

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print(f"\n{'='*70}")
print(f"PRE-MIGRATION DATA COMPATIBILITY REPORT")
print(f"Job: {job_name}")
print(f"Tables checked: {len(tables)}")
print(f"Run at: {datetime.now().isoformat()}")
print(f"{'='*70}\n")

# Group by risk
high_tables = [s for s in table_summaries if s["overall_risk"] == "HIGH"]
medium_tables = [s for s in table_summaries if s["overall_risk"] == "MEDIUM"]
low_tables = [s for s in table_summaries if s["overall_risk"] == "LOW"]

print(f"HIGH risk tables:   {len(high_tables)}")
print(f"MEDIUM risk tables: {len(medium_tables)}")
print(f"LOW risk tables:    {len(low_tables)}")

if high_tables:
    print(f"\n--- HIGH RISK (require pre-migration action) ---")
    for s in high_tables:
        print(f"\n  {s['table']}")
        print(f"    Score: {s['total_score']} | HIGH checks: {s['high_count']}")
        for check_name, risk in s["checks"].items():
            if risk in ("HIGH", "ERROR"):
                print(f"    [!!!] {check_name}: {risk}")

if medium_tables:
    print(f"\n--- MEDIUM RISK (review before migration) ---")
    for s in medium_tables:
        print(f"\n  {s['table']}")
        print(f"    Score: {s['total_score']}")
        for check_name, risk in s["checks"].items():
            if risk == "MEDIUM":
                print(f"    [!!] {check_name}")

if low_tables:
    print(f"\n--- LOW RISK (safe to proceed) ---")
    for s in low_tables:
        print(f"  {s['table']}")

# Store as JSON for downstream consumption
report = {
    "job_name": job_name,
    "run_timestamp": datetime.now().isoformat(),
    "tables_checked": len(tables),
    "summary": {
        "high": len(high_tables),
        "medium": len(medium_tables),
        "low": len(low_tables)
    },
    "table_results": table_summaries,
    "detailed_results": all_results
}

# Make available for downstream cells or export
spark.conf.set("migration.compatibility_report", json.dumps(report))
print(f"\nReport stored in spark.conf 'migration.compatibility_report'")
```

### Cell 5: Export to Delta (Optional)

```python
# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results to Delta Table (Optional)
# MAGIC
# MAGIC Uncomment and configure to persist results for tracking across multiple job assessments.

# COMMAND ----------

# report_catalog = "migration_tracking"
# report_schema = "compatibility_checks"
# report_table = f"{report_catalog}.{report_schema}.data_compatibility_results"
#
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
#
# rows = []
# for s in table_summaries:
#     rows.append((
#         job_name,
#         s["table"],
#         s["overall_risk"],
#         s["total_score"],
#         s["high_count"],
#         json.dumps(s["checks"]),
#         datetime.now()
#     ))
#
# schema = StructType([
#     StructField("job_name", StringType()),
#     StructField("table_name", StringType()),
#     StructField("overall_risk", StringType()),
#     StructField("total_score", IntegerType()),
#     StructField("high_count", IntegerType()),
#     StructField("check_details", StringType()),
#     StructField("check_timestamp", TimestampType()),
# ])
#
# df = spark.createDataFrame(rows, schema)
# df.write.mode("append").saveAsTable(report_table)
# print(f"Results saved to {report_table}")
```

---

## 12. Pre-Migration Actions

Based on the findings from all checks, the following pre-migration actions should be taken before a job is migrated. These are ordered by priority.

### Action Priority Matrix

| Priority | Trigger | Action | SQL / Command |
|---|---|---|---|
| **P0 — Blocking** | External table with >1000 small files | OPTIMIZE before migration | `OPTIMIZE catalog.schema.table;` |
| **P0 — Blocking** | Row tracking enabled + _metadata in code | Disable row tracking OR rename column | `ALTER TABLE t SET TBLPROPERTIES ('delta.enableRowTracking' = false);` |
| **P0 — Blocking** | Invalid date strings (`'00000000'`, etc.) | Fix in code: ensure `TRY_CAST` / `TRY_TO_DATE` is used everywhere | Code change — see `07-ansi-compliance-reference.md` |
| **P1 — High** | Spark 2.x parquet files + no rebase mode | Set rebase mode on table | `ALTER TABLE t SET TBLPROPERTIES ('delta.parquet.datetimeRebaseModeInRead' = 'LEGACY');` |
| **P1 — High** | BOOLEAN columns with INT comparison in code | Fix comparisons in code | `WHERE is_active = TRUE` instead of `WHERE is_active = 1` |
| **P1 — High** | Protocol v1/v2 with downstream readers on old DBR | Record current protocol; coordinate upgrade timing | Document in manifest |
| **P2 — Medium** | External tables missing statistics | Run ANALYZE TABLE | `ANALYZE TABLE catalog.schema.table COMPUTE STATISTICS FOR ALL COLUMNS;` |
| **P2 — Medium** | Short retention duration | Extend before migration to preserve time travel | `ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');` |
| **P2 — Medium** | Table needs VACUUM but hasn't been vacuumed recently | Run VACUUM before migration | `VACUUM catalog.schema.table RETAIN 168 HOURS;` |
| **P3 — Low** | ZORDER in use | Document ZORDER columns; preserve in first pass | No action needed now |
| **P3 — Low** | High cardinality partitions | Document for Phase 2 Liquid Clustering evaluation | No action needed now |

### Pre-Migration Checklist SQL

Run these commands for every HIGH-risk table before starting the migration:

```sql
-- 1. OPTIMIZE tables with small files
OPTIMIZE ${catalog}.${schema}.${table};

-- 2. Compute statistics for external tables
ANALYZE TABLE ${catalog}.${schema}.${table} COMPUTE STATISTICS FOR ALL COLUMNS;

-- 3. Extend retention if too short (preserves time travel for rollback comparison)
ALTER TABLE ${catalog}.${schema}.${table}
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');

-- 4. Disable row tracking if _metadata conflict is confirmed
ALTER TABLE ${catalog}.${schema}.${table}
SET TBLPROPERTIES ('delta.enableRowTracking' = false);

-- 5. Set rebase mode for tables with Spark 2.x parquet files
ALTER TABLE ${catalog}.${schema}.${table}
SET TBLPROPERTIES (
  'delta.parquet.datetimeRebaseModeInRead' = 'LEGACY',
  'delta.parquet.int96RebaseModeInRead' = 'LEGACY'
);

-- 6. Record current protocol version in manifest (for rollback verification)
SELECT
  '${catalog}.${schema}.${table}' AS table_name,
  minReaderVersion,
  minWriterVersion
FROM (DESCRIBE DETAIL ${catalog}.${schema}.${table});

-- 7. Verify OPTIMIZE results
DESCRIBE DETAIL ${catalog}.${schema}.${table};
-- Check that numFiles is reasonable and avg file size > 32MB
```

### Workflow Integration

These pre-migration actions feed into the assessment skill's workflow:

1. **Assessment skill** runs data compatibility checks (this document) as part of F6
2. Findings are included in the per-job structured report (F8)
3. P0 actions are listed as **blockers** — migration cannot proceed until resolved
4. P1 actions are listed as **required pre-work** — must be done before migration but do not block assessment
5. P2-P3 actions are listed as **recommendations** — tracked for future optimization passes

The `conversion_validator` skill (resource 08) then validates output equivalence **after** migration, comparing against the baseline recorded before these pre-migration actions were applied.
