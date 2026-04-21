# SQL to DBSQL Serverless Migration

This skill teaches Genie Code how to convert SQL-only notebooks (or PySpark notebooks that only wrap SQL) from classic compute to DBSQL Serverless Current Channel SQL notebooks.

## When to Use

Use this skill when the assessment identifies a job as Path D eligible:
- All cells are SQL, %sql magic, or Python cells that only call spark.sql()
- No DataFrame operations
- No Python UDFs (or they can be converted to SQL UDFs)
- No streaming, ML, or complex Python logic

## Eligibility Quick Check

Scan the notebook. If ANY of these are found, the notebook is NOT Path D eligible (use Path C instead):
- `.filter(`, `.select(`, `.withColumn(`, `.join(`, `.groupBy(` (DataFrame API)
- `@udf`, `F.udf(`, `spark.udf.register(` (Python UDFs)
- `readStream`, `writeStream` (streaming)
- `from sklearn`, `from torch`, `import tensorflow` (ML)
- Complex Python logic: `class `, `def ` (beyond trivial variable assignment)

## Notebook Format Conversion

### spark.sql() to SQL Cells

Every `spark.sql("...")` call becomes a direct SQL statement.

**Detect:**
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
display(result)
```

**After (SQL cell):**
```sql
SELECT member_id, claim_date, paid_amount
FROM claims_silver
WHERE claim_date >= '2024-01-01'
```

### Python Variable Interpolation to SQL Parameters

**Detect:**
```regex
spark\.sql\s*\(\s*f["']{1,3}
```

**Before:**
```python
env = dbutils.widgets.get("env")
spark.sql(f"SELECT * FROM {env}_catalog.schema.table WHERE state = '{state_code}'")
```

**After:**
```sql
SELECT * FROM IDENTIFIER(:env || '_catalog.schema.table')
WHERE state = :state_code
```

Or using the established USE CATALOG pattern:
```sql
USE CATALOG IDENTIFIER(:env || '_catalog');
SELECT * FROM schema.table WHERE state = :state_code
```

### display() Removal

**Detect:**
```regex
display\s*\(
```

Remove `display()` wrappers. DBSQL auto-displays SELECT results.

### dbutils.widgets to SQL Parameters

**Detect:**
```regex
dbutils\.widgets\.get\s*\(
dbutils\.widgets\.text\s*\(
```

**Before:**
```python
start_date = dbutils.widgets.get("start_date")
spark.sql(f"SELECT * FROM table WHERE date >= '{start_date}'")
```

**After:**
```sql
-- Parameter declared in DBSQL notebook parameter panel
SELECT * FROM table WHERE date >= :start_date
```

### %run Dependencies

**Detect:**
```regex
%run\s+
dbutils\.notebook\.run\s*\(
```

`%run` is not available in DBSQL SQL notebooks. Options:
- Restructure as separate workflow tasks with dependencies
- Inline the referenced notebook's SQL into the current notebook
- Use SQL UDFs for shared logic

### For Loops Over States/Dates

**Detect:**
```regex
for\s+\w+\s+in\s+.*:.*spark\.sql
```

Python loops that iterate and call spark.sql() per iteration cannot be directly converted to SQL. Options:
- Rewrite as a single SQL query with appropriate WHERE/GROUP BY
- If truly needs iteration, keep as Path C (PySpark serverless)
- Use workflow parameters to run the SQL job once per value

## ANSI Fixes for SQL

ANSI mode is always ON in DBSQL. Apply these fixes to all SQL:

| Unsafe | Safe |
|--------|------|
| `CAST(x AS INT)` | `TRY_CAST(x AS INT)` |
| `CAST(x AS DATE)` | `TRY_CAST(x AS DATE)` |
| `a / b` | `TRY_DIVIDE(a, b)` |
| `a % b` | `a % NULLIF(b, 0)` |
| `array[i]` | `TRY_ELEMENT_AT(array, i+1)` |
| `map['key']` | `TRY_ELEMENT_AT(map, 'key')` |
| `to_date(s, fmt)` | `try_to_date(s, fmt)` |
| `to_timestamp(s, fmt)` | `try_to_timestamp(s, fmt)` |
| `bool_col = 1` | `bool_col IS TRUE` |
| `bool_col = 0` | `bool_col IS NOT TRUE` |
| `CONCAT(a, NULL)` | `CONCAT_WS('', a, b)` |
| `SUM(int_col)` | `SUM(CAST(int_col AS BIGINT))` |

## Operations to Remove

| Operation | Action |
|-----------|--------|
| `SET spark.sql.ansi.enabled = false` | Remove. ANSI is always ON. |
| `CACHE TABLE` | Remove. DBSQL manages caching. |
| `UNCACHE TABLE` | Remove. |
| `REFRESH TABLE` | Remove. Usually not needed in DBSQL. |
| `spark.conf.set(...)` | Remove all. DBSQL manages configs. |

## SQL UDF Conversion

If the source has simple Python UDFs that are just SQL logic in Python, convert to SQL UDFs:

**Before (Python UDF):**
```python
@udf(returnType=StringType())
def categorize_risk(score):
    if score is None: return None
    if score >= 4.0: return "Critical"
    if score >= 3.0: return "High"
    if score >= 2.0: return "Moderate"
    return "Low"
```

**After (SQL UDF):**
```sql
CREATE OR REPLACE FUNCTION categorize_risk(score DOUBLE)
RETURNS STRING
RETURN CASE
    WHEN score IS NULL THEN NULL
    WHEN score >= 4.0 THEN 'Critical'
    WHEN score >= 3.0 THEN 'High'
    WHEN score >= 2.0 THEN 'Moderate'
    ELSE 'Low'
END;
```

## Workflow Configuration

For the job JSON, the task type changes from notebook_task to sql_task with a SQL warehouse:

```json
{
  "task_key": "gold_reporting",
  "sql_task": {
    "warehouse_id": "<serverless_sql_warehouse_id>",
    "query": {
      "query_id": "<query_id>"
    }
  }
}
```

Or keep as notebook_task but run on a SQL warehouse instead of general compute.
