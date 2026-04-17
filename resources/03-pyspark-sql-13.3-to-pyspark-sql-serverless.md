# Path C: PySpark/SQL on DBR 13.3 LTS to PySpark/SQL on Serverless General Compute

**Migration Path:** PySpark/SQL notebooks on DBR 13.3 LTS (Spark 3.4.1, Python 3.10) --> PySpark/SQL on Serverless General Compute, environment version 4 (Python 3.12)

**What this path combines:** A DBR upgrade (13.3 --> serverless latest Spark) AND a compute migration (classic clusters --> serverless). No language conversion is needed.

**Customer context:** Molina Healthcare, ~5000 jobs. Healthcare data -- silent data changes are unacceptable. Both standard and ML runtime source notebooks are in scope. Molina uses `USE CATALOG {{env}}_catalog` with 2-part table names -- this is correct Unity Catalog usage and must NOT be flagged as non-UC.

---

## Table of Contents

1. [Migration Overview](#1-migration-overview)
2. [ANSI Mode Migration (Critical)](#2-ansi-mode-migration-critical)
3. [Python 3.10 to 3.12 Changes](#3-python-310-to-312-changes)
4. [Serverless Compute Restrictions](#4-serverless-compute-restrictions)
5. [Spark Configuration Migration](#5-spark-configuration-migration)
6. [Environment Variable Migration](#6-environment-variable-migration)
7. [Job JSON Transformation](#7-job-json-transformation)
8. [Package and Dependency Analysis](#8-package-and-dependency-analysis)
9. [Performance Optimization](#9-performance-optimization)
10. [_metadata Column Conflicts](#10-_metadata-column-conflicts)
11. [Schema Inference on Serverless](#11-schema-inference-on-serverless)
12. [Testing and Validation](#12-testing-and-validation)
13. [Known Issues](#13-known-issues)
14. [Step-by-Step Process](#14-step-by-step-process)
15. [Documentation Links](#15-documentation-links)

---

## 1. Migration Overview

### What Changes

| Dimension | Before (Source) | After (Target) |
|-----------|----------------|----------------|
| Runtime | DBR 13.3 LTS (Spark 3.4.1) | Serverless General Compute, env v4 (latest Spark) |
| Compute type | Classic clusters (job clusters or all-purpose) | Serverless General Compute |
| Python version | 3.10 | 3.12 |
| ANSI mode | OFF by default (spark.sql.ansi.enabled = false) | ALWAYS ON, cannot be disabled |
| Default data format | parquet (spark.sql.sources.default) | delta |
| Infrastructure config | User-managed (executor memory, cores, autoscaling) | Fully managed by Databricks |
| Dependencies | %pip install, init scripts, cluster libraries | requirements.txt at Volume path or environment pane |
| Job JSON | job_clusters block, cluster spec per task | environments block with environment_key per task |

### What Stays the Same

- **Language:** PySpark and SQL -- no language conversion needed
- **DataFrame API:** All DataFrame operations remain identical
- **SQL syntax:** All Spark SQL, Delta MERGE/UPDATE/DELETE, window functions
- **dbutils API:** widgets, notebook.run, fs, secrets
- **Delta operations:** MERGE INTO, CREATE TABLE, OPTIMIZE
- **%run references:** Notebook %run calls work the same
- **Unity Catalog:** `USE CATALOG {{env}}_catalog` with 2-part names works as-is
- **spark.read / spark.write:** Same API surface

### Order of Operations

Apply changes in this specific order. Each step may introduce issues that later steps depend on:

1. **Apply ANSI fixes** -- highest priority, most code changes, most risk of silent data changes
2. **Apply serverless restrictions** -- remove unsupported operations, rewrite RDD APIs, etc.
3. **Migrate Spark configs** -- remove unsupported configs, adjust changed defaults
4. **Migrate environment variables** -- os.environ.get() to dbutils.widgets.get()
5. **Audit and migrate dependencies** -- %pip install to requirements.txt
6. **Transform job JSON** -- remove job_clusters, add environments block
7. **Review for performance** -- remove manual tuning, adopt serverless patterns
8. **Test and validate** -- parallel runs, data comparison

---

## 2. ANSI Mode Migration (Critical)

### Why This Is Critical

ANSI mode is **always ON** on serverless compute. There is no option to disable it. Setting `spark.sql.ansi.enabled = false` is silently ignored.

Under ANSI mode, operations that previously returned `null` silently now throw runtime exceptions:

| Operation | ANSI OFF (DBR 13.3 default) | ANSI ON (Serverless) |
|-----------|----------------------------|---------------------|
| `CAST('abc' AS INT)` | Returns `null` | Throws `NumberFormatException` |
| `10 / 0` | Returns `null` | Throws `ArithmeticException` |
| `array_col[999]` | Returns `null` | Throws `ArrayIndexOutOfBoundsException` |
| `map_col['missing']` | Returns `null` | Throws `NoSuchElementException` |
| `2147483647 + 1` (INT) | Silent wraparound to -2147483648 | Throws `ArithmeticException` |
| `to_timestamp('not-a-date')` | Returns `null` | Throws `SparkDateTimeException` |
| `to_date('00000000')` | Returns `null` | Throws `SparkDateTimeException` |

**For healthcare data, ANSI mode is actually safer** -- silent nulls can mask data quality issues. But existing code that depends on the old null-returning behavior will break.

**Design principle:** Never recommend `spark.sql.ansi.enabled = false` as a solution. Always generate the specific ANSI-safe code fix.

---

### 2.1 CAST to TRY_CAST

Invalid values that previously returned null now throw exceptions.

**Detect regex (SQL):**
```regex
(?i)\bCAST\s*\((?!.*\bAS\s+(STRING|VARCHAR|CHAR)\b)
```
Note: CAST to STRING never fails, so only flag CAST to numeric/date/timestamp/boolean types.

**Detect regex (PySpark):**
```regex
\.cast\s*\(\s*["']?(int|integer|long|bigint|short|smallint|tinyint|byte|float|double|decimal|numeric|date|timestamp|boolean)
```

**Before -- SQL:**
```sql
SELECT CAST(string_col AS INT) FROM claims
SELECT CAST(amount_str AS DECIMAL(10,2)) FROM payments
SELECT CAST(flag AS BOOLEAN) FROM members
```

**After -- SQL:**
```sql
SELECT TRY_CAST(string_col AS INT) FROM claims
SELECT TRY_CAST(amount_str AS DECIMAL(10,2)) FROM payments
SELECT TRY_CAST(flag AS BOOLEAN) FROM members
```

**Before -- PySpark:**
```python
df = df.withColumn("amount_int", F.col("amount_str").cast("int"))
df = df.withColumn("price", F.col("price_str").cast("decimal(10,2)"))
```

**After -- PySpark (when/otherwise guard):**
```python
# Option 1: Regex guard for integer
df = df.withColumn("amount_int",
    F.when(F.col("amount_str").rlike(r"^-?\d+$"), F.col("amount_str").cast("int"))
     .otherwise(F.lit(None).cast("int"))
)

# Option 2: Use SQL TRY_CAST via expr (preferred for complex types)
df = df.withColumn("price", F.expr("TRY_CAST(price_str AS DECIMAL(10,2))"))

# Option 3: For decimal with regex guard
df = df.withColumn("price",
    F.when(F.col("price_str").rlike(r"^-?\d+\.?\d*$"), F.col("price_str").cast("decimal(10,2)"))
     .otherwise(F.lit(None).cast("decimal(10,2)"))
)
```

---

### 2.2 Division by Zero to TRY_DIVIDE

Division by zero previously returned null, now throws ArithmeticException.

**Detect regex (SQL):**
```regex
(?i)\b\w+\s*/\s*\w+(?!\s*--.*TRY_DIVIDE)
```

**Detect regex (PySpark):**
```regex
F\.col\([^)]+\)\s*/\s*F\.col\(|\.col\([^)]+\)\s*/\s*
```

**Before -- SQL:**
```sql
SELECT total_amount / member_count FROM summary
SELECT paid / billed AS pct_paid FROM claims
```

**After -- SQL:**
```sql
-- Option 1: TRY_DIVIDE (returns null on divide by zero)
SELECT TRY_DIVIDE(total_amount, member_count) FROM summary
SELECT TRY_DIVIDE(paid, billed) AS pct_paid FROM claims

-- Option 2: Explicit CASE guard (when you want a specific default)
SELECT CASE WHEN member_count = 0 THEN 0 ELSE total_amount / member_count END FROM summary
```

**Before -- PySpark:**
```python
df = df.withColumn("rate", F.col("total") / F.col("count"))
df = df.withColumn("pct", F.col("paid") / F.col("billed"))
```

**After -- PySpark:**
```python
# Option 1: Null guard (matches TRY_DIVIDE behavior)
df = df.withColumn("rate",
    F.when(F.col("count") != 0, F.col("total") / F.col("count"))
     .otherwise(F.lit(None).cast("double"))
)

# Option 2: Use SQL TRY_DIVIDE via expr
df = df.withColumn("rate", F.expr("TRY_DIVIDE(total, count)"))

# Option 3: Default to zero instead of null
df = df.withColumn("pct",
    F.when(F.col("billed") != 0, F.col("paid") / F.col("billed"))
     .otherwise(F.lit(0).cast("double"))
)
```

---

### 2.3 Array Access to TRY_ELEMENT_AT

Out-of-bounds array access previously returned null, now throws ArrayIndexOutOfBoundsException.

**Detect regex (SQL):**
```regex
(?i)\w+\s*\[\s*\d+\s*\]
```

**Detect regex (PySpark):**
```regex
\.getItem\s*\(|F\.element_at\s*\((?!.*try)|\.col\([^)]+\)\[\d+\]
```

**Before -- SQL:**
```sql
SELECT array_col[0] FROM table
SELECT split(full_name, ' ')[0] AS first_name FROM members
SELECT split(diagnosis_codes, ',')[2] AS third_code FROM claims
```

**After -- SQL:**
```sql
-- TRY_ELEMENT_AT is 1-indexed (not 0-indexed like bracket notation)
SELECT TRY_ELEMENT_AT(array_col, 1) FROM table
SELECT TRY_ELEMENT_AT(split(full_name, ' '), 1) AS first_name FROM members
SELECT TRY_ELEMENT_AT(split(diagnosis_codes, ','), 3) AS third_code FROM claims
```

**Before -- PySpark:**
```python
df = df.withColumn("first_name", F.split(F.col("full_name"), " ").getItem(0))
df = df.withColumn("third_code", F.split(F.col("diagnosis_codes"), ",")[2])
```

**After -- PySpark:**
```python
# Option 1: Use try_element_at (1-indexed)
df = df.withColumn("first_name", F.try_element_at(F.split(F.col("full_name"), " "), F.lit(1)))

# Option 2: Bounds check with when/otherwise
df = df.withColumn("third_code",
    F.when(F.size(F.split(F.col("diagnosis_codes"), ",")) >= 3,
           F.split(F.col("diagnosis_codes"), ",").getItem(2))
     .otherwise(F.lit(None).cast("string"))
)

# Option 3: Via SQL expr
df = df.withColumn("first_name", F.expr("TRY_ELEMENT_AT(split(full_name, ' '), 1)"))
```

---

### 2.4 Map Access to TRY_ELEMENT_AT

Missing map key access previously returned null, now throws NoSuchElementException.

**Detect regex (SQL):**
```regex
(?i)\w+\s*\[\s*'[^']+'\s*\]
```

**Detect regex (PySpark):**
```regex
\.getItem\s*\(\s*["']|F\.element_at\s*\((?!.*try)
```

**Before -- SQL:**
```sql
SELECT properties['plan_type'] FROM member_attributes
SELECT config_map['threshold'] FROM pipeline_config
```

**After -- SQL:**
```sql
SELECT TRY_ELEMENT_AT(properties, 'plan_type') FROM member_attributes
SELECT TRY_ELEMENT_AT(config_map, 'threshold') FROM pipeline_config
```

**Before -- PySpark:**
```python
df = df.withColumn("plan_type", F.col("properties").getItem("plan_type"))
df = df.withColumn("plan_type", F.element_at(F.col("properties"), F.lit("plan_type")))
```

**After -- PySpark:**
```python
# Option 1: try_element_at
df = df.withColumn("plan_type", F.try_element_at(F.col("properties"), F.lit("plan_type")))

# Option 2: Guard with map_contains_key
df = df.withColumn("plan_type",
    F.when(F.map_contains_key(F.col("properties"), F.lit("plan_type")),
           F.col("properties").getItem("plan_type"))
     .otherwise(F.lit(None).cast("string"))
)

# Option 3: Via SQL expr
df = df.withColumn("plan_type", F.expr("TRY_ELEMENT_AT(properties, 'plan_type')"))
```

---

### 2.5 Integer Overflow to Type Widening

Integer arithmetic that overflows previously wrapped around silently, now throws ArithmeticException.

**Detect regex (SQL):**
```regex
(?i)\b(INT|INTEGER|SMALLINT|TINYINT)\b.*(\*|\+|\-)
```

**Detect regex (PySpark):**
```regex
\.cast\s*\(\s*["']?(int|integer|short|byte)["']?\s*\).*[\*\+\-]|[\*\+\-].*\.cast\s*\(\s*["']?(int|integer|short|byte)
```

**Before -- SQL:**
```sql
SELECT col_a * col_b FROM claims  -- both INT, product could overflow
SELECT member_count + 2147483647 FROM summary
```

**After -- SQL:**
```sql
SELECT CAST(col_a AS BIGINT) * col_b FROM claims
SELECT CAST(member_count AS BIGINT) + 2147483647 FROM summary
```

**Before -- PySpark:**
```python
df = df.withColumn("product", F.col("col_a") * F.col("col_b"))  # both int columns
```

**After -- PySpark:**
```python
df = df.withColumn("product", F.col("col_a").cast("bigint") * F.col("col_b"))
```

---

### 2.6 Boolean Comparisons to IS TRUE

Comparing boolean columns with integer literals (1/0) can fail under ANSI mode because implicit casts from boolean to integer are not allowed.

**Detect regex (SQL):**
```regex
(?i)\b(boolean_col|is_active|is_deleted|flag)\s*=\s*[01]\b
```

**Detect regex (PySpark):**
```regex
F\.col\([^)]+\)\s*==\s*[01]\s*(?!\.)|\.col\([^)]+\)\s*!=\s*[01]
```

Note: The detect patterns above are heuristic. Scan all boolean-typed columns being compared to integer literals.

**Before -- SQL:**
```sql
SELECT * FROM members WHERE is_active = 1
SELECT * FROM claims WHERE is_denied = 0
WHERE boolean_flag = true  -- this one is fine, no change needed
```

**After -- SQL:**
```sql
SELECT * FROM members WHERE is_active IS TRUE
SELECT * FROM claims WHERE is_denied IS NOT TRUE
-- OR equivalently:
SELECT * FROM claims WHERE NOT is_denied
```

**Before -- PySpark:**
```python
df = df.filter(F.col("is_active") == 1)
df = df.filter(F.col("is_denied") == 0)
```

**After -- PySpark:**
```python
df = df.filter(F.col("is_active") == True)  # noqa: E712 -- PySpark requires == True, not `is True`
df = df.filter(F.col("is_denied") == False)  # noqa: E712
# OR:
df = df.filter(F.col("is_active"))
df = df.filter(~F.col("is_denied"))
```

---

### 2.7 to_timestamp on Invalid Data to try_to_timestamp

`to_timestamp` previously returned null for unparseable strings, now throws SparkDateTimeException.

**Detect regex (SQL):**
```regex
(?i)\bto_timestamp\s*\(
```

**Detect regex (PySpark):**
```regex
F\.to_timestamp\s*\(
```

**Before -- SQL:**
```sql
SELECT to_timestamp(date_str, 'yyyy-MM-dd HH:mm:ss') FROM claims
SELECT to_timestamp(event_time) FROM events
```

**After -- SQL:**
```sql
SELECT try_to_timestamp(date_str, 'yyyy-MM-dd HH:mm:ss') FROM claims
SELECT try_to_timestamp(event_time) FROM events
```

**Before -- PySpark:**
```python
df = df.withColumn("event_ts", F.to_timestamp(F.col("date_str"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("event_ts", F.to_timestamp("event_time"))
```

**After -- PySpark:**
```python
# Use SQL expr for try_to_timestamp (not available as a direct PySpark function in all versions)
df = df.withColumn("event_ts", F.expr("try_to_timestamp(date_str, 'yyyy-MM-dd HH:mm:ss')"))
df = df.withColumn("event_ts", F.expr("try_to_timestamp(event_time)"))
```

---

### 2.8 to_date on Invalid Data to try_to_date

`to_date` previously returned null for unparseable strings, now throws SparkDateTimeException.

**Detect regex (SQL):**
```regex
(?i)\bto_date\s*\(
```

**Detect regex (PySpark):**
```regex
F\.to_date\s*\(
```

**Before -- SQL:**
```sql
SELECT to_date(date_str, 'yyyyMMdd') FROM claims
SELECT to_date(admit_date, 'MM/dd/yyyy') FROM encounters
```

**After -- SQL:**
```sql
SELECT try_to_date(date_str, 'yyyyMMdd') FROM claims
SELECT try_to_date(admit_date, 'MM/dd/yyyy') FROM encounters
```

**Before -- PySpark:**
```python
df = df.withColumn("claim_date", F.to_date(F.col("date_str"), "yyyyMMdd"))
df = df.withColumn("admit_date", F.to_date(F.col("admit_date"), "MM/dd/yyyy"))
```

**After -- PySpark:**
```python
df = df.withColumn("claim_date", F.expr("try_to_date(date_str, 'yyyyMMdd')"))
df = df.withColumn("admit_date", F.expr("try_to_date(admit_date, 'MM/dd/yyyy')"))
```

---

### 2.9 Implicit String-to-Number Casts

ANSI mode is stricter about implicit type coercion. Comparisons between string and numeric columns that relied on implicit casting may now fail.

**Detect regex (SQL):**
```regex
(?i)WHERE\s+\w+\s*=\s*'?\d+'?\s|JOIN\s+.*ON\s+.*\w+\s*=\s*'?\d+'?
```

**Detect regex (PySpark):**
```regex
F\.col\([^)]+\)\s*==\s*["']\d+["']
```

**Before -- SQL:**
```sql
-- If member_id is INT but compared to a string literal:
SELECT * FROM claims WHERE member_id = '12345'
-- If status_code is STRING but compared to a number:
SELECT * FROM claims WHERE status_code = 0
```

**After -- SQL:**
```sql
-- Be explicit about types:
SELECT * FROM claims WHERE member_id = 12345
SELECT * FROM claims WHERE status_code = '0'
```

**Before -- PySpark:**
```python
df = df.filter(F.col("member_id") == "12345")  # member_id is IntegerType
```

**After -- PySpark:**
```python
df = df.filter(F.col("member_id") == 12345)  # match the column's actual type
```

---

### ANSI Migration Summary Detection Table

| Pattern | Detect Regex (SQL) | Detect Regex (PySpark) | Fix |
|---------|-------------------|----------------------|-----|
| CAST to numeric/date | `(?i)\bCAST\s*\(` | `\.cast\s*\(` | TRY_CAST / when-otherwise guard |
| Division | `\w+\s*/\s*\w+` | `F\.col.*\s*/\s*` | TRY_DIVIDE / when guard |
| Array access | `\w+\s*\[\s*\d+\s*\]` | `\.getItem\s*\(\d` | TRY_ELEMENT_AT |
| Map access | `\w+\s*\[\s*'[^']+'\s*\]` | `\.getItem\s*\(\s*["']` | TRY_ELEMENT_AT |
| Integer overflow | INT columns in arithmetic | INT columns in arithmetic | CAST to BIGINT before operation |
| Boolean = int | `bool_col\s*=\s*[01]` | `== 1\|== 0` on boolean cols | IS TRUE / IS NOT TRUE |
| to_timestamp | `(?i)\bto_timestamp\s*\(` | `F\.to_timestamp\s*\(` | try_to_timestamp |
| to_date | `(?i)\bto_date\s*\(` | `F\.to_date\s*\(` | try_to_date |
| Implicit cast | String literal compared to number column | String/number type mismatch | Explicit type match |

---

## 3. Python 3.10 to 3.12 Changes

The serverless environment v4 runs Python 3.12. Code written for Python 3.10 (DBR 13.3) must be checked for compatibility.

### 3.1 Deprecated Features Removed in 3.12

| Feature | Python 3.10 | Python 3.12 | Action |
|---------|-------------|-------------|--------|
| `distutils` module | Available (deprecated) | **Removed** | Replace with `setuptools` or `packaging` |
| `imp` module | Available (deprecated) | **Removed** | Replace with `importlib` |
| `asynchat`, `asyncore` | Available (deprecated) | **Removed** | Replace with `asyncio` |
| `smtpd` module | Available | **Removed** | Replace with `aiosmtpd` |
| `typing.TypedDict` with keyword syntax | Works | Works | No change |
| `collections.MutableMapping` (etc.) | Removed in 3.10 | Removed | Use `collections.abc.MutableMapping` |
| `pkgutil.find_loader()` | Deprecated | **Removed** | Use `importlib.util.find_spec()` |
| `locale.getdefaultlocale()` | Deprecated | **Removed** | Use `locale.getlocale()` |

**Detect regex:**
```regex
\bimport\s+distutils\b|\bfrom\s+distutils\b|\bimport\s+imp\b|\bfrom\s+imp\b|\bimport\s+asynchat\b|\bimport\s+asyncore\b|\bimport\s+smtpd\b|\bpkgutil\.find_loader\b|\blocale\.getdefaultlocale\b
```

**Before:**
```python
import distutils.version
v = distutils.version.LooseVersion("1.2.3")

import imp
mod = imp.find_module("mymodule")
```

**After:**
```python
from packaging import version
v = version.parse("1.2.3")

import importlib
spec = importlib.util.find_spec("mymodule")
```

### 3.2 New Python Features Available

These are not required for migration but are available for use on serverless:

| Feature | Since | Example |
|---------|-------|---------|
| `match`/`case` (structural pattern matching) | 3.10 | Already available on 13.3 |
| `ExceptionGroup` and `except*` | 3.11 | `except* ValueError as eg:` |
| `tomllib` (TOML parser) | 3.11 | `import tomllib; tomllib.loads(s)` |
| `TaskGroup` for asyncio | 3.11 | `async with asyncio.TaskGroup() as tg:` |
| `Self` type hint | 3.11 | `from typing import Self` |
| Type parameter syntax | 3.12 | `def func[T](x: T) -> T:` |
| `override` decorator | 3.12 | `from typing import override` |
| Improved f-string parsing | 3.12 | Nested quotes, backslashes in expressions |

### 3.3 F-String Changes (Python 3.12)

Python 3.12 relaxes f-string restrictions. Code that previously required workarounds now works directly, but **existing workarounds still work** -- no migration needed for f-strings.

**New capabilities in 3.12 (informational only):**
```python
# Nested quotes in f-strings -- now allowed in 3.12
name = f"{'Hello'}"           # Was forbidden in 3.10, now works
msg = f"result: {d['key']}"   # Was forbidden in 3.10, now works

# Backslashes in f-string expressions -- now allowed in 3.12
path = f"{s.replace('\\', '/')}"  # Was forbidden in 3.10, now works

# Multi-line f-string expressions -- now allowed in 3.12
value = f"""{
    some_function(
        arg1,
        arg2
    )
}"""
```

**No action required:** Existing 3.10 f-string code works unchanged in 3.12.

### 3.4 match/case Availability

`match`/`case` was introduced in Python 3.10, so it is already available on DBR 13.3. It continues to work on serverless. No migration needed.

### 3.5 Typing Changes

| Feature | 3.10 | 3.12 | Migration Impact |
|---------|------|------|-----------------|
| `Union[X, Y]` | Use `X \| Y` or `Union[X, Y]` | Both work | None |
| `Optional[X]` | Works | Works | None |
| `TypeAlias` | `from typing import TypeAlias` | Same | None |
| `Self` return type | Not available | `from typing import Self` | Can adopt, not required |
| `type` statement | Not available | `type Point = tuple[int, int]` | Can adopt, not required |
| Generic syntax | `def f(x: list[T])` | `def f[T](x: list[T])` | Can adopt, not required |

**No breaking typing changes require migration.** Existing type hints work as-is.

### 3.6 Library Compatibility Impacts

Some libraries may have compatibility issues between Python 3.10 and 3.12:

**Detect regex (for imports that need version checking):**
```regex
\bimport\s+(numpy|pandas|scipy|scikit-learn|sklearn|pyarrow|cryptography|lxml|Cython|cffi)\b|\bfrom\s+(numpy|pandas|scipy|sklearn|pyarrow|cryptography|lxml|Cython|cffi)\s+import\b
```

| Library | 3.10 Compatible | 3.12 Compatible | Notes |
|---------|----------------|----------------|-------|
| pandas >= 2.0 | Yes | Yes | Serverless provides latest |
| numpy >= 1.24 | Yes | Yes | Serverless provides latest |
| pyarrow >= 12.0 | Yes | Yes | Serverless provides latest |
| scipy >= 1.11 | Yes | Yes | |
| scikit-learn >= 1.3 | Yes | Yes | |
| cryptography >= 41.0 | Yes | Yes | Older versions may fail |
| lxml >= 4.9 | Yes | Yes | Must be cp312 wheel |
| Cython < 3.0 | Yes | **No** | Must upgrade to Cython 3.0+ |

**Key rule:** Any compiled C extension (.so, .pyd) or wheel with `cp310` in the filename will NOT work. Must use `cp312` or `py3-none-any` wheels.

---

## 4. Serverless Compute Restrictions

Serverless compute has a restricted execution environment. Many operations that work on classic clusters are not supported. Each unsupported operation must be detected and rewritten.

### 4.1 .persist() / .cache() / CACHE TABLE -- Remove

Serverless automatically manages memory and caching. Manual cache hints are ignored or cause errors.

**Detect regex (PySpark):**
```regex
\.persist\s*\(|\.cache\s*\(|\.unpersist\s*\(
```

**Detect regex (SQL):**
```regex
(?i)\bCACHE\s+(LAZY\s+)?TABLE\b|\bUNCACHE\s+TABLE\b
```

**Before -- PySpark:**
```python
df = spark.table("claims").cache()
df.count()  # materialize cache
# ... use df multiple times ...
df.unpersist()

large_df = spark.table("encounters").persist(StorageLevel.MEMORY_AND_DISK)
```

**After -- PySpark:**
```python
df = spark.table("claims")
# ... use df multiple times -- serverless auto-manages caching ...

large_df = spark.table("encounters")
# Remove all .persist(), .cache(), .unpersist() calls
# Serverless auto-scaling replaces manual cache management
```

**Before -- SQL:**
```sql
CACHE TABLE claims_cached AS SELECT * FROM claims WHERE year = 2024;
-- ... queries using claims_cached ...
UNCACHE TABLE claims_cached;
```

**After -- SQL:**
```sql
-- Option 1: Use a temp view (serverless optimizes repeated reads)
CREATE OR REPLACE TEMP VIEW claims_filtered AS SELECT * FROM claims WHERE year = 2024;
-- ... queries using claims_filtered ...

-- Option 2: Use a CTE
WITH claims_filtered AS (SELECT * FROM claims WHERE year = 2024)
SELECT ... FROM claims_filtered ...
```

---

### 4.2 REFRESH TABLE -- Remove

**Detect regex (SQL):**
```regex
(?i)\bREFRESH\s+TABLE\b
```

**Before -- SQL:**
```sql
REFRESH TABLE claims_bronze;
```

**After -- SQL:**
```sql
-- Remove entirely. Serverless automatically refreshes table metadata.
-- No replacement needed.
```

---

### 4.3 MSCK REPAIR TABLE -- Remove

**Detect regex (SQL):**
```regex
(?i)\bMSCK\s+REPAIR\s+TABLE\b
```

**Before -- SQL:**
```sql
MSCK REPAIR TABLE external_claims;
```

**After -- SQL:**
```sql
-- Remove entirely. Not supported on serverless.
-- For external tables that need partition discovery, use:
-- ALTER TABLE external_claims RECOVER PARTITIONS;
-- But this is only needed for non-Delta external Hive tables.
-- For Delta tables, partitions are automatic.
```

---

### 4.4 REFRESH MATERIALIZED VIEW / CREATE MATERIALIZED VIEW -- Must Use SQL Warehouse

**Detect regex (SQL):**
```regex
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b|\bCREATE\s+(OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
```

**Before -- SQL:**
```sql
CREATE MATERIALIZED VIEW claims_summary AS
  SELECT plan_id, COUNT(*) AS claim_count FROM claims GROUP BY plan_id;

REFRESH MATERIALIZED VIEW claims_summary;
```

**After:** These operations are **not supported on serverless general compute**. They must be executed on a SQL Warehouse or via a Declarative Pipeline (DLT). Move materialized view management to a separate job using a SQL Warehouse task type.

---

### 4.5 Global Temp Views -- Convert to Session Temp Views or Tables

Global temp views (`global_temp.view_name`) are not supported on serverless.

**Detect regex (PySpark):**
```regex
\.createGlobalTempView\s*\(|\.createOrReplaceGlobalTempView\s*\(
```

**Detect regex (SQL):**
```regex
(?i)\bglobal_temp\.\w+|\bCREATE\s+(OR\s+REPLACE\s+)?GLOBAL\s+TEMP(ORARY)?\s+VIEW\b
```

**Before -- PySpark:**
```python
df.createGlobalTempView("shared_claims")
result = spark.sql("SELECT * FROM global_temp.shared_claims")
```

**After -- PySpark:**
```python
# Option 1: Session-scoped temp view (most common replacement)
df.createOrReplaceTempView("shared_claims")
result = spark.sql("SELECT * FROM shared_claims")

# Option 2: Write to a UC table if cross-notebook access is needed
df.write.mode("overwrite").saveAsTable("claims_temp")
```

**Before -- SQL:**
```sql
CREATE OR REPLACE GLOBAL TEMP VIEW shared_claims AS SELECT * FROM claims;
SELECT * FROM global_temp.shared_claims;
```

**After -- SQL:**
```sql
CREATE OR REPLACE TEMP VIEW shared_claims AS SELECT * FROM claims;
SELECT * FROM shared_claims;
```

---

### 4.6 RDD APIs -- Rewrite as DataFrame Operations

RDD operations are not supported on serverless. All RDD-based code must be rewritten using DataFrame API.

#### sc.parallelize() -- Use spark.createDataFrame()

**Detect regex:**
```regex
sc\.parallelize\s*\(|spark\.sparkContext\.parallelize\s*\(
```

**Before:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
df = rdd.toDF(["letter", "number"])

# Or creating reference data
lookup_rdd = sc.parallelize([(1, "Active"), (2, "Inactive"), (3, "Pending")])
lookup_df = lookup_rdd.toDF(["status_id", "status_name"])
```

**After:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Option 1: spark.createDataFrame with explicit schema (preferred)
schema = StructType([
    StructField("letter", StringType(), True),
    StructField("number", IntegerType(), True),
])
df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], schema=schema)

# Option 2: spark.createDataFrame with column names (simpler for small data)
df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["letter", "number"])

# Lookup data
lookup_df = spark.createDataFrame(
    [(1, "Active"), (2, "Inactive"), (3, "Pending")],
    ["status_id", "status_name"]
)
```

#### sc.textFile() -- Use spark.read.text()

**Detect regex:**
```regex
sc\.textFile\s*\(|spark\.sparkContext\.textFile\s*\(
```

**Before:**
```python
rdd = sc.textFile("/mnt/data/raw/claims.csv")
header = rdd.first()
data_rdd = rdd.filter(lambda line: line != header)
parsed_rdd = data_rdd.map(lambda line: line.split(","))
df = parsed_rdd.toDF(["claim_id", "amount", "date"])
```

**After:**
```python
df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/data/raw/claims.csv"))

# Or for truly unstructured text:
df = spark.read.text("/mnt/data/raw/claims.csv")
# df has a single column "value" containing each line
```

#### rdd.map() -- Use DataFrame .withColumn() or UDF

**Detect regex:**
```regex
\.rdd\.|\.map\s*\(lambda|\.flatMap\s*\(lambda|\.mapPartitions\s*\(|\.foreach\s*\(lambda
```

**Before:**
```python
# RDD map to transform data
result_rdd = df.rdd.map(lambda row: (row.claim_id, row.amount * 1.1, row.status.upper()))
result_df = result_rdd.toDF(["claim_id", "adjusted_amount", "status_upper"])

# RDD flatMap to explode data
exploded_rdd = df.rdd.flatMap(lambda row: [(row.id, code) for code in row.codes.split(",")])
exploded_df = exploded_rdd.toDF(["id", "code"])
```

**After:**
```python
# DataFrame withColumn (preferred -- fully distributed)
result_df = (df
    .select(
        F.col("claim_id"),
        (F.col("amount") * 1.1).alias("adjusted_amount"),
        F.upper(F.col("status")).alias("status_upper")
    )
)

# For explode:
exploded_df = (df
    .withColumn("code", F.explode(F.split(F.col("codes"), ",")))
    .select("id", "code")
)

# For complex transformations not expressible in DataFrame API, use a UDF:
@F.udf(returnType=StringType())
def complex_transform(value):
    # ... complex logic ...
    return result

result_df = df.withColumn("new_col", complex_transform(F.col("old_col")))
```

---

### 4.7 dbutils.library.install / restartPython -- Use requirements.txt

**Detect regex:**
```regex
dbutils\.library\.(install|installPyPI|restartPython|list)|dbutils\.library\.install
```

**Before:**
```python
dbutils.library.installPyPI("openpyxl", version="3.1.2")
dbutils.library.restartPython()
import openpyxl
```

**After:**
No in-notebook equivalent. Add the dependency to requirements.txt:
```
# /Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
openpyxl==3.1.2
```
Then remove the `dbutils.library` calls from the notebook entirely.

---

### 4.8 %pip install -- Use requirements.txt or Environment Pane

**Detect regex:**
```regex
%pip\s+install|%pip\s+uninstall
```

**Before:**
```python
%pip install openpyxl==3.1.2 xlrd==2.0.1
%pip install /Volumes/dev_catalog/default/wheels/custom_lib-1.0-py3-none-any.whl
```

**After:**
Remove all `%pip install` cells. Add dependencies to requirements.txt:
```
# /Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
openpyxl==3.1.2
xlrd==2.0.1
/Volumes/${env}_catalog/default/wheels/custom_lib-1.0-py3-none-any.whl
```

Or configure via the environment pane in the job definition.

**Important:** Custom wheels must be compiled for Python 3.12 (`cp312`) or be pure Python (`py3-none-any`). Wheels built for `cp310` will NOT work.

---

### 4.9 Multithreading with concurrent.futures -- Use Workflows/For-Each Tasks

**Detect regex:**
```regex
concurrent\.futures|ThreadPoolExecutor|ProcessPoolExecutor|threading\.Thread|multiprocessing\.Process
```

**Before:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

tables = ["claims", "members", "providers", "encounters"]

def process_table(table_name):
    df = spark.table(table_name)
    df.write.mode("overwrite").saveAsTable(f"silver_{table_name}")

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(process_table, t): t for t in tables}
    for future in as_completed(futures):
        table = futures[future]
        future.result()
        print(f"Completed {table}")
```

**After:**

Option 1: Sequential processing (simplest, often sufficient because serverless auto-scales):
```python
tables = ["claims", "members", "providers", "encounters"]

for table_name in tables:
    df = spark.table(table_name)
    df.write.mode("overwrite").saveAsTable(f"silver_{table_name}")
    print(f"Completed {table_name}")
```

Option 2: Use Databricks Workflows with for-each tasks (preferred for true parallelism):
Move each table processing to a separate notebook and use a for-each task in the job definition with the table names as input.

Option 3: `dbutils.notebook.run` in a loop (parallel via Databricks jobs API):
```python
import json

tables = ["claims", "members", "providers", "encounters"]
for table_name in tables:
    dbutils.notebook.run("process_table", timeout_seconds=3600, arguments={"table_name": table_name})
```

---

### 4.10 spark.sparkContext -- Limited Access

**Detect regex:**
```regex
spark\.sparkContext\b|sc\.\w+(?!\.)|spark\.sparkContext\.setLocalProperty|sc\.setLogLevel|sc\.addFile|sc\.addPyFile|sc\.broadcast\(
```

**Before:**
```python
sc = spark.sparkContext
sc.setLogLevel("WARN")
sc.addFile("/path/to/config.json")
broadcast_var = sc.broadcast(lookup_dict)
```

**After:**
```python
# setLogLevel -- remove, not configurable on serverless
# sc.addFile -- use Volumes instead
#   Read the file from a Volume path directly in the notebook

# broadcast -- use DataFrame broadcast hint instead
lookup_df = spark.createDataFrame(list(lookup_dict.items()), ["key", "value"])
# Then use F.broadcast(lookup_df) in joins

# For broadcast variables in UDFs, pass the data directly:
lookup_dict = {"key1": "val1", "key2": "val2"}

@F.udf(returnType=StringType())
def lookup_udf(key):
    return lookup_dict.get(key)
# Note: Python UDFs capture closure variables. This works on serverless.
```

---

### Serverless Restrictions Summary Table

| Operation | Detect Pattern | Replacement |
|-----------|---------------|-------------|
| `.persist()` / `.cache()` | `\.persist\(\|\.cache\(` | Remove -- serverless auto-manages |
| `.unpersist()` | `\.unpersist\(` | Remove |
| `CACHE TABLE` | `CACHE\s+TABLE` | Remove or use temp view |
| `UNCACHE TABLE` | `UNCACHE\s+TABLE` | Remove |
| `REFRESH TABLE` | `REFRESH\s+TABLE` | Remove -- auto-handled |
| `MSCK REPAIR TABLE` | `MSCK\s+REPAIR` | Remove |
| `REFRESH MATERIALIZED VIEW` | `REFRESH\s+MATERIALIZED` | Move to SQL Warehouse task |
| `CREATE MATERIALIZED VIEW` | `CREATE.*MATERIALIZED\s+VIEW` | Move to SQL Warehouse task |
| Global temp views | `global_temp\.\|createGlobalTempView` | Session temp view or table |
| `sc.parallelize()` | `sc\.parallelize` | `spark.createDataFrame()` |
| `sc.textFile()` | `sc\.textFile` | `spark.read.text()` |
| `rdd.map()` | `\.rdd\.\|\.map\(lambda` | DataFrame `.withColumn()` or UDF |
| `dbutils.library.install` | `dbutils\.library\.install` | requirements.txt |
| `%pip install` | `%pip\s+install` | requirements.txt |
| `concurrent.futures` | `concurrent\.futures\|ThreadPoolExecutor` | Sequential or for-each task |
| `spark.sparkContext` | `spark\.sparkContext\b\|sc\.` | DataFrame API equivalents |

---

## 5. Spark Configuration Migration

### 5.1 Configs SUPPORTED on Serverless

These configurations can be set on serverless compute and will be respected:

| Config | Notes | Default on Serverless |
|--------|-------|-----------------------|
| `spark.sql.shuffle.partitions` | Controls shuffle partition count | `auto` (AQE manages) |
| `spark.sql.adaptive.enabled` | Adaptive Query Execution | `true` (always) |
| `spark.sql.adaptive.coalescePartitions.enabled` | AQE partition coalescing | `true` |
| `spark.sql.adaptive.skewJoin.enabled` | AQE skew join handling | `true` |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | Target partition size for AQE | Managed |
| `spark.sql.ansi.enabled` | ANSI SQL compliance | `true` (always -- setting is a no-op) |
| `spark.databricks.delta.optimizeWrite.enabled` | Auto-optimize writes | `true` |
| `spark.databricks.delta.autoCompact.enabled` | Auto-compact small files | `true` |
| `spark.databricks.delta.merge.repartitionBeforeWrite.enabled` | Repartition before merge writes | `true` |
| `spark.databricks.delta.properties.defaults.minReaderVersion` | Default reader protocol | Managed |
| `spark.databricks.delta.properties.defaults.minWriterVersion` | Default writer protocol | Managed |
| `spark.databricks.delta.schema.autoMerge.enabled` | Auto merge schemas | `false` |
| `spark.databricks.delta.retentionDurationCheck.enabled` | Retention check for VACUUM | `true` |
| `spark.sql.files.maxPartitionBytes` | Max partition bytes for file scan | Managed |
| `spark.sql.autoBroadcastJoinThreshold` | Threshold for broadcast joins | Managed |
| `spark.sql.sources.partitionOverwriteMode` | STATIC or DYNAMIC | `STATIC` |
| `spark.databricks.io.cache.enabled` | Disk cache | `true` |
| `spark.sql.legacy.timeParserPolicy` | Date/time parsing policy | `EXCEPTION` (ANSI) |

### 5.2 Configs NOT SUPPORTED on Serverless -- Remove

These configurations are managed by serverless infrastructure and must be removed. Setting them will either be ignored or cause errors.

**Detect regex:**
```regex
spark\.conf\.set\s*\(\s*["'](spark\.executor\.|spark\.driver\.|spark\.dynamicAllocation\.|spark\.shuffle\.service\.|spark\.sql\.warehouse\.dir|spark\.hadoop\.|spark\.serializer|spark\.databricks\.cluster\.|spark\.master|spark\.submit\.|spark\.yarn\.|spark\.mesos\.|spark\.kubernetes\.)
```

| Config Pattern | Action | Why |
|---------------|--------|-----|
| `spark.executor.memory` | **Remove** | Managed by serverless |
| `spark.executor.cores` | **Remove** | Managed by serverless |
| `spark.executor.instances` | **Remove** | Managed by serverless |
| `spark.driver.memory` | **Remove** | Managed by serverless |
| `spark.driver.cores` | **Remove** | Managed by serverless |
| `spark.driver.maxResultSize` | **Remove** | Managed by serverless |
| `spark.dynamicAllocation.enabled` | **Remove** | Always enabled, managed |
| `spark.dynamicAllocation.minExecutors` | **Remove** | Managed by serverless |
| `spark.dynamicAllocation.maxExecutors` | **Remove** | Managed by serverless |
| `spark.dynamicAllocation.initialExecutors` | **Remove** | Managed by serverless |
| `spark.shuffle.service.enabled` | **Remove** | Managed by serverless |
| `spark.shuffle.service.port` | **Remove** | Managed by serverless |
| `spark.sql.warehouse.dir` | **Remove** | Not applicable on serverless |
| `spark.hadoop.*` | **Remove** | Not configurable on serverless |
| `spark.serializer` | **Remove** | Managed by serverless |
| `spark.databricks.cluster.*` | **Remove** | Not applicable |
| `spark.master` | **Remove** | Not applicable |
| `spark.submit.*` | **Remove** | Not applicable |
| `spark.jars` | **Remove** | Use requirements.txt or environment |
| `spark.jars.packages` | **Remove** | Use requirements.txt |
| Custom JVM options (`-Xmx`, `-XX:*`) | **Remove** | Not supported |

**Before:**
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
spark.conf.set("spark.hadoop.fs.azure.account.key.storage.dfs.core.windows.net", key)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

**After:**
```python
# All of the above are REMOVED. No replacement needed.
# Serverless manages all infrastructure configuration automatically.
# If you were setting spark.hadoop.* for Azure storage access,
# this is now handled by Unity Catalog external locations.
```

### 5.3 Configs with Changed Defaults Between 13.3 and Serverless

| Config | DBR 13.3 Default | Serverless Default | Impact |
|--------|-----------------|-------------------|--------|
| `spark.sql.ansi.enabled` | `false` | `true` (**mandatory**) | See Section 2 -- all ANSI fixes required |
| `spark.sql.sources.default` | `parquet` | `delta` | `spark.read`/`spark.write` without explicit format now uses delta |
| `spark.sql.adaptive.enabled` | `true` | `true` (more aggressive) | AQE may produce different partition counts -- usually better |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` (more aggressive) | More aggressive small partition coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` (more aggressive) | Better skew handling |
| `spark.sql.legacy.timeParserPolicy` | `LEGACY` in some configs | `EXCEPTION` | Strict date/time parsing (see ANSI fixes) |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` (unless configured) | `true` | Writes automatically optimized |
| `spark.databricks.delta.autoCompact.enabled` | `false` (unless configured) | `true` | Small files automatically compacted |

**Key action:** If any notebook explicitly reads without specifying format and depends on parquet behavior:

**Before:**
```python
# This read parquet on 13.3, reads delta on serverless
df = spark.read.load("/path/to/data")
```

**After:**
```python
# Be explicit about format
df = spark.read.format("parquet").load("/path/to/data")
```

### 5.4 Detecting and Evaluating All spark.conf.set Calls

**Detect regex (PySpark):**
```regex
spark\.conf\.set\s*\(|spark\.conf\.get\s*\(|sqlContext\.setConf\s*\(|SET\s+spark\.\w+
```

**Detect regex (SQL):**
```regex
(?i)\bSET\s+spark\.\w+
```

**Migration script pattern:**
```python
# Audit all spark.conf.set calls in a notebook
# For each one, determine if the config is:
# 1. SUPPORTED on serverless -- keep as-is
# 2. NOT SUPPORTED -- remove with comment
# 3. CHANGED DEFAULT -- evaluate if explicit override is needed

SUPPORTED_PREFIXES = [
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.",
    "spark.databricks.delta.",
    "spark.sql.files.",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.sources.partitionOverwriteMode",
    "spark.sql.ansi.enabled",  # no-op but harmless
    "spark.databricks.io.cache.",
    "spark.sql.legacy.timeParserPolicy",
]

REMOVE_PREFIXES = [
    "spark.executor.",
    "spark.driver.",
    "spark.dynamicAllocation.",
    "spark.shuffle.service.",
    "spark.sql.warehouse.dir",
    "spark.hadoop.",
    "spark.serializer",
    "spark.databricks.cluster.",
    "spark.master",
    "spark.submit.",
    "spark.yarn.",
    "spark.mesos.",
    "spark.kubernetes.",
    "spark.jars",
]
```

---

## 6. Environment Variable Migration

### 6.1 os.environ.get() to dbutils.widgets.get()

On serverless compute, environment variables set at the cluster level are not available. Job parameters are passed via widgets.

**Detect regex:**
```regex
os\.environ\s*\[|os\.environ\.get\s*\(|os\.getenv\s*\(
```

**Before:**
```python
import os

env = os.environ.get("ENV", "dev")
catalog = f"{env}_catalog"
source_path = os.environ.get("SOURCE_PATH", "/mnt/landing/claims")
batch_date = os.environ.get("BATCH_DATE")
max_retries = int(os.environ.get("MAX_RETRIES", "3"))
```

**After:**
```python
# Define widgets at the top of the notebook (must be first command in many cases)
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("source_path", "/mnt/landing/claims", "Source Path")
dbutils.widgets.text("batch_date", "", "Batch Date")
dbutils.widgets.text("max_retries", "3", "Max Retries")

# Read widget values
env = dbutils.widgets.get("env")
catalog = f"{env}_catalog"
source_path = dbutils.widgets.get("source_path")
batch_date = dbutils.widgets.get("batch_date")
max_retries = int(dbutils.widgets.get("max_retries"))
```

### 6.2 How Job Parameters Work on Serverless

In the job JSON, parameters are defined in the `parameters` block of each task. These are automatically mapped to notebook widgets:

```json
{
  "notebook_task": {
    "notebook_path": "/Workspace/path/to/notebook",
    "base_parameters": {
      "env": "{{env}}",
      "source_path": "/mnt/landing/claims",
      "batch_date": "{{job.start_time.iso_date}}"
    }
  }
}
```

At runtime, `dbutils.widgets.get("env")` returns the value passed from the job parameter, or the default defined in `dbutils.widgets.text()` if running interactively.

### 6.3 Widget Parameter Patterns with Defaults

**Pattern for required parameters (no default):**
```python
dbutils.widgets.text("env", "", "Environment")
env = dbutils.widgets.get("env")
if not env:
    raise ValueError("Required parameter 'env' not provided")
```

**Pattern for optional parameters with defaults:**
```python
dbutils.widgets.text("parallelism", "8", "Processing Parallelism")
parallelism = int(dbutils.widgets.get("parallelism"))
```

**Pattern for dropdown parameters:**
```python
dbutils.widgets.dropdown("env", "dev", ["dev", "uat", "prod"], "Environment")
env = dbutils.widgets.get("env")
```

**Pattern for boolean-like parameters:**
```python
dbutils.widgets.text("dry_run", "false", "Dry Run")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
```

### 6.4 Common Molina Environment Patterns

```python
# Standard Molina notebook header for serverless
dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env")

spark.sql(f"USE CATALOG {env}_catalog")

# All subsequent table references use 2-part names:
df = spark.table("claims_schema.claims_bronze")
```

---

## 7. Job JSON Transformation

### 7.1 Complete Before/After JSON Template

**Before (Classic Cluster):**
```json
{
  "name": "claims_etl_pipeline",
  "tags": {
    "team": "data-engineering",
    "domain": "claims"
  },
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 4,
        "spark_conf": {
          "spark.sql.shuffle.partitions": "200",
          "spark.executor.memory": "8g",
          "spark.dynamicAllocation.maxExecutors": "10"
        },
        "azure_attributes": {
          "availability": "ON_DEMAND_AZURE"
        },
        "init_scripts": [
          {
            "workspace": {
              "destination": "/Workspace/init/install_deps.sh"
            }
          }
        ],
        "custom_tags": {
          "cost_center": "12345"
        }
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "job_cluster_key": "etl_cluster",
      "notebook_task": {
        "notebook_path": "/Workspace/pipelines/claims/bronze_ingest",
        "base_parameters": {
          "source_path": "/mnt/landing/claims"
        }
      },
      "timeout_seconds": 3600
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingest"}],
      "job_cluster_key": "etl_cluster",
      "notebook_task": {
        "notebook_path": "/Workspace/pipelines/claims/silver_transform",
        "base_parameters": {}
      },
      "timeout_seconds": 7200
    }
  ],
  "max_concurrent_runs": 1
}
```

**After (Serverless):**
```json
{
  "name": "claims_etl_pipeline",
  "tags": {
    "team": "data-engineering",
    "domain": "claims"
  },
  "environments": [
    {
      "environment_key": "serverless_environment_v1",
      "spec": {
        "client": "4",
        "dependencies": [
          "/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
        ]
      }
    }
  ],
  "parameters": [
    {
      "name": "env",
      "default": "dev"
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "environment_key": "serverless_environment_v1",
      "notebook_task": {
        "notebook_path": "/Workspace/pipelines/claims/bronze_ingest",
        "base_parameters": {
          "env": "{{job.parameters.env}}",
          "source_path": "/mnt/landing/claims"
        }
      },
      "timeout_seconds": 3600
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingest"}],
      "environment_key": "serverless_environment_v1",
      "notebook_task": {
        "notebook_path": "/Workspace/pipelines/claims/silver_transform",
        "base_parameters": {
          "env": "{{job.parameters.env}}"
        }
      },
      "timeout_seconds": 7200
    }
  ],
  "queue": {
    "enabled": true
  },
  "max_concurrent_runs": 1
}
```

### 7.2 Key Changes Summary

| Change | Classic | Serverless |
|--------|---------|-----------|
| Cluster definition | `job_clusters` block with `new_cluster` spec | **Remove entirely** |
| Task compute reference | `job_cluster_key: "etl_cluster"` | `environment_key: "serverless_environment_v1"` |
| Environment spec | N/A | `environments` block with `client: "4"` and `dependencies` |
| Dependencies | Init scripts, cluster libraries, %pip | `dependencies` array pointing to requirements.txt on Volume |
| Parameters | Cluster env vars or task `base_parameters` | Top-level `parameters` block + `{{job.parameters.X}}` in tasks |
| Queue | Optional | **Add** `"queue": {"enabled": true}` |
| Performance target | N/A | Optional: `"performance_target": "PERFORMANCE_OPTIMIZED"` |
| Spark configs | In `spark_conf` under cluster | Supported configs set in notebook code |
| Init scripts | Under `new_cluster.init_scripts` | **Remove** -- use requirements.txt |

### 7.3 Environment Block Detail

```json
"environments": [
  {
    "environment_key": "serverless_environment_v1",
    "spec": {
      "client": "4",
      "dependencies": [
        "/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
      ]
    }
  }
]
```

- `client: "4"` = environment version 4 (Python 3.12, latest Spark)
- `dependencies` = path to requirements.txt on a Unity Catalog Volume
- `%env%` is substituted at deploy time by the CI/CD pipeline

### 7.4 Parameters Block with %env% Substitution

```json
"parameters": [
  {
    "name": "env",
    "default": "dev"
  },
  {
    "name": "batch_date",
    "default": ""
  }
]
```

In task `base_parameters`, reference job-level parameters:
```json
"base_parameters": {
  "env": "{{job.parameters.env}}",
  "batch_date": "{{job.parameters.batch_date}}"
}
```

---

## 8. Package and Dependency Analysis

### 8.1 Audit All Dependencies

**Detect all dependency declarations:**

```regex
# %pip install in notebooks
%pip\s+install\s+(.+)

# dbutils.library calls
dbutils\.library\.(installPyPI|install)\s*\(([^)]+)\)

# import statements (to identify what's actually used)
^import\s+(\w+)|^from\s+(\w+)\s+import
```

### 8.2 Dependency Decision Matrix

For each package found, determine:

| Question | If Yes | If No |
|----------|--------|-------|
| Is it pre-installed on serverless? | No action needed | Continue to next question |
| Can it be replaced with native PySpark/SQL? | Replace -- eliminates dependency | Continue |
| Is it a pure Python package? | Add to requirements.txt | Continue |
| Is it a compiled package with cp312 wheel? | Add to requirements.txt | Continue |
| Does it have JVM dependencies? | **Cannot use on serverless** -- must rewrite | N/A |

### 8.3 Common Package Replacements

#### spark-excel (com.crealytics.spark.excel) -- Replace with pandas + openpyxl

**Detect regex:**
```regex
com\.crealytics|spark-excel|spark\.read\.format\s*\(\s*["']com\.crealytics
```

**Before:**
```python
df = (spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .load("/mnt/data/report.xlsx"))
```

**After:**
```python
import pandas as pd

# Read Excel with pandas (openpyxl must be in requirements.txt)
pdf = pd.read_excel("/Volumes/dev_catalog/default/data/report.xlsx", sheet_name="Sheet1")

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)

# For large Excel files, specify schema explicitly:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("claim_date", DateType(), True),
])
df = spark.createDataFrame(pdf, schema=schema)
```

#### koalas -- Replace with Native pandas API on Spark

**Detect regex:**
```regex
import\s+databricks\.koalas|from\s+databricks\.koalas|import\s+pyspark\.pandas
```

**Before:**
```python
import databricks.koalas as ks
kdf = ks.read_csv("/mnt/data/claims.csv")
result = kdf.groupby("plan_id").agg({"amount": "sum"})
```

**After:**
```python
import pyspark.pandas as ps
psdf = ps.read_csv("/mnt/data/claims.csv")
result = psdf.groupby("plan_id").agg({"amount": "sum"})

# Or better -- use native PySpark DataFrame API:
df = spark.read.csv("/mnt/data/claims.csv", header=True, inferSchema=True)
result = df.groupBy("plan_id").agg(F.sum("amount").alias("amount"))
```

#### dbldatagen -- Replace with Native Spark Data Generation

**Detect regex:**
```regex
import\s+dbldatagen|from\s+dbldatagen
```

**Before:**
```python
import dbldatagen as dg

ds = (dg.DataGenerator(spark, name="test_data", rowcount=10000)
    .withColumn("id", "int", minValue=1, maxValue=10000)
    .withColumn("name", "string", template=r"\\w \\w")
)
df = ds.build()
```

**After:**
```python
# Native Spark data generation
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

df = (spark.range(10000)
    .withColumn("id", (F.col("id") + 1).cast("int"))
    .withColumn("name", F.concat(
        F.expr("uuid()").substr(1, 5),
        F.lit(" "),
        F.expr("uuid()").substr(1, 5)
    ))
)
```

### 8.4 Arrow-Optimized UDF Pattern

For packages that cannot be replaced with native PySpark, consider Arrow-optimized UDFs for better performance:

**Standard Python UDF (slow):**
```python
@F.udf(returnType=StringType())
def clean_address(addr):
    import usaddress
    try:
        parsed, _ = usaddress.tag(addr)
        return f"{parsed.get('AddressNumber', '')} {parsed.get('StreetName', '')}"
    except Exception:
        return addr
```

**Arrow-optimized pandas UDF (faster):**
```python
@F.pandas_udf(StringType())
def clean_address_batch(addr_series: pd.Series) -> pd.Series:
    import usaddress

    def _clean(addr):
        if addr is None:
            return None
        try:
            parsed, _ = usaddress.tag(addr)
            return f"{parsed.get('AddressNumber', '')} {parsed.get('StreetName', '')}"
        except Exception:
            return addr

    return addr_series.apply(_clean)

# Usage:
df = df.withColumn("clean_addr", clean_address_batch(F.col("raw_address")))
```

**When to use Arrow/pandas UDFs:**
- Processing large volumes of data with a Python function
- The function operates row-by-row but can be vectorized
- The external library processes individual values (not DataFrames)

### 8.5 requirements.txt Format and Dependency Resolution

**File location:** `/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`

**Format:**
```
# Core dependencies
openpyxl==3.1.2
xlrd==2.0.1
python-dateutil==2.8.2
requests>=2.31.0

# Custom wheels from Volumes
/Volumes/${env}_catalog/default/wheels/custom_lib-1.0-py3-none-any.whl

# ML dependencies (if needed)
scikit-learn>=1.3.0
```

**Rules:**
- One package per line
- Pin versions for reproducibility (`==`) or use minimum versions (`>=`)
- Custom wheels must be cp312 or py3-none-any
- No JVM/JAR dependencies
- No `git+https://` URLs (not supported)
- Volume paths can use `${env}` for environment substitution
- Packages are installed at job startup -- large dependency trees add startup time

**Dependency resolution failures:** If a package fails to install, the job fails before any notebook code runs. Check the driver logs for pip resolution errors. Common causes:
- Package not available for Python 3.12
- Conflicting version requirements between packages
- Network access to PyPI blocked (use Volumes for air-gapped environments)

---

## 9. Performance Optimization

### 9.1 Single-Threaded Operations to Distributed

**Detect regex:**
```regex
for\s+\w+\s+in\s+\w+\.collect\(\)|\.toPandas\(\).*for\s+|\.collect\(\).*for\s+
```

**Before:**
```python
# Anti-pattern: collecting to driver and processing in Python loop
rows = df.collect()
results = []
for row in rows:
    results.append(transform(row))
result_df = spark.createDataFrame(results, schema)
```

**After:**
```python
# Use DataFrame operations or UDFs for distributed processing
@F.udf(returnType=result_schema)
def transform_udf(col1, col2, col3):
    return transform_logic(col1, col2, col3)

result_df = df.withColumn("result", transform_udf(F.col("col1"), F.col("col2"), F.col("col3")))
```

### 9.2 Remove Unnecessary .count() -- Use .first() is not None

**Detect regex:**
```regex
\.count\(\)\s*>\s*0|\.count\(\)\s*==\s*0|\.count\(\)\s*!=\s*0|if\s+.*\.count\(\)
```

**Before:**
```python
# Expensive -- scans entire dataset just to check existence
if df.filter(F.col("status") == "ERROR").count() > 0:
    handle_errors(df)

# Expensive -- materializes count just to check non-empty
if df.count() > 0:
    process(df)
```

**After:**
```python
# Cheap -- stops at first matching row
if df.filter(F.col("status") == "ERROR").first() is not None:
    handle_errors(df)

# Cheap -- reads at most one row
if df.first() is not None:
    process(df)

# For limit-based checks:
if df.limit(1).count() > 0:  # also efficient
    process(df)
```

### 9.3 VACUUM LITE Instead of VACUUM

**Detect regex (SQL):**
```regex
(?i)\bVACUUM\s+\w+(?!\s+LITE)
```

**Before -- SQL:**
```sql
VACUUM claims_bronze RETAIN 168 HOURS;
```

**After -- SQL:**
```sql
-- VACUUM LITE is faster on serverless -- only removes files already marked for deletion
VACUUM claims_bronze LITE;
-- Or with retention:
VACUUM claims_bronze RETAIN 168 HOURS LITE;
```

### 9.4 Liquid Clustering Instead of ZORDER

**Detect regex (SQL):**
```regex
(?i)\bOPTIMIZE\s+\w+\s+ZORDER\s+BY\b
```

**Before -- SQL:**
```sql
OPTIMIZE claims_bronze ZORDER BY (claim_id, member_id);
```

**After -- SQL:**
```sql
-- For new tables, use Liquid Clustering:
ALTER TABLE claims_bronze CLUSTER BY (claim_id, member_id);
-- Then OPTIMIZE without ZORDER:
OPTIMIZE claims_bronze;

-- Note: For existing production tables, the switch from ZORDER to Liquid Clustering
-- should be a deliberate decision, not automatic. Liquid Clustering changes the
-- physical layout strategy. Test with representative queries first.
```

### 9.5 Remove Manual Partition/Shuffle Tuning

**Detect regex:**
```regex
\.repartition\s*\(\s*\d+\s*\)|\.coalesce\s*\(\s*\d+\s*\)|spark\.conf\.set.*shuffle\.partitions
```

Serverless auto-tunes partitioning via AQE. Manual tuning may hurt more than help.

**Before:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
df = df.repartition(100)
df = df.coalesce(1)  # single file output
```

**After:**
```python
# Remove shuffle.partitions -- AQE manages this on serverless
# Remove repartition with hard-coded numbers
df = df  # AQE handles partitioning

# Keep coalesce(1) ONLY if single-file output is genuinely required:
df = df.coalesce(1)  # keep only if downstream consumer requires single file
```

### 9.6 OPTIMIZE and ANALYZE TABLE for External Tables

External tables on serverless do not benefit from Predictive Optimization. Run these manually:

```sql
-- For external Delta tables:
OPTIMIZE external_claims;
ANALYZE TABLE external_claims COMPUTE STATISTICS FOR ALL COLUMNS;

-- Schedule these as separate maintenance jobs
```

### 9.7 Serverless Auto-Scaling Replaces Manual .persist()/.cache()

On classic clusters, `.cache()` and `.persist()` were used to avoid recomputation. On serverless:

- Serverless has intelligent caching built into the query optimizer
- Repeated reads of the same Delta table are automatically cached
- The disk cache (`spark.databricks.io.cache`) is enabled by default
- Manual caching can actually hurt performance by pinning memory

**Recommendation:** Remove all `.cache()`/`.persist()` calls. If a specific query plan shows excessive recomputation (visible in Spark UI), revisit on a case-by-case basis using temp views or materialized intermediate tables.

---

## 10. _metadata Column Conflicts

### What Changed

Starting with DBR 16.4+, row tracking creates a system-managed `_metadata` column on Delta tables. Code that explicitly references `_metadata` (the file metadata struct available during reads) may conflict.

### Detection

**Detect regex:**
```regex
_metadata\b|\["_metadata"\]|F\.col\(\s*["']_metadata["']\s*\)|_metadata\.file_path|_metadata\.file_name|_metadata\.file_size|_metadata\.file_modification_time
```

### Common Pattern

**Before:**
```python
# Reading file metadata during ingestion
df = (spark.read.format("parquet").load("/mnt/landing/claims/")
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("file_mod_time", F.col("_metadata.file_modification_time"))
)
```

**After:**
```python
# If the table has row tracking enabled, _metadata may conflict.
# Use the full struct reference or alias immediately:
df = (spark.read.format("parquet").load("/mnt/landing/claims/")
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("file_mod_time", F.col("_metadata.file_modification_time"))
    .drop("_metadata")  # Drop the metadata column after extracting what you need
)

# For Delta table reads where row tracking is enabled:
# The _metadata column from row tracking and _metadata from file metadata
# are separate. If you need file metadata, read from the raw files, not the table.
```

### Resolution

1. **Check if tables have row tracking enabled:**
   ```sql
   SHOW TBLPROPERTIES claims_bronze ('delta.enableRowTracking');
   ```

2. **If row tracking is enabled AND code references _metadata:**
   - Extract needed file metadata during the initial read from raw files
   - Alias the extracted columns immediately
   - Drop the `_metadata` struct before writing to Delta
   - Do NOT reference `_metadata` on subsequent reads from Delta tables with row tracking

3. **If row tracking is NOT enabled:** No conflict -- `_metadata` file metadata works as before.

---

## 11. Schema Inference on Serverless

### The Problem

`spark.createDataFrame()` without an explicit schema relies on schema inference from the data. Serverless may infer types differently than DBR 13.3, particularly for:

- Mixed-type columns (some rows integer, some string)
- Nested/complex data structures
- None values in all rows of a column
- Decimal precision

### Detection

**Detect regex:**
```regex
spark\.createDataFrame\s*\([^)]*\)(?!\s*,\s*(schema|StructType))
```

More precise: look for `spark.createDataFrame(data)` or `spark.createDataFrame(data, columns)` where `columns` is a list of strings (not a StructType).

### Before (Risky)

```python
# Schema inferred -- may differ between runtimes
data = [("1", 100.0, None), ("2", 200.5, "2024-01-15")]
df = spark.createDataFrame(data, ["id", "amount", "date_str"])
# On 13.3: date_str might infer as StringType
# On serverless: might infer differently if None handling changed
```

### After (Safe)

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("date_str", StringType(), True),
])
data = [("1", 100.0, None), ("2", 200.5, "2024-01-15")]
df = spark.createDataFrame(data, schema=schema)
```

### When Explicit Schema Is Critical

- Nested structures (arrays of structs, maps of arrays)
- Columns that are all null in some runs
- Data used in MERGE operations (schema must match target table exactly)
- DataFrames that feed into typed APIs (write to Delta with enforced schema)
- Test/reference data created inline in notebooks

### When Inference Is Acceptable

- Simple single-column DataFrames with obvious types
- Temporary DataFrames used only for display/debugging
- DataFrames created from `spark.read` with explicit format (format handles inference)

---

## 12. Testing and Validation

### 12.1 Archive and Copy Workflow

**Step 1: Archive original notebooks**
```python
# Before ANY modifications, archive the original notebook
# Use Databricks Repos or Workspace API to copy
import json

source_path = "/Workspace/pipelines/claims/bronze_ingest"
archive_path = "/Workspace/archive/pre_serverless/claims/bronze_ingest"

# Via Workspace API:
notebook_content = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
# Or manually: Export notebook from workspace, store in archive folder
```

**Step 2: Record baseline Delta table versions**
```sql
-- For every output table, record the current version
DESCRIBE HISTORY claims_bronze LIMIT 1;
DESCRIBE HISTORY claims_silver LIMIT 1;
DESCRIBE HISTORY claims_gold LIMIT 1;
-- Store version numbers for comparison after migration
```

**Step 3: Copy notebooks to new location for modification**
```python
# Work on copies, not originals
# /Workspace/pipelines/claims/ -> /Workspace/pipelines_serverless/claims/
```

### 12.2 Parallel Run Strategy

Run the old job on classic compute and the new job on serverless with the same input data. Compare outputs.

**Classic run:** Original notebooks on DBR 13.3 cluster, writing to `{env}_catalog.claims_schema`
**Serverless run:** Modified notebooks on serverless, writing to `{env}_catalog.claims_schema_serverless`

Then compare:
```python
for table_name in ["claims_bronze", "claims_silver", "claims_gold"]:
    original_df = spark.table(f"{env}_catalog.claims_schema.{table_name}")
    serverless_df = spark.table(f"{env}_catalog.claims_schema_serverless.{table_name}")

    # Run validation checks (see conversion_validator skill)
    validate(original_df, serverless_df, table_name)
```

### 12.3 Validation Checks

Run all checks from the `conversion_validator` skill. At minimum:

1. **Schema comparison** -- column names, types, nullability
2. **Row count** -- exact match, zero orphan rows
3. **Null counts per column** -- identical
4. **Aggregate comparison** -- sum, avg, min, max within tolerance
5. **Distinct value comparison** -- identical value sets for string columns
6. **Row-by-row comparison** -- using `eqNullSafe` on primary key join
7. **Date/timestamp deep validation** -- timezone shifts, null/epoch confusion
8. **Null vs empty string** -- the most common silent bug
9. **Numeric precision** -- rounding differences
10. **UDF output consistency** -- for columns produced by UDFs

### 12.4 Serverless-Specific Checks

**Check: Confirm job ran on serverless**
```python
import requests

# Via Jobs API -- check run details
run_id = dbutils.widgets.get("run_id")
# GET /api/2.1/jobs/runs/get?run_id={run_id}
# Verify cluster_spec shows serverless attributes
```

**Check: No unsupported configs set**
```python
# Scan all spark.conf.set calls in the run logs
# Verify none of the REMOVE_PREFIXES configs were set
# If they were set, they may have been silently ignored -- verify expected behavior
```

**Check: Environment key verification**
```python
# From the job JSON, verify every task has:
# "environment_key": "serverless_environment_v1"
# And NO task has "job_cluster_key"
```

**Check: Performance comparison**
```sql
-- Compare runtime and DBU cost from system tables
SELECT
  run_id,
  task_key,
  execution_duration_ms,
  compute_type,
  total_dbu
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '<job_id>'
ORDER BY start_time DESC
LIMIT 20;
```

### 12.5 Test Coverage Requirements

If the source job has no existing tests:

1. **Before migration:** Create baseline snapshots of all output tables
2. **Add data quality assertions** to the notebook (post-processing):
   ```python
   # Assert row count is reasonable
   assert df.count() > 0, "Output table is empty"

   # Assert no unexpected nulls in required columns
   null_check = df.select([
       F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
       for c in required_columns
   ]).collect()[0]
   for col_name in required_columns:
       assert null_check[col_name] == 0, f"Unexpected nulls in {col_name}"

   # Assert schema matches expected
   expected_columns = {"claim_id", "member_id", "amount", "status"}
   actual_columns = set(df.columns)
   assert expected_columns.issubset(actual_columns), f"Missing columns: {expected_columns - actual_columns}"
   ```

---

## 13. Known Issues

### 13.1 Common Serverless Errors by Error Message

| Error Message | Cause | Resolution |
|--------------|-------|------------|
| `UNSUPPORTED_FEATURE: CACHE TABLE is not supported on serverless compute` | `CACHE TABLE` in SQL | Remove CACHE TABLE statements (Section 4.1) |
| `AnalysisException: Global temporary view is not supported` | `createGlobalTempView` | Convert to session temp view (Section 4.5) |
| `Py4JJavaError: ... RDD operations are not supported` | Any RDD API usage | Rewrite with DataFrame API (Section 4.6) |
| `NumberFormatException: invalid input syntax for type integer` | CAST on invalid data with ANSI ON | Use TRY_CAST (Section 2.1) |
| `ArithmeticException: divide by zero` | Division by zero with ANSI ON | Use TRY_DIVIDE (Section 2.2) |
| `ArrayIndexOutOfBoundsException` | Array access out of bounds | Use TRY_ELEMENT_AT (Section 2.3) |
| `NoSuchElementException: key not found` | Map key not found | Use TRY_ELEMENT_AT (Section 2.4) |
| `SparkDateTimeException: Cannot parse` | to_timestamp/to_date on invalid date string | Use try_to_timestamp/try_to_date (Section 2.7/2.8) |
| `ArithmeticException: integer overflow` | Integer arithmetic overflow | Widen to BIGINT (Section 2.5) |
| `ModuleNotFoundError: No module named 'xyz'` | Missing package not in requirements.txt | Add to requirements.txt (Section 8.5) |
| `ImportError: cannot import name` | Package version incompatible with Python 3.12 | Upgrade package version (Section 3.6) |
| `SparkException: Cannot set config 'spark.executor.memory'` | Unsupported config on serverless | Remove config (Section 5.2) |
| `UNSUPPORTED_FEATURE: MSCK REPAIR TABLE` | MSCK REPAIR TABLE | Remove statement (Section 4.3) |
| `UNSUPPORTED_FEATURE: REFRESH TABLE` | REFRESH TABLE on serverless | Remove statement (Section 4.2) |
| `FileNotFoundException: /dbfs/ path not accessible` | DBFS paths not available on serverless | Use Volume paths or Unity Catalog |
| `Permission denied: Cannot access SparkContext` | Direct SparkContext access | Use DataFrame API (Section 4.10) |
| `INVALID_PARAMETER_VALUE: Environment version '4' requires Python 3.12` | Wheel compiled for Python 3.10 | Rebuild wheel for cp312 (Section 8.5) |

### 13.2 Python 3.12 Breaking Changes

| Issue | Symptom | Resolution |
|-------|---------|------------|
| `distutils` removed | `ModuleNotFoundError: No module named 'distutils'` | Use `setuptools` or `packaging` |
| `imp` removed | `ModuleNotFoundError: No module named 'imp'` | Use `importlib` |
| `locale.getdefaultlocale()` removed | `AttributeError` | Use `locale.getlocale()` |
| Cython < 3.0 incompatible | Compilation errors | Upgrade to Cython 3.0+ |
| `asynchat`/`asyncore` removed | `ModuleNotFoundError` | Use `asyncio` |

### 13.3 pandas/Arrow Version Conflicts

Serverless environment v4 ships with specific pandas and PyArrow versions. If your code pins older versions, conflicts may arise.

| Symptom | Cause | Resolution |
|---------|-------|------------|
| `FutureWarning: DataFrame.applymap is deprecated` | pandas 2.0+ renamed `applymap` to `map` | Use `.map()` instead |
| `AttributeError: 'DataFrame' has no attribute 'append'` | pandas 2.0+ removed `DataFrame.append` | Use `pd.concat([df1, df2])` |
| `TypeError: pa.array() got an unexpected keyword argument` | PyArrow API changes | Check pyarrow version and update calls |
| pandas UDF returns wrong types | pandas 2.0 stricter about nullable integer types | Use `pd.Int64Dtype()` for nullable integers |

### 13.4 Delta Protocol on Serverless

| Issue | Cause | Resolution |
|-------|-------|------------|
| Protocol auto-upgrade on write | Serverless uses latest Delta writer | Check `DESCRIBE DETAIL` after first write; older readers may not be able to read |
| Deletion vectors enabled by default | New tables get deletion vectors | No action needed -- performance benefit |
| Row tracking columns | `_metadata` conflicts | See Section 10 |

### 13.5 Widget Parameter Gotchas

| Issue | Symptom | Resolution |
|-------|---------|------------|
| Widget not defined before `get()` | `InputWidgetNotDefined` error | Always call `dbutils.widgets.text()` before `dbutils.widgets.get()` |
| Widget value is empty string, not None | `if not value:` catches empty string but not a missing parameter | Check explicitly: `if value == ""` vs `if value is None` |
| Widgets cached between interactive runs | Old parameter value persists | Use `dbutils.widgets.removeAll()` at notebook start if needed |
| Parameter name case sensitivity | `env` != `ENV` | Standardize on lowercase parameter names |
| Special characters in parameter values | Spaces, quotes in values may cause issues | URL-encode if passing through API |

### 13.6 requirements.txt Resolution Failures

| Issue | Cause | Resolution |
|-------|-------|------------|
| `Could not find a version that satisfies the requirement` | Package not available for Python 3.12 | Check PyPI for cp312 wheels; find alternative package |
| `ResolutionImpossible` | Conflicting version constraints between packages | Relax version pins; use `pip install --dry-run` locally to debug |
| Timeout during package install | Large dependency tree or slow PyPI mirror | Pre-download wheels to a Volume; reference local paths |
| `ERROR: Could not install packages due to an OSError: [Errno 28] No space left on device` | Too many/large dependencies | Reduce dependency count; use lighter alternatives |
| Volume path not found | requirements.txt path incorrect or Volume not mounted | Verify path exists: `dbutils.fs.ls("/Volumes/...")` |
| `%env%` not substituted | Raw `%env%` in path instead of actual environment name | Ensure CI/CD pipeline substitutes `%env%` before deployment |

---

## 14. Step-by-Step Process

### Phase 1: Preparation

**Step 1: Archive original notebooks**
- Export all notebooks in the job to an archive location
- Record the export path and timestamp
- Do NOT modify originals until archive is verified

**Step 2: Record baseline Delta table versions**
```sql
-- For EVERY output table written by the job:
DESCRIBE HISTORY {env}_catalog.{schema}.{table} LIMIT 1;
```
Store version numbers in a tracking spreadsheet or metadata table.

**Step 3: Copy notebooks to new location**
- Copy to a parallel path (e.g., `/Workspace/pipelines_serverless/...`)
- All modifications happen on copies

### Phase 2: Code Migration

**Step 4: Apply ANSI fixes**
- Scan all notebooks for patterns in Section 2
- Apply TRY_CAST, TRY_DIVIDE, TRY_ELEMENT_AT, try_to_timestamp, try_to_date
- Add type widening for integer overflow risks
- Fix boolean comparisons
- Fix implicit string-to-number casts
- **Test each fix individually if possible**

**Step 5: Remove unsupported operations**
- Scan for and remove/replace all patterns in Section 4
- .cache()/.persist() -> remove
- Global temp views -> session temp views
- RDD APIs -> DataFrame rewrites
- %pip install -> requirements.txt
- concurrent.futures -> sequential or for-each tasks
- REFRESH TABLE, MSCK REPAIR TABLE, CACHE TABLE -> remove

**Step 6: Migrate Spark configs**
- Identify all spark.conf.set calls (Section 5)
- Remove unsupported configs
- Evaluate configs with changed defaults
- Keep only supported configs that are explicitly needed

**Step 7: Migrate environment variables**
- Replace os.environ.get() with dbutils.widgets.get() (Section 6)
- Add widget definitions at the top of each notebook
- Map job-level parameters to widget names

### Phase 3: Infrastructure Migration

**Step 8: Audit and migrate dependencies**
- Scan all %pip install and import statements (Section 8)
- Determine if each dependency is needed, replaceable, or compatible
- Create requirements.txt with pinned versions
- Upload to Volume at the expected path

**Step 9: Transform job JSON**
- Remove job_clusters block
- Add environments block with client "4" and requirements.txt path
- Replace job_cluster_key with environment_key on every task
- Add parameters block with env parameter
- Add queue.enabled = true
- Add performance_target if desired
- Update base_parameters to use `{{job.parameters.env}}`

### Phase 4: Testing

**Step 10: Review for performance**
- Remove unnecessary .count() -> .first() is not None
- Consider VACUUM LITE
- Consider Liquid Clustering for new tables
- Remove manual partition/shuffle tuning

**Step 11: Add test coverage**
- If no existing tests, add data quality assertions
- Add schema validation
- Add row count validation
- Add null count checks for critical columns

**Step 12: Run on serverless**
- Deploy the new job JSON
- Trigger a run with the same input parameters
- Monitor for errors in driver logs

**Step 13: Validate against baseline**
- Compare all output tables against baseline (Section 12)
- Run full conversion_validator check suite
- Document any differences and root causes

### Phase 5: Finalization

**Step 14: Generate report**
- Document all changes made to each notebook
- List all removed configs and operations
- List all dependency changes
- Record performance comparison (runtime, DBU cost)
- Flag any manual review items

**Step 15: Run parallel SIT (System Integration Testing)**
- Run both classic and serverless versions in parallel for a defined period
- Compare outputs daily/weekly
- Sign off when outputs match consistently
- Cut over to serverless
- Decommission classic job after grace period

---

## 15. Documentation Links

### Serverless Compute

- [Serverless compute overview](https://docs.databricks.com/en/compute/serverless.html)
- [Serverless compute limitations](https://docs.databricks.com/en/compute/serverless.html#limitations)
- [Supported Spark configurations on serverless](https://docs.databricks.com/en/compute/serverless.html#supported-spark-configurations)
- [Serverless environment versions](https://docs.databricks.com/en/compute/serverless.html#environment-versions)

### Environment Version 4

- [Environment version 4 release notes](https://docs.databricks.com/en/release-notes/serverless.html)
- [Python 3.12 on serverless](https://docs.databricks.com/en/compute/serverless.html#python-version)
- [Managing dependencies on serverless](https://docs.databricks.com/en/compute/serverless.html#manage-dependencies)

### ANSI Mode

- [ANSI mode in Databricks](https://docs.databricks.com/en/sql/language-manual/ansi-compliance.html)
- [TRY_CAST function](https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html)
- [TRY_DIVIDE function](https://docs.databricks.com/en/sql/language-manual/functions/try_divide.html)
- [TRY_ELEMENT_AT function](https://docs.databricks.com/en/sql/language-manual/functions/try_element_at.html)
- [try_to_timestamp function](https://docs.databricks.com/en/sql/language-manual/functions/try_to_timestamp.html)
- [try_to_date function](https://docs.databricks.com/en/sql/language-manual/functions/try_to_date.html)

### Python 3.12

- [Python 3.12 changelog](https://docs.python.org/3/whatsnew/3.12.html)
- [Python 3.11 changelog](https://docs.python.org/3/whatsnew/3.11.html) (intermediate version)
- [Removed modules in 3.12](https://docs.python.org/3/whatsnew/3.12.html#removed-modules)

### Delta Lake

- [Delta Lake on serverless](https://docs.databricks.com/en/delta/index.html)
- [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
- [VACUUM LITE](https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html)
- [Row tracking](https://docs.databricks.com/en/delta/row-tracking.html)
- [Delta protocol versions](https://docs.databricks.com/en/delta/table-properties.html#table-protocol)

### Jobs and Workflows

- [Serverless jobs configuration](https://docs.databricks.com/en/workflows/jobs/serverless-jobs.html)
- [Job parameters](https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html)
- [Environment configuration for jobs](https://docs.databricks.com/en/workflows/jobs/environments.html)
- [For-each tasks](https://docs.databricks.com/en/workflows/jobs/for-each-task.html)

---

## Appendix: Quick Reference -- Regex Patterns for Automated Scanning

This section consolidates all detection patterns for use by automated tools (Genie Code, scripts).

### ANSI Issues (Must Fix)

```
# CAST (SQL)
(?i)\bCAST\s*\(

# .cast() (PySpark)
\.cast\s*\(

# Division (SQL)
(?i)\b\w+\s*/\s*\w+

# Division (PySpark)
F\.col\([^)]+\)\s*/\s*F\.col\(

# Array bracket access (SQL)
\w+\s*\[\s*\d+\s*\]

# .getItem() (PySpark)
\.getItem\s*\(

# Map bracket access (SQL)
\w+\s*\[\s*'[^']+'\s*\]

# to_timestamp (SQL)
(?i)\bto_timestamp\s*\(

# F.to_timestamp (PySpark)
F\.to_timestamp\s*\(

# to_date (SQL)
(?i)\bto_date\s*\(

# F.to_date (PySpark)
F\.to_date\s*\(

# Boolean = integer comparison (SQL)
(?i)\b\w+\s*=\s*[01]\b

# Integer arithmetic on narrow types
(?i)\bCAST\s*\(\s*\w+\s+AS\s+(INT|INTEGER|SMALLINT|TINYINT)\s*\)\s*[\*\+\-]
```

### Serverless Blockers (Must Remove/Rewrite)

```
# Cache operations
\.persist\s*\(
\.cache\s*\(
\.unpersist\s*\(
(?i)\bCACHE\s+(LAZY\s+)?TABLE\b
(?i)\bUNCACHE\s+TABLE\b

# Table maintenance (remove)
(?i)\bREFRESH\s+TABLE\b
(?i)\bMSCK\s+REPAIR\s+TABLE\b

# Materialized views (move to SQL Warehouse)
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b
(?i)\bCREATE\s+(OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b

# Global temp views
\.createGlobalTempView\s*\(
\.createOrReplaceGlobalTempView\s*\(
(?i)\bglobal_temp\.\w+

# RDD operations
sc\.parallelize\s*\(
sc\.textFile\s*\(
\.rdd\.
\.rdd\s*$
sc\.broadcast\s*\(
sc\.addFile\s*\(
sc\.addPyFile\s*\(
sc\.setLogLevel\s*\(
spark\.sparkContext\.parallelize

# Library management
dbutils\.library\.(install|installPyPI|restartPython)
%pip\s+install
%pip\s+uninstall

# Multithreading
concurrent\.futures
ThreadPoolExecutor
ProcessPoolExecutor
threading\.Thread
multiprocessing\.Process

# Environment variables
os\.environ\s*\[
os\.environ\.get\s*\(
os\.getenv\s*\(
```

### Spark Config Issues

```
# Unsupported configs (must remove)
spark\.conf\.set\s*\(\s*["'](spark\.executor\.|spark\.driver\.|spark\.dynamicAllocation\.|spark\.shuffle\.service\.|spark\.sql\.warehouse\.dir|spark\.hadoop\.|spark\.serializer|spark\.databricks\.cluster\.)

# All config set calls (audit each)
spark\.conf\.set\s*\(
sqlContext\.setConf\s*\(
(?i)\bSET\s+spark\.\w+

# _metadata column references
_metadata\b
_metadata\.file_path
_metadata\.file_name
```

### Dependency Issues

```
# External packages
(?i)com\.crealytics
(?i)spark-excel
import\s+databricks\.koalas
from\s+databricks\.koalas
import\s+dbldatagen
import\s+distutils
from\s+distutils
import\s+imp\b
from\s+imp\s+import
```

### Python 3.12 Compatibility

```
# Removed modules
\bimport\s+distutils\b
\bfrom\s+distutils\b
\bimport\s+imp\b
\bfrom\s+imp\b
\bimport\s+asynchat\b
\bimport\s+asyncore\b
\bimport\s+smtpd\b
\bpkgutil\.find_loader\b
\blocale\.getdefaultlocale\b

# pandas 2.0 breaking changes
\.applymap\s*\(
\.append\s*\(.*ignore_index
DataFrame\.append\s*\(
```
