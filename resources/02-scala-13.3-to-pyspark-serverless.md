# Path B: Scala on DBR 13.3 LTS to PySpark on Serverless General Compute

**Migration Path:** Scala (DBR 13.3 LTS, Spark 3.4.1) --> PySpark (Serverless Environment Version 4, Python 3.12)
**Audience:** Genie Code (automated migrations), the customer engineering team (edge case reference)
**Last Updated:** 2026-04-16

---

## Table of Contents

1. [Migration Overview and Compound Changes](#1-migration-overview-and-compound-changes)
2. [Language Conversion: Scala to PySpark](#2-language-conversion-scala-to-pyspark)
3. [ANSI Mode Migration](#3-ansi-mode-migration)
4. [Serverless Compute Restrictions](#4-serverless-compute-restrictions)
5. [Spark Configuration Migration](#5-spark-configuration-migration)
6. [Environment Variable Migration](#6-environment-variable-migration)
7. [Job JSON Transformation](#7-job-json-transformation)
8. [Package Dependency Analysis](#8-package-dependency-analysis)
9. [Performance Optimization Opportunities](#9-performance-optimization-opportunities)
10. [_metadata Column Conflict Detection](#10-_metadata-column-conflict-detection)
11. [Schema Inference Issues](#11-schema-inference-issues)
12. [Testing and Validation Framework](#12-testing-and-validation-framework)
13. [Test Coverage Requirements](#13-test-coverage-requirements)
14. [Known Issues Catalog](#14-known-issues-catalog)
15. [Step-by-Step Migration Process](#15-step-by-step-migration-process)
16. [Documentation Links](#16-documentation-links)

---

## 1. Migration Overview and Compound Changes

### Three Migrations in One

Path B is the most complex migration path. It combines **three distinct migrations** that each carry independent risk:

| Migration Layer | What Changes | Risk Level |
|----------------|-------------|------------|
| **Language Conversion** | Scala --> PySpark (syntax, types, UDFs, operator precedence, null handling) | HIGH -- business logic rewrite |
| **DBR Upgrade** | 13.3 LTS (Spark 3.4.1) --> 16.4+ (Spark 3.5.x) with ANSI mode default ON | MEDIUM -- behavioral changes in error handling |
| **Compute Migration** | Classic clusters --> Serverless General Compute (env v4) | MEDIUM -- unsupported operations, config restrictions, dependency changes |

**Combined risk: CRITICAL.** Each layer can introduce silent data changes. When all three are applied simultaneously, root cause isolation becomes difficult. The order of operations below is designed to minimize that difficulty.

### Order of Operations

**Always apply changes in this sequence:**

```
Step 1: Convert Language (Scala --> PySpark)
  |  Produces: PySpark code that is functionally identical to Scala
  |  Test: Run on DBR 13.3 classic cluster, compare output to Scala baseline
  |
Step 2: Apply DBR 16.4 / ANSI Fixes
  |  Produces: PySpark code safe for ANSI mode
  |  Test: Run on DBR 16.4 classic cluster, compare output to Step 1
  |
Step 3: Apply Serverless Restrictions
  |  Produces: PySpark code safe for serverless
  |  Test: Run on serverless, compare output to Step 2
```

**Why this order matters:** If you apply all three simultaneously and validation fails, you cannot tell whether the failure is from language conversion (UDF null handling), ANSI mode (TRY_CAST returning null differently), or serverless (removed .cache() causing recomputation). Layered application with intermediate validation isolates root causes.

In practice, Steps 2 and 3 may be combined for simple notebooks with no ANSI-sensitive patterns. But for notebooks with UDFs, date parsing, or division operations, the layered approach is mandatory.

### Risk Assessment Matrix

| Pattern Found in Notebook | Language Risk | ANSI Risk | Serverless Risk | Recommended Approach |
|--------------------------|--------------|-----------|-----------------|---------------------|
| UDFs with business logic | HIGH | LOW | LOW | Layer strictly -- validate after Step 1 |
| Division operations | LOW | HIGH | LOW | Can combine Steps 1+2, validate before Step 3 |
| .persist() / .cache() | LOW | LOW | MEDIUM | Can combine Steps 1+2+3 |
| spark-excel library | LOW | LOW | HIGH | Must rewrite during Step 3 |
| Date parsing in UDFs | HIGH | HIGH | LOW | Layer strictly -- validate after each step |
| Simple ETL (read/transform/write) | LOW | LOW | LOW | Can combine all three steps |

---

## 2. Language Conversion: Scala to PySpark

### Reference Skill

The full Scala-to-PySpark conversion guide is in `skills/scala_to_pyspark/skill.md`. That skill covers all syntax translation, type system changes, and behavioral differences in detail. **Do not duplicate that content here.** Instead, this section covers the patterns most commonly encountered in the customer's codebase and the ones most likely to cause silent data changes.

### Critical Patterns Quick Reference

#### Column References

**Detect:**
```regex
\$"[^"]+"|\.as\s*\(|===|=!=
```

**Fix:**
```python
# Scala: $"column_name"
# PySpark:
F.col("column_name")

# Scala: $"col".as("alias")
# PySpark:
F.col("col").alias("alias")

# Scala: $"a" === $"b"
# PySpark:
F.col("a") == F.col("b")

# Scala: $"a" =!= $"b"
# PySpark:
F.col("a") != F.col("b")
```

#### Case Classes to StructType

**Detect:**
```regex
case\s+class\s+\w+\s*\(
```

**Fix:**
```python
# Scala:
# case class RawClaim(claim_id: String, amount: Double, claim_date: String)
# val df = spark.read.schema(Encoders.product[RawClaim].schema).csv(path)

# PySpark:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

raw_claim_schema = StructType([
    StructField("claim_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("claim_date", StringType(), nullable=True),
])
df = spark.read.schema(raw_claim_schema).csv(path)
```

#### UDF Conversion Rules

**Detect:**
```regex
udf\s*\(|UserDefinedFunction|spark\.udf\.register
```

**Fix -- every UDF must:**
1. Specify explicit `returnType`
2. Handle `None` inputs on every code path
3. Return `None` (not `""`, `0`, or `False`) for null semantics
4. Use `is not None` checks, never bare `if x:`

```python
# Scala:
# val cleanCode = udf((s: String) => {
#   Option(s).map(_.trim.toUpperCase).getOrElse(null)
# })

# PySpark:
@F.udf(returnType=StringType())
def clean_code(s):
    if s is not None:
        return s.strip().upper()
    return None  # NOT "" -- must preserve null semantics
```

**UDF returning struct:**
```python
# Scala:
# case class Result(code: String, score: Double)
# val myUdf = udf((x: String) => Result(x, 1.0))

# PySpark:
from pyspark.sql import Row

result_schema = StructType([
    StructField("code", StringType()),
    StructField("score", DoubleType()),
])

@F.udf(returnType=result_schema)
def my_udf(x):
    if x is not None:
        return Row(code=x, score=1.0)
    return None
```

#### Array/Map Access Sugar Pitfalls

**Detect (array access):**
```regex
split\s*\([^)]+\)\s*\(\d+\)|\.getItem\s*\(
```

**Fix:**
```python
# Scala: split($"col", "\\.")(1)
# WRONG: F.split(F.col("col"), "\\.")(1)  -- TypeError: 'Column' not callable
# CORRECT:
F.split(F.col("col"), "\\.").getItem(1)
# or:
F.split(F.col("col"), "\\.")[1]
```

**Detect (map access):**
```regex
typedLit\s*\(\s*Map|\.apply\s*\(\s*\$
```

**Fix:**
```python
# Scala: typedLit(Map("a" -> 1, "b" -> 2))($"key_col")
# WRONG: F.create_map(...)[F.col("key_col")]
# CORRECT:
lookup_map = F.create_map(
    F.lit("a"), F.lit(1),
    F.lit("b"), F.lit(2)
)
F.element_at(lookup_map, F.col("key_col"))
```

#### Boolean Operator Precedence

**Detect:**
```regex
&\s*F\.col|F\.col[^)]+\)\s*&|[^&]&[^&]|\|\s*F\.col|F\.col[^)]+\)\s*\|
```

**Fix -- always parenthesize both sides:**
```python
# Scala: df.filter($"a" > 0 && $"b" < 100)

# WRONG -- Python & has higher precedence than >:
df.filter(F.col("a") > 0 & F.col("b") < 100)
# Evaluates as: a > (0 & b) < 100

# CORRECT:
df.filter((F.col("a") > 0) & (F.col("b") < 100))
```

#### Numeric Precision (Banker's Rounding)

**Detect:**
```regex
BigDecimal|setScale|HALF_UP|\.round\(|round\s*\(
```

**Fix:**
```python
# Scala: BigDecimal(3.315).setScale(2, RoundingMode.HALF_UP)  --> 3.32
# Python: round(3.315, 2)  --> 3.31 (banker's rounding!)

# CORRECT -- faithful HALF_UP translation:
from decimal import Decimal, ROUND_HALF_UP

def round_half_up(value, places):
    if value is None:
        return None
    quantize_str = "0." + "0" * places
    return float(Decimal(str(value)).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP))
```

#### Date Parsing Edge Cases

**Detect:**
```regex
SimpleDateFormat|java\.text\.SimpleDateFormat|setLenient|strptime|parse_date
```

**Fix:**
```python
# Scala SimpleDateFormat (lenient=true by default):
#   "02/29/2023" --> rolls to 03/01/2023
#   "13/01/2024" --> rolls to 01/01/2025

# Python strptime ALWAYS raises ValueError for invalid dates.
# Must add explicit error handling:

from datetime import datetime

def parse_date_lenient(date_str, fmt="%m/%d/%Y"):
    """Matches Scala SimpleDateFormat lenient behavior."""
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str.strip(), fmt).date()
    except (ValueError, AttributeError):
        return None  # or implement rollover logic if Scala baseline depends on it
```

**Integer Division:**

**Detect:**
```regex
(?<!\/)\/(?!\/)(?!\*)
```

**Fix:**
```python
# Scala: 5 / 2 --> 2 (integer division)
# Python: 5 / 2 --> 2.5 (float division)
# Fix: Use // for integer division in UDFs
result = a // b  # not a / b
```

---

## 3. ANSI Mode Migration

### ANSI is Mandatory on Serverless

**There is no escape hatch.** On serverless compute, ANSI mode is always enabled. You cannot set `spark.sql.ansi.enabled = false`. Every ANSI-unsafe pattern in the code MUST be fixed before running on serverless.

This is the correct behavior for healthcare data pipelines -- silent null returns on invalid casts or divide-by-zero mask data quality issues. The fixes below make error handling explicit.

### Complete ANSI Pattern Catalog

#### Pattern 1: Type Casting

**Detect:**
```regex
\.cast\s*\(|CAST\s*\((?!.*TRY_CAST)
```

**Risk:** `CAST("abc" AS INT)` throws `NumberFormatException` in ANSI mode instead of returning null.

**Fix -- SQL:**
```sql
-- Before:
SELECT CAST(amount_str AS INT) FROM claims

-- After:
SELECT TRY_CAST(amount_str AS INT) FROM claims
```

**Fix -- PySpark:**
```python
# Before:
df = df.withColumn("amount_int", F.col("amount_str").cast("int"))

# After -- when/otherwise guard:
df = df.withColumn("amount_int",
    F.when(
        F.col("amount_str").rlike(r"^-?\d+$"),
        F.col("amount_str").cast("int")
    ).otherwise(F.lit(None).cast("int"))
)

# After -- SQL expression with TRY_CAST:
df = df.withColumn("amount_int",
    F.expr("TRY_CAST(amount_str AS INT)")
)
```

**Safe casts that do NOT need TRY_CAST:**
- `INT` --> `BIGINT` (widening -- always safe)
- `FLOAT` --> `DOUBLE` (widening -- always safe)
- `STRING` --> `STRING` (no-op)
- Any cast where the source column type guarantees validity

#### Pattern 2: Division by Zero

**Detect:**
```regex
\s+\/\s+|TRY_DIVIDE|F\.col\([^)]+\)\s*\/\s*F\.col
```

**Risk:** Division by zero throws `ArithmeticException` in ANSI mode instead of returning null.

**Fix -- SQL:**
```sql
-- Before:
SELECT paid_amount / billed_amount FROM claims

-- After (option 1 -- TRY_DIVIDE):
SELECT TRY_DIVIDE(paid_amount, billed_amount) FROM claims

-- After (option 2 -- explicit guard):
SELECT CASE WHEN billed_amount = 0 THEN NULL
       ELSE paid_amount / billed_amount END FROM claims
```

**Fix -- PySpark:**
```python
# Before:
df = df.withColumn("pay_rate", F.col("paid") / F.col("billed"))

# After:
df = df.withColumn("pay_rate",
    F.when(F.col("billed") != 0, F.col("paid") / F.col("billed"))
     .otherwise(F.lit(None).cast("double"))
)
```

#### Pattern 3: Array Index Out of Bounds

**Detect:**
```regex
\.getItem\s*\(|array_col\s*\[|split\s*\([^)]+\)\s*\[|element_at\s*\((?!.*try_element_at)
```

**Risk:** Accessing an array index beyond the array length throws `ArrayIndexOutOfBoundsException` in ANSI mode.

**Fix -- SQL:**
```sql
-- Before:
SELECT codes[5] FROM claims

-- After:
SELECT TRY_ELEMENT_AT(codes, 6) FROM claims
-- NOTE: TRY_ELEMENT_AT is 1-indexed, bracket notation is 0-indexed
```

**Fix -- PySpark:**
```python
# Before:
df = df.withColumn("code", F.col("codes").getItem(5))

# After -- bounds check:
df = df.withColumn("code",
    F.when(F.size(F.col("codes")) > 5, F.col("codes").getItem(5))
     .otherwise(F.lit(None).cast("string"))
)

# After -- SQL expression:
df = df.withColumn("code", F.expr("TRY_ELEMENT_AT(codes, 6)"))
```

#### Pattern 4: Map Key Not Found

**Detect:**
```regex
\.getItem\s*\(.*(?:lit|col)|map_col\s*\[|element_at\s*\(
```

**Risk:** Accessing a map key that does not exist throws `NoSuchElementException` in ANSI mode.

**Fix -- SQL:**
```sql
-- Before:
SELECT config_map['missing_key'] FROM job_config

-- After:
SELECT TRY_ELEMENT_AT(config_map, 'missing_key') FROM job_config
```

**Fix -- PySpark:**
```python
# Before:
df = df.withColumn("val", F.element_at(F.col("config_map"), F.lit("key")))

# After:
df = df.withColumn("val",
    F.when(
        F.map_contains_key(F.col("config_map"), F.lit("key")),
        F.element_at(F.col("config_map"), F.lit("key"))
    ).otherwise(F.lit(None))
)

# Or use TRY_ELEMENT_AT via SQL expression:
df = df.withColumn("val", F.expr("TRY_ELEMENT_AT(config_map, 'key')"))
```

#### Pattern 5: Integer Overflow

**Detect:**
```regex
\*\s*F\.col|F\.col[^)]+\)\s*\*|IntegerType|\.cast\s*\(\s*["']int["']\s*\)\s*[\*\+\-]
```

**Risk:** Integer arithmetic that exceeds `Int.MaxValue` (2,147,483,647) throws `ArithmeticException` in ANSI mode instead of wrapping around.

**Fix -- widen the type before arithmetic:**
```python
# Before:
df = df.withColumn("product", F.col("qty").cast("int") * F.col("unit_price").cast("int"))

# After:
df = df.withColumn("product", F.col("qty").cast("bigint") * F.col("unit_price").cast("bigint"))
```

**Fix -- SQL:**
```sql
-- Before:
SELECT qty * unit_price FROM line_items

-- After:
SELECT CAST(qty AS BIGINT) * unit_price FROM line_items
```

#### Pattern 6: Boolean Comparisons

**Detect:**
```regex
boolean_col\s*=\s*1|boolean_col\s*=\s*true|=\s*(true|false|1|0)\b
```

**Risk:** In ANSI mode, comparing a boolean column to an integer (`boolean_col = 1`) may throw or behave differently.

**Fix:**
```sql
-- Before:
SELECT * FROM claims WHERE is_active = 1

-- After:
SELECT * FROM claims WHERE is_active IS TRUE

-- Before:
SELECT * FROM claims WHERE is_active = 0

-- After:
SELECT * FROM claims WHERE is_active IS NOT TRUE
-- or:
SELECT * FROM claims WHERE is_active IS FALSE
```

**Fix -- PySpark:**
```python
# Before:
df = df.filter(F.col("is_active") == 1)

# After:
df = df.filter(F.col("is_active") == True)  # noqa: E712
# or:
df = df.filter(F.col("is_active"))
```

#### Pattern 7: Date/Timestamp Parsing

**Detect:**
```regex
to_timestamp\s*\(|to_date\s*\(|from_unixtime\s*\(|date_format\s*\(
```

**Risk:** `to_timestamp` and `to_date` throw on invalid date strings in ANSI mode instead of returning null.

**Fix -- SQL:**
```sql
-- Before:
SELECT to_timestamp(date_str, 'yyyy-MM-dd') FROM claims

-- After:
SELECT try_to_timestamp(date_str, 'yyyy-MM-dd') FROM claims

-- Before:
SELECT to_date(date_str, 'MM/dd/yyyy') FROM claims

-- After:
SELECT CASE WHEN date_str RLIKE '^\d{2}/\d{2}/\d{4}$'
       THEN to_date(date_str, 'MM/dd/yyyy')
       ELSE NULL END FROM claims
```

**Fix -- PySpark:**
```python
# Before:
df = df.withColumn("claim_dt", F.to_timestamp("date_str", "yyyy-MM-dd"))

# After:
df = df.withColumn("claim_dt", F.expr("try_to_timestamp(date_str, 'yyyy-MM-dd')"))

# Or with guard:
df = df.withColumn("claim_dt",
    F.when(
        F.col("date_str").rlike(r"^\d{4}-\d{2}-\d{2}$"),
        F.to_timestamp("date_str", "yyyy-MM-dd")
    ).otherwise(F.lit(None).cast("timestamp"))
)
```

### ANSI Pattern Summary Table

| # | Pattern | Detect Regex | Fix Strategy |
|---|---------|-------------|-------------|
| 1 | Type casting | `\.cast\s*\(\|CAST\s*\(` | TRY_CAST or when/otherwise guard |
| 2 | Division | `\s+\/\s+` on column expressions | TRY_DIVIDE or zero guard |
| 3 | Array access | `\.getItem\s*\(\|element_at` | TRY_ELEMENT_AT or bounds check |
| 4 | Map access | `\.getItem\s*\(.*lit\|map_col\[` | TRY_ELEMENT_AT or map_contains_key |
| 5 | Integer overflow | `\.cast\("int"\)\s*[\*\+]` | Widen to BIGINT before arithmetic |
| 6 | Boolean comparison | `=\s*(true\|false\|1\|0)` | IS TRUE / IS FALSE |
| 7 | Date parsing | `to_timestamp\s*\(\|to_date\s*\(` | try_to_timestamp or regex guard |

---

## 4. Serverless Compute Restrictions

### Unsupported Operations

#### 4.1 Caching and Persistence

**Detect:**
```regex
\.persist\s*\(|\.cache\s*\(|\.unpersist\s*\(|CACHE\s+TABLE|UNCACHE\s+TABLE|CACHE\s+LAZY\s+TABLE
```

**Fix -- remove entirely.** Serverless auto-manages memory and caching. Explicit caching is not supported and will throw errors.

```python
# Before:
df = df.cache()
df.count()  # force materialization
# ... multiple actions on df ...
df.unpersist()

# After -- simply remove the cache/unpersist calls:
df = df  # no .cache()
# ... multiple actions on df ...
# no .unpersist() needed
```

```sql
-- Before:
CACHE TABLE my_temp_table;
SELECT * FROM my_temp_table;
UNCACHE TABLE my_temp_table;

-- After -- remove CACHE/UNCACHE:
-- (if my_temp_table is a temp view, just use it directly)
SELECT * FROM my_temp_table;
```

#### 4.2 REFRESH TABLE

**Detect:**
```regex
REFRESH\s+TABLE
```

**Fix -- remove entirely.** Serverless automatically invalidates metadata caches.

```sql
-- Before:
REFRESH TABLE catalog.schema.my_table;
SELECT * FROM catalog.schema.my_table;

-- After:
SELECT * FROM catalog.schema.my_table;
```

#### 4.3 MSCK REPAIR TABLE

**Detect:**
```regex
MSCK\s+REPAIR\s+TABLE
```

**Fix -- remove entirely.** This command is for Hive-style partitioned tables and is not supported on serverless. If the table is a Delta table, it is not needed. If the table is external non-Delta, convert to Delta or use a SQL warehouse for this operation.

```sql
-- Before:
MSCK REPAIR TABLE catalog.schema.external_table;

-- After:
-- Remove entirely for Delta tables
-- For non-Delta: run this on a SQL warehouse, not serverless compute
```

#### 4.4 Materialized Views

**Detect:**
```regex
CREATE\s+MATERIALIZED\s+VIEW|REFRESH\s+MATERIALIZED\s+VIEW|DROP\s+MATERIALIZED\s+VIEW
```

**Fix -- materialized views must be managed via SQL Warehouse or Spark Declarative Pipelines (SDP).**

```sql
-- These operations CANNOT run on serverless general compute:
-- CREATE MATERIALIZED VIEW ...
-- REFRESH MATERIALIZED VIEW ...

-- Instead, create and manage materialized views through:
-- 1. A Spark Declarative Pipeline (SDP)
-- 2. A SQL Warehouse
-- 3. A classic cluster
```

#### 4.5 Global Temp Views

**Detect:**
```regex
createGlobalTempView|createOrReplaceGlobalTempView|global_temp\.|GLOBAL\s+TEMPORARY\s+VIEW|GLOBAL\s+TEMP\s+VIEW
```

**Fix -- convert to session-scoped temp views or permanent tables:**

```python
# Before:
df.createOrReplaceGlobalTempView("shared_data")
spark.sql("SELECT * FROM global_temp.shared_data")

# After -- session temp view (if only used within the same notebook/task):
df.createOrReplaceTempView("shared_data")
spark.sql("SELECT * FROM shared_data")

# After -- permanent table (if shared across tasks):
df.write.mode("overwrite").saveAsTable("catalog.schema.shared_data")
```

#### 4.6 RDD APIs

**Detect:**
```regex
sc\.textFile|sc\.parallelize|sc\.wholeTextFiles|\.rdd\.|rdd\.map|rdd\.flatMap|rdd\.filter|rdd\.reduce|rdd\.collect|SparkContext|\.toRDD|\.mapPartitions
```

**Fix -- rewrite as DataFrame operations:**

```python
# Before (RDD):
rdd = sc.textFile("/path/to/file.txt")
result = rdd.map(lambda line: line.split(",")).filter(lambda x: len(x) > 3)

# After (DataFrame):
df = spark.read.text("/path/to/file.txt")
df = df.withColumn("parts", F.split(F.col("value"), ","))
df = df.filter(F.size(F.col("parts")) > 3)

# Before (RDD from list):
rdd = sc.parallelize([("a", 1), ("b", 2)])
df = rdd.toDF(["key", "value"])

# After (DataFrame directly):
data = [("a", 1), ("b", 2)]
df = spark.createDataFrame(data, ["key", "value"])
```

#### 4.7 Library Installation Commands

**Detect:**
```regex
dbutils\.library\.install|dbutils\.library\.restartPython|%pip\s+install|!pip\s+install
```

**Fix -- move to requirements.txt:**

```python
# Before (in notebook):
dbutils.library.install("pypi", "openpyxl==3.1.2")
dbutils.library.restartPython()

# Before (in notebook):
# %pip install openpyxl==3.1.2 pandas==2.1.4

# After -- add to requirements.txt at:
# /Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
# Contents:
# openpyxl==3.1.2
# pandas==2.1.4

# Remove the install commands from the notebook entirely.
# Dependencies are installed automatically when the serverless environment starts.
```

#### 4.8 Multithreading and Parallelism

**Detect:**
```regex
ThreadPoolExecutor|concurrent\.futures|threading\.Thread|multiprocessing|Pool\(|\.submit\(|asyncio\.gather|dbutils\.notebook\.run.*ThreadPool|Parallel\(|joblib
```

**Fix -- refactor to Databricks Workflows or for-each tasks:**

```python
# Before -- parallel notebook execution:
from concurrent.futures import ThreadPoolExecutor

notebooks = ["nb_01", "nb_02", "nb_03"]
def run_nb(nb):
    return dbutils.notebook.run(f"/path/{nb}", 3600, {"env": env})

with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(run_nb, notebooks))

# After -- use a for-each task in the job JSON (see Section 7),
# or run sequentially if parallelism is not performance-critical:
for nb in notebooks:
    dbutils.notebook.run(f"/path/{nb}", 3600, {"env": env})
```

**Why:** Serverless compute manages resources dynamically. Spawning threads to run notebooks in parallel can cause resource contention and unpredictable behavior. Use Databricks Workflows with for-each tasks for parallel execution.

### Unsupported Libraries

#### JVM/JAR-Based Libraries

**Detect:**
```regex
com\.crealytics|spark-excel|\.jar\b|addJar|sc\.addJar|spark\.jars|--jars|libraryDependencies
```

**Fix for spark-excel (most common case):**

```python
# Before (Scala with spark-excel):
# val df = spark.read
#   .format("com.crealytics.spark.excel")
#   .option("header", "true")
#   .option("dataAddress", "'Sheet1'!A1")
#   .load(path)

# After (PySpark with pandas + openpyxl):
import pandas as pd

# Read Excel from Volume or cloud storage
pandas_df = pd.read_excel(
    "/Volumes/catalog/schema/volume/file.xlsx",
    sheet_name="Sheet1",
    header=0,
    engine="openpyxl"
)
df = spark.createDataFrame(pandas_df)

# For large files, read in chunks:
chunks = pd.read_excel(path, sheet_name="Sheet1", chunksize=10000, engine="openpyxl")
dfs = [spark.createDataFrame(chunk) for chunk in chunks]
df = dfs[0]
for additional_df in dfs[1:]:
    df = df.union(additional_df)
```

**All JAR-based libraries must be rewritten in Python.** There is no JAR support on serverless compute.

#### Wheel File Compatibility

**Detect:**
```regex
\.whl\b|cp3[0-9]{1,2}|manylinux
```

All wheel files must be compatible with **Python 3.12** (`cp312`). Wheels built for older Python versions (`cp39`, `cp310`, `cp311`) will not work.

```bash
# Check wheel compatibility:
# Look for cp312 or py3-none-any in the filename:
# package-1.0.0-cp312-cp312-manylinux_2_17_x86_64.whl  (OK)
# package-1.0.0-py3-none-any.whl                         (OK - pure Python)
# package-1.0.0-cp311-cp311-manylinux_2_17_x86_64.whl  (FAIL - wrong Python)
```

### Init Scripts

**Detect:**
```regex
init_scripts|initScripts|dbfs:/init|/databricks/init
```

**Fix:** Init scripts are NOT supported on serverless. All functionality must move to:

1. **requirements.txt** -- for package installations
2. **Notebook code** -- for environment configuration
3. **Environment variables** -- via job parameters (see Section 6)

```python
# Before (init script):
# #!/bin/bash
# pip install custom-package==1.0
# export MY_VAR="value"

# After:
# 1. Add to requirements.txt:
#    custom-package==1.0
#
# 2. In notebook, replace env var with widget:
#    my_var = dbutils.widgets.get("MY_VAR")
```

---

## 5. Spark Configuration Migration

### Configs Supported on Serverless

The following Spark configs can be set via `spark.conf.set()` in notebooks on serverless:

| Config | Description | Default on Serverless |
|--------|-------------|----------------------|
| `spark.sql.shuffle.partitions` | Number of shuffle partitions | Auto-tuned |
| `spark.sql.files.maxPartitionBytes` | Max bytes per partition when reading | Auto-tuned |
| `spark.sql.autoBroadcastJoinThreshold` | Threshold for broadcast joins | Auto-tuned |
| `spark.sql.adaptive.enabled` | Adaptive Query Execution | `true` |
| `spark.sql.adaptive.coalescePartitions.enabled` | Auto-coalesce | `true` |
| `spark.sql.adaptive.skewJoin.enabled` | Skew join optimization | `true` |
| `spark.databricks.delta.optimizeWrite.enabled` | Optimize write | `true` |
| `spark.databricks.delta.autoCompact.enabled` | Auto-compaction | `true` |
| `spark.databricks.delta.schema.autoMerge.enabled` | Schema auto-merge | Varies |
| `spark.sql.ansi.enabled` | ANSI mode | `true` (CANNOT be changed) |
| `spark.sql.sources.default` | Default data source | `delta` |
| `spark.sql.legacy.timeParserPolicy` | Timestamp parsing | `CORRECTED` |

### Configs NOT Supported on Serverless

These configs have no effect or will throw errors on serverless. **Remove them.**

#### Executor/Driver Configs -- Remove

**Detect:**
```regex
spark\.executor\.|spark\.driver\.maxResultSize|spark\.driver\.memory|spark\.driver\.cores
```

```python
# Before:
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.instances", "10")
spark.conf.set("spark.driver.memory", "16g")
spark.conf.set("spark.driver.maxResultSize", "4g")

# After -- remove all of the above. Serverless manages these automatically.
```

#### Dynamic Allocation Configs -- Remove

**Detect:**
```regex
spark\.dynamicAllocation\.
```

```python
# Before:
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")

# After -- remove all. Serverless handles scaling automatically.
```

#### Shuffle Configs -- Remove (Auto-Managed)

**Detect:**
```regex
spark\.shuffle\.|spark\.sql\.shuffle\.partitions
```

```python
# Before:
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# After -- remove all. Serverless auto-tunes shuffle behavior.
# EXCEPTION: spark.sql.shuffle.partitions CAN be set if you have a specific
# reason, but it is auto-tuned by default and usually better left alone.
```

#### Custom JVM Options -- Not Supported

**Detect:**
```regex
spark\.driver\.extraJavaOptions|spark\.executor\.extraJavaOptions|spark\.driver\.extraClassPath|spark\.executor\.extraClassPath
```

```python
# Before:
spark.conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -Xss4m")
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

# After -- remove entirely. No JVM customization on serverless.
```

### Notebook-Level spark.conf.set Calls -- Decision Guide

When scanning notebooks, use this decision tree for each `spark.conf.set()` call:

```
spark.conf.set("config.name", "value")
    |
    +--> Is it in the "NOT Supported" list above?
    |       YES --> Remove it
    |       NO  --> Continue
    |
    +--> Is it spark.sql.ansi.enabled = false?
    |       YES --> Remove it (ANSI is mandatory on serverless)
    |              Apply ANSI fixes from Section 3 instead
    |       NO  --> Continue
    |
    +--> Is it a Delta table property config?
    |       YES --> Keep it (e.g., spark.databricks.delta.*)
    |       NO  --> Continue
    |
    +--> Is it a supported config from the "Supported" list?
    |       YES --> Keep it, but consider if auto-tuning is better
    |       NO  --> Research whether it is supported; remove if not
```

### Cluster-Level Spark Configs

Configs that were set at the cluster level in the classic job must be migrated to one of:

1. **Notebook-level** `spark.conf.set()` -- for configs that are still supported
2. **Removed entirely** -- for unsupported configs
3. **Job parameters** -- for configs that were used to pass environment-specific values

**Detect cluster-level configs in job JSON:**
```regex
"spark_conf"\s*:\s*\{|"new_cluster"\s*:.*"spark_conf"
```

---

## 6. Environment Variable Migration

### os.environ.get() to dbutils.widgets.get()

**Detect:**
```regex
os\.environ\.get\s*\(|os\.environ\[|os\.getenv\s*\(|sys\.argv|argparse\.ArgumentParser
```

On serverless, OS environment variables set via cluster config or init scripts are not available. All parameterization must use Databricks widgets (for notebooks) or job parameters.

**Fix -- notebooks:**

```python
# Before:
import os
env = os.environ.get("ENVIRONMENT", "dev")
catalog = os.environ.get("CATALOG_NAME", f"{env}_catalog")
landing_path = os.environ.get("PATH_LANDING", f"abfss://landing@{env}storage.dfs.core.windows.net/")

# After:
# At the top of the notebook, define widgets with defaults:
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("path_landing", "", "Landing Path")

# Then retrieve values:
env = dbutils.widgets.get("env")
catalog = dbutils.widgets.get("catalog_name") or f"{env}_catalog"
landing_path = dbutils.widgets.get("path_landing") or f"abfss://landing@{env}storage.dfs.core.windows.net/"
```

**Fix -- Python scripts with sys.argv:**

```python
# Before:
import sys
env = sys.argv[1]
job_date = sys.argv[2]

# After -- use dbutils.widgets for notebook tasks:
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("job_date", "")
env = dbutils.widgets.get("env")
job_date = dbutils.widgets.get("job_date")

# After -- for Python wheel/script tasks, parameters come from task config:
# (see job JSON in Section 7 for how to pass parameters)
```

### Job Parameters Configuration

Parameters are passed to notebooks via the job JSON `parameters` block. See Section 7 for the complete JSON template.

**Widget parameter patterns:**

```python
# Pattern 1: Simple env-based parameterization
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

USE_CATALOG = f"{env}_catalog"
spark.sql(f"USE CATALOG {USE_CATALOG}")

# Pattern 2: Multiple parameters with defaults
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("batch_size", "1000")

env = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date") or str(date.today())
batch_size = int(dbutils.widgets.get("batch_size"))

# Pattern 3: Dropdown for fixed choices
dbutils.widgets.dropdown("mode", "full", ["full", "incremental"])
mode = dbutils.widgets.get("mode")
```

---

## 7. Job JSON Transformation

### Complete Before/After Template

**BEFORE -- Classic cluster job JSON:**

```json
{
  "name": "claims_etl_pipeline",
  "email_notifications": {
    "on_failure": ["oncall@example.com"]
  },
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 4,
        "spark_conf": {
          "spark.sql.ansi.enabled": "false",
          "spark.executor.memory": "8g",
          "spark.databricks.delta.optimizeWrite.enabled": "true"
        },
        "spark_env_vars": {
          "ENVIRONMENT": "{{env}}",
          "PATH_LANDING": "abfss://landing@{{env}}storage.dfs.core.windows.net/"
        },
        "init_scripts": [
          {
            "workspace": {
              "destination": "/Shared/init_scripts/install_deps.sh"
            }
          }
        ]
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "job_cluster_key": "etl_cluster",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/01_bronze",
        "base_parameters": {
          "env": "{{env}}"
        }
      }
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingest"}],
      "job_cluster_key": "etl_cluster",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/02_silver",
        "base_parameters": {
          "env": "{{env}}"
        }
      }
    },
    {
      "task_key": "gold_aggregate",
      "depends_on": [{"task_key": "silver_transform"}],
      "job_cluster_key": "etl_cluster",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/03_gold",
        "base_parameters": {
          "env": "{{env}}"
        }
      }
    }
  ]
}
```

**AFTER -- Serverless job JSON:**

```json
{
  "name": "claims_etl_pipeline",
  "email_notifications": {
    "on_failure": ["oncall@example.com"]
  },
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "parameters": [
    {
      "name": "env",
      "default": "dev"
    },
    {
      "name": "path_landing",
      "default": "abfss://landing@{{env}}storage.dfs.core.windows.net/"
    }
  ],
  "queue": {
    "enabled": true
  },
  "environments": [
    {
      "environment_key": "serverless_env",
      "spec": {
        "client": "4",
        "dependencies": [
          "/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
        ]
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "environment_key": "serverless_env",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/01_bronze",
        "base_parameters": {
          "env": "{{env}}"
        }
      },
      "performance_target": "PERFORMANCE_OPTIMIZED"
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingest"}],
      "environment_key": "serverless_env",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/02_silver",
        "base_parameters": {
          "env": "{{env}}"
        }
      },
      "performance_target": "PERFORMANCE_OPTIMIZED"
    },
    {
      "task_key": "gold_aggregate",
      "depends_on": [{"task_key": "silver_transform"}],
      "environment_key": "serverless_env",
      "notebook_task": {
        "notebook_path": "/Repos/prod/claims_pipeline/03_gold",
        "base_parameters": {
          "env": "{{env}}"
        }
      },
      "performance_target": "PERFORMANCE_OPTIMIZED"
    }
  ]
}
```

### Summary of JSON Changes

| Change | Action | Details |
|--------|--------|---------|
| `job_clusters` section | **Remove entirely** | No cluster definitions on serverless |
| `job_cluster_key` on tasks | **Replace** with `environment_key` | Points to the serverless environment |
| `environments` block | **Add** | Contains `client: "4"` and requirements.txt path |
| `parameters` block | **Add** at job level | Replaces `spark_env_vars` from cluster config |
| `queue.enabled` | **Add** | Required for serverless jobs |
| `performance_target` | **Add** to each task | `"PERFORMANCE_OPTIMIZED"` for ETL workloads |
| `spark_conf` | **Remove** unsupported | Move supported configs to notebook code |
| `spark_env_vars` | **Remove** | Replace with job `parameters` + `dbutils.widgets.get()` |
| `init_scripts` | **Remove** | Move to requirements.txt or notebook code |
| `spark_version` | **Remove** | Serverless manages the runtime version |
| `node_type_id` | **Remove** | Serverless manages compute |
| `num_workers` | **Remove** | Serverless manages scaling |

### Requirements.txt Path

The requirements.txt file must be stored in a Unity Catalog Volume:

```
/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
```

Where `%env%` is substituted by the job parameter value (e.g., `dev`, `uat`, `prod`).

---

## 8. Package Dependency Analysis

### Step 1: Audit All Package Installs

**Detect all package installation patterns across notebooks:**

```regex
%pip\s+install|!pip\s+install|dbutils\.library\.install|import\s+(?!pyspark|delta|databricks|mlflow|os|sys|re|json|datetime|typing|collections|functools|itertools|abc|io|pathlib|math|decimal|copy|uuid|hashlib|base64|struct|time|calendar|logging)
```

For each notebook in the pipeline, scan for:

1. `%pip install` commands
2. `dbutils.library.install()` calls
3. Third-party imports (non-standard-library, non-Spark)

### Step 2: Evaluate Each Package

For every package found, determine:

| Question | Action if Yes | Action if No |
|----------|--------------|-------------|
| Is it still needed? | Continue evaluation | Remove the import and install |
| Can it be replaced with native PySpark/SQL? | Replace (see common replacements below) | Continue |
| Is it compatible with Python 3.12? | Continue | Find alternative or upgrade |
| Does it have a cp312 wheel? | Add to requirements.txt | Build from source or find alternative |
| Is it a JVM/JAR package? | Must be rewritten in Python | N/A |

### Common Package Replacements

| Original Package | Replacement | Notes |
|-----------------|-------------|-------|
| `com.crealytics.spark.excel` (JAR) | `pandas` + `openpyxl` | Read Excel via pandas, convert to Spark DataFrame |
| `koalas` | Native pandas API on Spark | Built into DBR 16.4: `import pyspark.pandas as ps` |
| `databricks-koalas` | Native pandas API on Spark | Same as above |
| `pyspark.ml` (legacy) | `pyspark.ml` (current) | API is stable; check for deprecated methods |
| Custom UDF helper JARs | Native PySpark functions | Rewrite UDFs in Python |
| `azure-storage-blob` (for file access) | `dbutils.fs` or Unity Catalog Volumes | Use Volumes for file I/O |
| `pyodbc` / `jaydebeapi` (JDBC) | `spark.read.format("jdbc")` | Use Spark JDBC connector |
| `xlrd` (legacy Excel) | `openpyxl` | `xlrd` no longer supports `.xlsx` |

### UDF Package Analysis

For packages used inside UDFs, evaluate whether map + Arrow would improve performance:

```python
# Before -- row-at-a-time UDF with external package:
import some_package

@F.udf(returnType=StringType())
def process_row(value):
    return some_package.transform(value)

df = df.withColumn("result", process_row(F.col("input")))

# After -- if some_package supports vectorized operations,
# use pandas_udf with Arrow for 10-100x performance:
@F.pandas_udf(StringType())
def process_batch(series: pd.Series) -> pd.Series:
    return series.apply(some_package.transform)

df = df.withColumn("result", process_batch(F.col("input")))
```

### requirements.txt Format

```
# /Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
#
# All packages needed by serverless jobs.
# Pinned versions for reproducibility.
# Only cp312-compatible packages.

openpyxl==3.1.5
pandas==2.2.1
pyarrow==15.0.2
requests==2.31.0
python-dateutil==2.9.0
```

**Rules:**
- Pin exact versions (no `>=` or `~=`)
- Test locally with Python 3.12 before deploying
- Do not include packages already in the serverless runtime (pyspark, delta-spark, mlflow, etc.)
- One requirements.txt per environment (dev/uat/prod) stored in corresponding Volume

---

## 9. Performance Optimization Opportunities

### Migrate from Single-Threaded to Distributed

**Detect:**
```regex
for\s+row\s+in\s+.*\.collect\(\)|\.toPandas\(\)\s*\n.*for|iterrows|itertuples
```

**Fix -- use vectorized operations:**

```python
# Before -- single-threaded loop:
rows = df.collect()
results = []
for row in rows:
    results.append(transform(row["value"]))

# After -- distributed with map + Arrow:
@F.pandas_udf(StringType())
def transform_batch(series: pd.Series) -> pd.Series:
    return series.apply(transform)

df = df.withColumn("result", transform_batch(F.col("value")))
```

### Remove Unnecessary .count() Actions

**Detect:**
```regex
\.count\(\)\s*>\s*0|if\s+.*\.count\(\)|print.*\.count\(\)
```

**Fix:**
```python
# Before -- expensive full table scan:
if df.count() > 0:
    process(df)

# After -- stops at first row:
if df.first() is not None:
    process(df)

# Before -- counting for logging:
print(f"Rows: {df.count()}")
process(df)

# After -- avoid double computation:
count = df.count()
print(f"Rows: {count}")
# only if count is needed for the print; if not, remove the count entirely
```

### VACUUM on Serverless

Standard VACUUM works the same on serverless. No code change needed.

```sql
VACUUM catalog.schema.my_table RETAIN 168 HOURS;
```

For external tables, schedule regular VACUUM since Predictive Optimization is not available.

> **Note:** VACUUM LITE is available as a Public Preview feature but is not GA. Use standard VACUUM until GA.

### Liquid Clustering Instead of ZORDER

**Detect:**
```regex
ZORDER\s+BY|OPTIMIZE.*ZORDER
```

**Fix -- for new or recreatable tables:**
```sql
-- Before:
OPTIMIZE catalog.schema.claims ZORDER BY (claim_id, member_id);

-- After (one-time migration):
ALTER TABLE catalog.schema.claims CLUSTER BY (claim_id, member_id);

-- Then just run OPTIMIZE without ZORDER:
OPTIMIZE catalog.schema.claims;
```

**Note:** Liquid Clustering is only for new tables or tables you can recreate. Do not change existing production tables mid-migration without testing.

### Remove Manual Partition/Shuffle Configs

**Detect:**
```regex
spark\.conf\.set.*shuffle\.partitions|\.repartition\(\d+\)|\.coalesce\(\d+\)
```

**Fix -- serverless auto-tunes these:**
```python
# Before:
spark.conf.set("spark.sql.shuffle.partitions", "200")
df = df.repartition(100)

# After -- remove both. Let serverless auto-tune.
# Only keep repartition/coalesce if there is a specific business reason
# (e.g., writing to a specific number of output files).
```

### Arrow-Based UDFs for Better Performance

**Detect:**
```regex
@F\.udf|F\.udf\(
```

**Evaluate -- convert row-at-a-time UDFs to pandas_udf where possible:**

```python
# Before -- row-at-a-time UDF (~10 rows/sec):
@F.udf(returnType=DoubleType())
def calculate_risk(age, diagnosis_count, chronic_flag):
    if age is None or diagnosis_count is None:
        return None
    base = age * 0.1 + diagnosis_count * 2.5
    if chronic_flag:
        base *= 1.5
    return base

# After -- pandas_udf (~1000 rows/sec):
@F.pandas_udf(DoubleType())
def calculate_risk(age: pd.Series, diagnosis_count: pd.Series, chronic_flag: pd.Series) -> pd.Series:
    base = age * 0.1 + diagnosis_count * 2.5
    base = base.where(~chronic_flag.astype(bool), base * 1.5)
    return base.where(age.notna() & diagnosis_count.notna(), None)
```

**When NOT to convert to pandas_udf:**
- UDF has complex branching logic that does not vectorize
- UDF calls external services or APIs
- UDF has side effects (logging, file I/O)

---

## 10. _metadata Column Conflict Detection

### Background

DBR 16.4 introduces row tracking for Delta tables, which uses a system column called `_metadata`. If your notebooks reference a column named `_metadata` (either as a user-defined column or via the file metadata feature), there may be conflicts.

### Detection

**Detect in code:**
```regex
_metadata|select\s*\(\s*["']_metadata|col\s*\(\s*["']_metadata|F\.col\s*\(\s*["']_metadata
```

**Detect in tables -- check table properties for row tracking:**
```sql
SHOW TBLPROPERTIES catalog.schema.my_table;
-- Look for: delta.enableRowTracking = true
```

**Cross-reference:**
```python
# Check all output tables for row tracking
tables = ["catalog.schema.table_a", "catalog.schema.table_b"]

for table in tables:
    props = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    row_tracking = any(r["key"] == "delta.enableRowTracking" and r["value"] == "true" for r in props)

    # Check if code references _metadata for this table
    if row_tracking:
        print(f"WARNING: {table} has row tracking enabled -- check for _metadata column conflicts")
```

### Fix

If code reads `_metadata` for file-level metadata (e.g., `df.select("_metadata.file_path")`), this pattern still works but may need explicit disambiguation if the table also has row tracking enabled.

```python
# Before -- ambiguous on tables with row tracking:
df = spark.read.table("catalog.schema.my_table")
file_path = df.select("_metadata.file_path")

# After -- use the input_file_name function instead:
df = df.withColumn("source_file", F.input_file_name())
```

---

## 11. Schema Inference Issues

### Problem

`spark.createDataFrame()` without an explicit schema relies on schema inference. The inference algorithm may behave differently on serverless (different runtime, different Spark version) than on DBR 13.3 classic, especially for:

- Complex nested data (lists of dicts, dicts with mixed types)
- Numeric types (int vs long vs double)
- Null values in the first batch of rows
- Timestamps vs strings

### Detection

**Detect:**
```regex
spark\.createDataFrame\s*\(\s*(?!.*schema\s*=)(?!.*StructType)|createDataFrame\s*\([^,]+\s*,\s*\[
```

This regex finds `spark.createDataFrame()` calls that do NOT have a `schema=` parameter or `StructType` argument.

### Fix

Always provide an explicit schema:

```python
# Before -- schema inferred (risky):
data = [
    {"claim_id": "C001", "amount": 150.00, "claim_date": "2024-01-15"},
    {"claim_id": "C002", "amount": None, "claim_date": "2024-02-20"},
]
df = spark.createDataFrame(data)
# On 13.3: amount inferred as DoubleType
# On serverless: might infer as DecimalType or behave differently with None

# After -- explicit schema (safe):
schema = StructType([
    StructField("claim_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("claim_date", StringType(), nullable=True),
])
df = spark.createDataFrame(data, schema=schema)
```

```python
# Before -- list of tuples with column names only:
data = [("C001", 150.00), ("C002", None)]
df = spark.createDataFrame(data, ["claim_id", "amount"])

# After -- explicit schema:
schema = StructType([
    StructField("claim_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
])
df = spark.createDataFrame(data, schema=schema)
```

---

## 12. Testing and Validation Framework

### Pre-Migration: Archive Original Notebooks

Before modifying any notebook, archive the original:

```python
# Use the workspace API or dbutils to copy originals
source_path = "/Repos/prod/claims_pipeline/01_bronze"
archive_path = "/Archive/pre_serverless_migration/claims_pipeline/01_bronze"

# Via dbutils:
dbutils.notebook.run("/Shared/utils/archive_notebook", 600, {
    "source": source_path,
    "destination": archive_path,
    "timestamp": "2026-04-16"
})
```

### Pre-Migration: Record Baseline Delta Table Versions

```python
from delta.tables import DeltaTable

output_tables = [
    "catalog.schema.claims_bronze",
    "catalog.schema.claims_silver",
    "catalog.schema.claims_gold",
]

baseline_versions = {}
for table_name in output_tables:
    dt = DeltaTable.forName(spark, table_name)
    version = dt.history(1).select("version").collect()[0][0]
    baseline_versions[table_name] = version
    print(f"{table_name}: baseline version = {version}")

# Store baseline_versions for use in validation
```

### Parallel Validation Approach

Run the original job AND the converted job, then compare all output tables:

1. **Run original Scala job** on DBR 13.3 classic cluster --> record output table versions
2. **Run converted PySpark job** on serverless --> writes to same or separate tables
3. **Compare** using the `conversion_validator` skill (see `skills/conversion_validator/skill.md`)

### Comparison Dimensions

For every output table, validate:

| Dimension | Check | Pass Criteria |
|-----------|-------|--------------|
| **Schema** | Column names, types, nullability | Exact match |
| **Row count** | Total rows, orphan rows | Exact match, zero orphans |
| **Data match** | Row-by-row comparison via eqNullSafe | Zero mismatches |
| **Table names** | Output table names match expected | All tables created |
| **Column names** | No renamed/missing columns | Exact match |
| **Null counts** | Per-column null distribution | Exact match |
| **Aggregates** | Sum, avg, min, max per numeric column | Within tolerance (1e-6 for float) |
| **Distinct values** | Per-column distinct value sets | Exact match |

### Serverless-Specific Validation

In addition to the data validation above, confirm:

#### Confirm Job Ran on Serverless

```python
import requests

# Via Jobs API -- check run details
run_id = "<run_id>"
response = requests.get(
    f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
    headers={"Authorization": f"Bearer {token}"}
)
run = response.json()

for task in run.get("tasks", []):
    cluster_instance = task.get("cluster_instance", {})
    # Serverless runs will NOT have a cluster_id in the traditional sense
    # Check for environment_key in the task spec
    print(f"Task: {task['task_key']}")
    print(f"  environment_key: {task.get('environment_key', 'NOT SET')}")
    print(f"  compute_type: {task.get('compute_type', 'UNKNOWN')}")
```

#### Verify No Unsupported Spark Configs Were Set

```python
# In the converted notebook, add a diagnostic cell at the end:
all_confs = spark.sparkContext.getConf().getAll()

unsupported_prefixes = [
    "spark.executor.",
    "spark.driver.extraJavaOptions",
    "spark.driver.extraClassPath",
    "spark.dynamicAllocation.",
    "spark.shuffle.",
]

for key, value in all_confs:
    for prefix in unsupported_prefixes:
        if key.startswith(prefix):
            print(f"WARNING: Unsupported config found: {key} = {value}")
```

#### Verify environment_key Present on All Tasks

```python
import requests

job_id = "<job_id>"
response = requests.get(
    f"{host}/api/2.1/jobs/get?job_id={job_id}",
    headers={"Authorization": f"Bearer {token}"}
)
job = response.json()

for task in job["settings"]["tasks"]:
    env_key = task.get("environment_key")
    jc_key = task.get("job_cluster_key")
    if env_key:
        print(f"PASS: Task '{task['task_key']}' uses environment_key: {env_key}")
    elif jc_key:
        print(f"FAIL: Task '{task['task_key']}' still uses job_cluster_key: {jc_key}")
    else:
        print(f"WARN: Task '{task['task_key']}' has no environment_key or job_cluster_key")
```

#### Performance Comparison

```sql
-- Compare classic vs serverless run duration and cost
-- Using system tables

-- Run duration:
SELECT
  r.run_id,
  r.run_name,
  t.task_key,
  t.compute_type,
  t.execution_duration_ms / 1000.0 AS duration_seconds,
  t.result_state
FROM system.lakeflow.job_task_run_timeline t
JOIN system.lakeflow.job_run_timeline r ON t.run_id = r.run_id
WHERE r.job_id = <job_id>
  AND r.start_time > '2026-04-01'
ORDER BY r.run_id DESC
LIMIT 20;

-- DBU cost comparison:
SELECT
  u.usage_date,
  u.sku_name,
  u.usage_quantity AS dbus,
  u.usage_metadata.job_id
FROM system.billing.usage u
WHERE u.usage_metadata.job_id = '<job_id>'
  AND u.usage_date > '2026-04-01'
ORDER BY u.usage_date DESC;
```

---

## 13. Test Coverage Requirements

### When Tests Do Not Exist in the Git Repo

If the pipeline does not have existing tests, add the following as part of the migration:

#### Unit Tests for Python Classes and Methods

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

class TestClaimTransformations:
    def test_clean_code_handles_null(self, spark):
        """Verify UDF returns None for null input, not empty string."""
        from notebooks.transformations import clean_code
        df = spark.createDataFrame([(None,)], ["code"])
        result = df.withColumn("cleaned", clean_code(F.col("code")))
        assert result.first()["cleaned"] is None

    def test_clean_code_trims_and_uppercases(self, spark):
        from notebooks.transformations import clean_code
        df = spark.createDataFrame([("  abc  ",)], ["code"])
        result = df.withColumn("cleaned", clean_code(F.col("code")))
        assert result.first()["cleaned"] == "ABC"
```

#### Unit Tests for All UDFs

Every UDF must have tests covering:
- Normal input
- Null input (must return None)
- Empty string input
- Edge cases specific to the UDF's domain

```python
class TestCategorizeDiagnosisUDF:
    def test_valid_code(self, spark):
        df = spark.createDataFrame([("E11.9",)], ["code"])
        result = df.withColumn("cat", categorize_diagnosis(F.col("code")))
        assert result.first()["cat"] == "Endocrine/Metabolic"

    def test_null_code(self, spark):
        df = spark.createDataFrame([(None,)], ["code"])
        result = df.withColumn("cat", categorize_diagnosis(F.col("code")))
        assert result.first()["cat"] is None  # or "Unknown" -- match Scala behavior

    def test_empty_code(self, spark):
        df = spark.createDataFrame([("",)], ["code"])
        result = df.withColumn("cat", categorize_diagnosis(F.col("code")))
        assert result.first()["cat"] is None  # or "Unknown" -- match Scala behavior

    def test_unknown_prefix(self, spark):
        df = spark.createDataFrame([("Z99.9",)], ["code"])
        result = df.withColumn("cat", categorize_diagnosis(F.col("code")))
        assert result.first()["cat"] == "Other"
```

#### Integration Tests for Notebook Execution

```python
# tests/test_notebook_integration.py
class TestBronzeNotebook:
    def test_bronze_creates_output_table(self, spark):
        """Run bronze notebook and verify output table exists with expected schema."""
        dbutils.notebook.run(
            "/Repos/dev/claims_pipeline/01_bronze",
            600,
            {"env": "test", "run_date": "2026-01-01"}
        )
        df = spark.table("test_catalog.claims.claims_bronze")
        assert df.count() > 0
        assert "claim_id" in df.columns
        assert "member_id" in df.columns

    def test_bronze_handles_empty_input(self, spark):
        """Verify bronze notebook handles empty input gracefully."""
        # Set up empty input path, run notebook, verify behavior
        pass
```

#### Data Validation Tests for Output Tables

```python
# tests/test_data_quality.py
class TestClaimsSilverDataQuality:
    def test_no_duplicate_claim_ids(self, spark):
        df = spark.table("catalog.schema.claims_silver")
        total = df.count()
        distinct = df.select("claim_id").distinct().count()
        assert total == distinct, f"Duplicate claim_ids: {total} total, {distinct} distinct"

    def test_amount_is_non_negative(self, spark):
        df = spark.table("catalog.schema.claims_silver")
        negatives = df.filter(F.col("paid_amount") < 0).count()
        assert negatives == 0, f"Found {negatives} rows with negative paid_amount"

    def test_dates_are_valid(self, spark):
        df = spark.table("catalog.schema.claims_silver")
        nulls = df.filter(F.col("claim_date").isNull()).count()
        future = df.filter(F.col("claim_date") > F.current_date()).count()
        print(f"Null dates: {nulls}, Future dates: {future}")
        assert future == 0, f"Found {future} rows with future claim_date"
```

---

## 14. Known Issues Catalog

### Serverless-Specific Errors

| Error Message | Root Cause | Resolution |
|--------------|------------|------------|
| `UNSUPPORTED_FEATURE: caching` or `AnalysisException: ... cache` | `.cache()`, `.persist()`, or `CACHE TABLE` used | Remove all caching calls. Serverless auto-manages memory. |
| `AnalysisException: REFRESH TABLE is not supported` | `REFRESH TABLE` statement | Remove the statement. Serverless auto-invalidates caches. |
| `AnalysisException: MSCK REPAIR TABLE is not supported` | `MSCK REPAIR TABLE` statement | Remove. Not applicable for Delta tables. |
| `Py4JJavaError: ... NoClassDefFoundError` | JVM/JAR library referenced | Rewrite using Python libraries. No JAR support on serverless. |
| `ModuleNotFoundError: No module named 'X'` | Package not in requirements.txt | Add to `/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt` |
| `AnalysisException: Cannot set spark.sql.ansi.enabled` | Attempting to disable ANSI mode | Remove the config set. Apply ANSI fixes from Section 3 instead. |
| `ValueError: unsupported format character` | Python 3.12 format string change | Check for `%` format strings with invalid specifiers. Use f-strings. |
| `SparkException: environment_key not found` | `environment_key` references an undefined environment | Verify `environments` block in job JSON matches `environment_key` on tasks. |
| `GLOBAL TEMPORARY view is not supported` | `createGlobalTempView()` used | Convert to `createOrReplaceTempView()` or permanent table. |
| `AnalysisException: Cannot resolve column name '_metadata'` | Row tracking conflict | Use `input_file_name()` instead of `_metadata.file_path`. |

### Python 3.12 Compatibility Issues

| Issue | Detect Pattern | Resolution |
|-------|---------------|------------|
| `imp` module removed | `import imp` | Replace with `importlib` |
| `distutils` removed | `import distutils` or `from distutils` | Replace with `setuptools` or `shutil` |
| `typing.TypeAlias` changed | `from typing import TypeAlias` | Use `type` statement (Python 3.12+) or `TypeAlias` from `typing_extensions` |
| `datetime.utcnow()` deprecated | `datetime.utcnow()` or `datetime.utcfromtimestamp()` | Use `datetime.now(timezone.utc)` |
| `asyncio.coroutine` removed | `@asyncio.coroutine` decorator | Use `async def` |
| f-string changes | Nested f-strings, backslashes in f-strings | Python 3.12 relaxes f-string rules; older patterns may need updating |
| `pkg_resources` deprecated | `import pkg_resources` | Use `importlib.metadata` |
| `ssl` default changes | `ssl.SSLContext()` | Specify protocol version explicitly |

### Arrow/Pandas Version Conflicts

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Arrow version mismatch | `ArrowInvalid` or `ArrowTypeError` in pandas_udf | Pin `pyarrow` version in requirements.txt to match serverless runtime |
| Pandas 2.x breaking changes | `FutureWarning` or `TypeError` in DataFrame operations | Pin `pandas>=2.0,<3.0` in requirements.txt. Check for `append()` (removed), `inplace` behavior changes |
| numpy 2.0 breaking changes | `AttributeError` on numpy functions | Pin `numpy>=1.26,<2.0` or update code for numpy 2.0 API changes |

### Delta Protocol Issues on Serverless

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Protocol version auto-upgrade | `DeltaTableFeatureException` when reading from older clusters | After serverless writes, older DBR clusters may not read the table. Check `DESCRIBE DETAIL` for protocol version. |
| Deletion vectors incompatibility | Read errors from external tools | If external tools read the table, set `delta.enableDeletionVectors = false` on the table before migration. |
| Column mapping conflicts | Schema evolution errors | Check `delta.columnMapping.mode` in table properties. If set to `name`, ensure all readers support it. |
| Liquid clustering + ZORDER | `OPTIMIZE ... ZORDER BY` fails after Liquid Clustering enabled | Once a table uses Liquid Clustering, ZORDER is not allowed. Use `OPTIMIZE` without ZORDER. |

---

## 15. Step-by-Step Migration Process

### Complete Procedure for Each Job

```
Phase 0: Preparation
====================
  0.1  Confirm job is assigned to Path B in the manifest
  0.2  Confirm all input tables are accessible from serverless
  0.3  Confirm Unity Catalog permissions are in place

Phase 1: Baseline
=================
  1.1  Archive original Scala notebooks
       (copy to /Archive/pre_serverless_migration/<pipeline_name>/)
  1.2  Run original Scala job on DBR 13.3 classic cluster
  1.3  Record baseline Delta table versions for ALL output tables
  1.4  Record baseline row counts, schemas, and sample data

Phase 2: Language Conversion
============================
  2.1  Convert Scala notebooks to PySpark using scala_to_pyspark skill
       - Follow all rules in skills/scala_to_pyspark/skill.md
       - Preserve notebook cell structure
       - Preserve all comments and markdown
       - Do NOT optimize or refactor -- faithful translation only
  2.2  [Optional validation point] Run PySpark on DBR 13.3 classic cluster
       - Compare output to Scala baseline using conversion_validator
       - If mismatches: fix language conversion issues before proceeding

Phase 3: ANSI / DBR 16.4 Fixes
===============================
  3.1  Scan all notebooks for ANSI-unsafe patterns (Section 3)
  3.2  Apply fixes: TRY_CAST, TRY_DIVIDE, TRY_ELEMENT_AT, IS TRUE, etc.
  3.3  Scan for deprecated configs (Section 5)
  3.4  Remove or replace deprecated configs
  3.5  [Optional validation point] Run PySpark on DBR 16.4 classic cluster
       - Compare output to Phase 2 baseline
       - If mismatches: fix ANSI issues before proceeding

Phase 4: Serverless Restrictions
================================
  4.1  Remove unsupported operations:
       - .persist() / .cache() / CACHE TABLE / UNCACHE TABLE
       - REFRESH TABLE
       - MSCK REPAIR TABLE
       - Global temp views
       - RDD APIs
  4.2  Remove unsupported Spark configs (Section 5)
  4.3  Migrate environment variables (Section 6):
       - os.environ.get() --> dbutils.widgets.get()
  4.4  Replace unsupported libraries (Section 8):
       - spark-excel --> pandas + openpyxl
       - Remove %pip install / dbutils.library.install
       - Add all dependencies to requirements.txt
  4.5  Remove init script references
  4.6  Check for _metadata column conflicts (Section 10)
  4.7  Add explicit schemas to spark.createDataFrame() calls (Section 11)

Phase 5: Job JSON Transformation
=================================
  5.1  Transform job JSON (Section 7):
       - Remove job_clusters section
       - Replace job_cluster_key with environment_key
       - Add environments block with client "4" and requirements.txt path
       - Add parameters block
       - Add queue.enabled = true
       - Add performance_target to each task
  5.2  Deploy transformed job JSON

Phase 6: Performance Review
===========================
  6.1  Review for performance optimization opportunities (Section 9):
       - Remove unnecessary .count() actions
       - Evaluate pandas_udf for row-at-a-time UDFs
       - Remove manual partition configs
       - Consider Liquid Clustering for new tables
       - Schedule regular VACUUM for external tables

Phase 7: Test Coverage
======================
  7.1  Check if tests exist in the git repo
  7.2  If not, add test coverage (Section 13):
       - Unit tests for all UDFs
       - Integration tests for notebook execution
       - Data validation tests for output tables

Phase 8: Run on Serverless
==========================
  8.1  Run the converted job on serverless compute
  8.2  Monitor for errors (check Section 14 known issues)
  8.3  If errors: fix and re-run

Phase 9: Validate Against Baseline
===================================
  9.1  Run conversion_validator against ALL output tables
       (see skills/conversion_validator/skill.md)
  9.2  Confirm: schema match, row count match, data match
  9.3  Confirm: serverless-specific validations (Section 12)
       - Job ran on serverless
       - No unsupported configs set
       - environment_key present on all tasks
  9.4  Compare performance: duration and DBU cost

Phase 10: Generate Report
=========================
  10.1  Generate conversion report using conversion_report skill
        (see skills/conversion_report/skill.md)
  10.2  Include serverless-specific change sections
  10.3  Include performance comparison

Phase 11: Parallel SIT
======================
  11.1  Run converted job in parallel with original for 1-2 production cycles
  11.2  Compare outputs after each run
  11.3  Sign off on migration when outputs match consistently
```

### Decision Points and Rollback

| Phase | Decision Point | Rollback Action |
|-------|---------------|-----------------|
| Phase 2 | Language conversion validation fails | Fix UDFs/translations, re-validate |
| Phase 3 | ANSI fixes introduce regressions | Review ANSI fix patterns, adjust guards |
| Phase 8 | Serverless run fails | Check known issues (Section 14), fix, re-deploy |
| Phase 9 | Data validation fails | Isolate which phase caused the issue (language, ANSI, or serverless), fix at source |
| Phase 11 | Parallel SIT shows drift | Investigate -- may be non-determinism (acceptable) or real regression (fix required) |

---

## 16. Documentation Links

### Serverless Compute
- [Serverless compute overview](https://docs.databricks.com/en/compute/serverless.html)
- [Serverless compute for notebooks](https://docs.databricks.com/en/compute/serverless/notebook-serverless.html)
- [Serverless compute for jobs](https://docs.databricks.com/en/jobs/serverless-jobs.html)
- [Serverless compute limitations](https://docs.databricks.com/en/compute/serverless.html#limitations)

### Environment Version 4
- [Serverless environment dependencies](https://docs.databricks.com/en/compute/serverless/dependencies.html)
- [Environment version release notes](https://docs.databricks.com/en/release-notes/serverless.html)
- [Client version specification](https://docs.databricks.com/en/compute/serverless/dependencies.html#client-version)

### Spark Configuration
- [Supported Spark configs on serverless](https://docs.databricks.com/en/compute/serverless.html#supported-spark-configuration-properties)
- [Spark SQL configuration reference](https://spark.apache.org/docs/latest/configuration.html#spark-sql)

### Python 3.12
- [What's New in Python 3.12](https://docs.python.org/3/whatsnew/3.12.html)
- [Python 3.12 removed modules](https://docs.python.org/3/whatsnew/3.12.html#removed)
- [Python 3.12 deprecations](https://docs.python.org/3/whatsnew/3.12.html#deprecated)

### Requirements.txt
- [pip requirements file format](https://pip.pypa.io/en/stable/reference/requirements-file-format/)
- [Serverless dependency management](https://docs.databricks.com/en/compute/serverless/dependencies.html)

### Delta Lake
- [Delta Lake protocol versioning](https://docs.databricks.com/en/delta/table-properties.html#delta-protocol-versions)
- [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
- [Deletion vectors](https://docs.databricks.com/en/delta/deletion-vectors.html)

### DBR Migration
- [Databricks Runtime release notes](https://docs.databricks.com/en/release-notes/runtime/index.html)
- [DBR 16.4 LTS release notes](https://docs.databricks.com/en/release-notes/runtime/16.4lts.html)
- [Migration guide from earlier DBR versions](https://docs.databricks.com/en/release-notes/runtime/index.html#migration-guides)

### ANSI Mode
- [ANSI compliance in Databricks](https://docs.databricks.com/en/sql/language-manual/ansi-compliance.html)
- [TRY_CAST function](https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html)
- [TRY_DIVIDE function](https://docs.databricks.com/en/sql/language-manual/functions/try_divide.html)
- [TRY_ELEMENT_AT function](https://docs.databricks.com/en/sql/language-manual/functions/try_element_at.html)

### Toolkit Cross-References
- **Language conversion detail:** `skills/scala_to_pyspark/skill.md`
- **DBR upgrade detail:** `skills/dbr_upgrade/skill.md`
- **Validation framework:** `skills/conversion_validator/skill.md`
- **Report generation:** `skills/conversion_report/skill.md`
- **Toolkit plan:** `PLAN.md`
