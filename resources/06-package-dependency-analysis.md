# Package and Dependency Analysis Guide

This reference teaches Genie Code how to audit, evaluate, and migrate package dependencies when converting Databricks jobs to serverless compute (environment version 4, Python 3.12).

## Why This Matters

Serverless compute has specific constraints:
- **No init scripts** — all dependencies must be in requirements.txt or the notebook environment pane
- **No JVM/JAR libraries** from notebooks — must use Python packages only
- **Python 3.12** — all wheels must be cp312-compatible
- **No arbitrary system packages** — only PyPI packages

Beyond constraints, this is an opportunity to **reduce dependencies**. Many packages used in DBR 13.3 notebooks have native PySpark/SQL equivalents in DBR 16.4+ and serverless. Removing unnecessary packages reduces build time, dependency conflicts, and maintenance burden.

---

## Step 1: Discover All Dependencies

### Scan Patterns

Search every notebook for these patterns:

```
# %pip install in notebooks
(?m)^%pip\s+install\s+(.+)$

# dbutils.library.install (deprecated)
dbutils\.library\.install\s*\(

# dbutils.library.restartPython (deprecated)
dbutils\.library\.restartPython\s*\(

# Python import statements
(?m)^(?:from|import)\s+([\w\.]+)

# Scala/Java library references (for Scala→PySpark conversions)
(?m)^\s*%scala
import\s+(?:com|org|net|io)\.[\w\.]+

# Maven coordinates in %run or cluster libraries
(?:com|org|net|io)\.[\w\-]+:[\w\-]+:[\d\.]+

# requirements.txt or pip.conf references
requirements\.txt
pip\.conf
```

### Build the Dependency Inventory

For each notebook, create an inventory:

```
Notebook: /path/to/notebook
Dependencies:
  - pandas==1.5.3 (via %pip install)
  - openpyxl (via %pip install, no version pin)
  - requests (via import, no explicit install — relies on cluster preinstall)
  - com.crealytics:spark-excel_2.12:3.3.1 (via Maven/JAR)
  - custom_utils (via %run ./utils — internal module)
```

---

## Step 2: Classify Each Dependency

For each discovered dependency, classify it:

### Category A: Already Available on Serverless (No Action)

These packages are pre-installed on serverless environment v4. No need to add to requirements.txt.

| Package | Serverless v4 Version | Notes |
|---------|----------------------|-------|
| `pyspark` | Built-in | Core Spark — always available |
| `pandas` | 2.x | Major version bump from 13.3 (1.x). Check for API changes. |
| `numpy` | 1.26+ | Generally compatible |
| `pyarrow` | 14+ | Major version bump. Check Arrow-related code. |
| `requests` | 2.31+ | Compatible |
| `scipy` | 1.11+ | Compatible |
| `matplotlib` | 3.8+ | Compatible |
| `scikit-learn` | 1.3+ | On ML runtime. Check API deprecations if upgrading from 1.0.x |
| `mlflow` | 2.x | Check for API changes if upgrading from 1.x |
| `cryptography` | 41+ | Compatible |
| `boto3` | 1.28+ | Compatible |
| `azure-storage-blob` | 12.x | Compatible |
| `openpyxl` | 3.1+ | Compatible |
| `xlrd` | 2.0+ | Compatible (but only reads .xls, not .xlsx) |

**Action:** Remove from %pip install / requirements.txt. If a specific version is pinned and conflicts with the pre-installed version, evaluate compatibility.

### Category B: Needs to Be Added to requirements.txt

Packages not pre-installed that are still needed.

**Action:** Add to the shared requirements.txt at:
`/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`

**Requirements for the package:**
1. Must be on PyPI (or a custom package index)
2. Must have a wheel for Python 3.12 (cp312) or be pure Python
3. Must not require system-level dependencies (C libraries not in the serverless image)
4. Must not require JVM/Java dependencies

### Category C: Can Be Replaced with Native Spark Functions

This is the key optimization. Many packages are used for operations that Spark can do natively.

| Package | Common Use | Native Spark Replacement | Example |
|---------|-----------|-------------------------|---------|
| `koalas` | pandas-like API on Spark | `pyspark.pandas` (built-in since 3.2) | `import pyspark.pandas as ps` |
| `spark-excel` (`com.crealytics`) | Read/write Excel files | `pandas` + `openpyxl` via local read | See detailed pattern below |
| `dbldatagen` | Synthetic data generation | `pyspark.sql.functions` + `F.rand()` / `F.randn()` | Build generators with Spark functions |
| `pyspark-test` | DataFrame testing | Native `assertEqual` patterns | See testing guide |
| `great_expectations` | Data validation | Delta Live Tables expectations or custom SQL checks | `CONSTRAINT valid_amount EXPECT (amount > 0)` |
| `phonenumbers` | Phone number parsing | `F.regexp_replace` + `F.regexp_extract` | Works for standardization; validation still needs the library |
| `email-validator` | Email validation | `F.rlike` with email regex | `F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$")` |
| `dateutil` | Date parsing | `F.to_date` / `F.to_timestamp` with format | `F.to_date(F.col("date_str"), "MM/dd/yyyy")` |
| `fuzzywuzzy` / `rapidfuzz` | Fuzzy string matching | `F.levenshtein` / `F.soundex` | `F.levenshtein(F.col("a"), F.col("b")) < 3` |
| `unidecode` | Unicode normalization | `F.translate` / `F.regexp_replace` | For accent removal patterns |
| `hashlib` (in UDFs) | Hashing | `F.md5` / `F.sha1` / `F.sha2` / `F.xxhash64` | `F.sha2(F.col("data"), 256)` |
| `json` (in UDFs) | JSON parsing | `F.from_json` / `F.get_json_object` / `F.json_tuple` | See JSON pattern below |
| `re` (in UDFs) | Regex operations | `F.regexp_extract` / `F.regexp_replace` / `F.rlike` | See regex pattern below |
| `collections.Counter` (in UDFs) | Value counting | `F.groupBy().count()` | Rewrite as Spark aggregation |
| `uuid` (in UDFs) | UUID generation | `F.expr("uuid()")` | Built-in since Spark 3.x |
| `base64` (in UDFs) | Base64 encoding | `F.base64` / `F.unbase64` | `F.base64(F.col("data"))` |
| `urllib.parse` (in UDFs) | URL parsing | `F.regexp_extract` with URL pattern | See URL pattern below |
| `datetime` (in UDFs) | Date math | `F.date_add` / `F.date_sub` / `F.datediff` / `F.months_between` | `F.date_add(F.col("date"), 30)` |
| `decimal` (in UDFs) | Precision rounding | `F.round` / `F.bround` / `CAST(x AS DECIMAL(p,s))` | `F.bround(F.col("amount"), 2)` for banker's rounding |

### Category D: JVM/JAR Libraries (Must Be Rewritten)

JAR-based libraries cannot run on serverless. They must be rewritten in Python.

| JVM Library | Purpose | Python Replacement |
|-------------|---------|-------------------|
| `com.crealytics:spark-excel` | Excel files | `pandas` + `openpyxl` (see pattern below) |
| `com.databricks:spark-xml` | XML files | `spark.read.format("xml")` (built-in since DBR 14.3) |
| `com.databricks:spark-csv` | CSV files | `spark.read.csv()` (built-in since Spark 2.0) |
| `com.databricks:spark-avro` | Avro files | `spark.read.format("avro")` (built-in) |
| Custom Scala JARs | Business logic | Rewrite as Python UDFs or PySpark operations |
| `spark-monitoring` | Metrics/logging | System Tables + Azure Diagnostic Settings |

### Category E: Needs Evaluation (UDF with Map + Arrow)

For packages that have no direct Spark equivalent but are used inside UDFs, evaluate whether using `pandas_udf` with Arrow would improve performance.

---

## Step 3: Detailed Replacement Patterns

### Pattern: spark-excel → pandas + openpyxl

```python
# BEFORE (classic compute with spark-excel JAR):
df = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .load("abfss://container@account.dfs.core.windows.net/file.xlsx"))

# AFTER (serverless — pandas + openpyxl):
import pandas as pd

# Read Excel to pandas DataFrame
pdf = pd.read_excel(
    "/Volumes/catalog/schema/volume/file.xlsx",
    sheet_name="Sheet1",
    header=0
)

# Convert to Spark DataFrame with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
schema = StructType([
    StructField("col1", StringType()),
    StructField("col2", DoubleType()),
    # ... define all columns
])
df = spark.createDataFrame(pdf, schema=schema)
```

**For large Excel files** (> 100MB):
```python
# Read in chunks to avoid driver memory issues
chunks = pd.read_excel(path, sheet_name="Sheet1", chunksize=50000)
dfs = []
for chunk in chunks:
    dfs.append(spark.createDataFrame(chunk, schema=schema))
df = dfs[0]
for additional_df in dfs[1:]:
    df = df.union(additional_df)
```

### Pattern: JSON Parsing in UDFs → Native Spark

```python
# BEFORE (Python UDF with json module):
import json

@F.udf(returnType=StringType())
def extract_field(json_str):
    if json_str is None:
        return None
    try:
        data = json.loads(json_str)
        return data.get("field_name")
    except json.JSONDecodeError:
        return None

df = df.withColumn("field", extract_field(F.col("json_col")))

# AFTER (native Spark):
df = df.withColumn("field", F.get_json_object(F.col("json_col"), "$.field_name"))

# For complex nested JSON:
from pyspark.sql.types import StructType, StructField, StringType
json_schema = StructType([
    StructField("field_name", StringType()),
    StructField("nested", StructType([
        StructField("inner_field", StringType())
    ]))
])
df = df.withColumn("parsed", F.from_json(F.col("json_col"), json_schema))
df = df.withColumn("field", F.col("parsed.field_name"))
df = df.withColumn("inner", F.col("parsed.nested.inner_field"))
```

### Pattern: Regex in UDFs → Native Spark

```python
# BEFORE (Python UDF with re module):
import re

@F.udf(returnType=StringType())
def extract_code(text):
    if text is None:
        return None
    match = re.search(r"CODE-(\d{4})", text)
    return match.group(1) if match else None

df = df.withColumn("code", extract_code(F.col("text")))

# AFTER (native Spark):
df = df.withColumn("code", F.regexp_extract(F.col("text"), r"CODE-(\d{4})", 1))
# Returns empty string instead of null for no match — add null guard if needed:
df = df.withColumn("code",
    F.when(F.col("text").rlike(r"CODE-(\d{4})"),
           F.regexp_extract(F.col("text"), r"CODE-(\d{4})", 1))
     .otherwise(F.lit(None))
)
```

### Pattern: Date Parsing in UDFs → Native Spark

```python
# BEFORE (Python UDF with datetime):
from datetime import datetime

@F.udf(returnType=DateType())
def parse_date(date_str):
    if date_str is None:
        return None
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None

df = df.withColumn("parsed_date", parse_date(F.col("date_str")))

# AFTER (native Spark — try multiple formats with COALESCE):
df = df.withColumn("parsed_date",
    F.coalesce(
        F.to_date(F.col("date_str"), "MM/dd/yyyy"),
        F.to_date(F.col("date_str"), "yyyy-MM-dd"),
        F.to_date(F.col("date_str"), "MM-dd-yyyy")
    )
)
# Note: In ANSI mode, use try_to_date instead of to_date to avoid errors:
df = df.withColumn("parsed_date",
    F.coalesce(
        F.expr("try_to_date(date_str, 'MM/dd/yyyy')"),
        F.expr("try_to_date(date_str, 'yyyy-MM-dd')"),
        F.expr("try_to_date(date_str, 'MM-dd-yyyy')")
    )
)
```

### Pattern: Hashing in UDFs → Native Spark

```python
# BEFORE:
import hashlib

@F.udf(returnType=StringType())
def hash_row(col1, col2, col3):
    data = f"{col1}|{col2}|{col3}"
    return hashlib.sha256(data.encode()).hexdigest()

df = df.withColumn("row_hash", hash_row("col1", "col2", "col3"))

# AFTER:
df = df.withColumn("row_hash",
    F.sha2(F.concat_ws("|", F.col("col1"), F.col("col2"), F.col("col3")), 256)
)
```

### Pattern: UUID in UDFs → Native Spark

```python
# BEFORE:
import uuid

@F.udf(returnType=StringType())
def gen_uuid():
    return str(uuid.uuid4())

df = df.withColumn("id", gen_uuid())

# AFTER:
df = df.withColumn("id", F.expr("uuid()"))
```

---

## Step 4: Evaluate UDFs for Arrow Optimization

For UDFs that cannot be replaced with native Spark functions, evaluate whether a `pandas_udf` with Arrow would be significantly faster.

### When to Recommend pandas_udf

- The UDF processes **many rows** (> 100K)
- The UDF does **row-independent** computation (no cross-row state)
- The UDF uses **pandas/numpy operations** internally
- The UDF processes **vectorizable** operations (math, string ops, lookups)

### When NOT to Recommend pandas_udf

- The UDF maintains **state across rows** (running totals, etc.)
- The UDF makes **external API calls** per row
- The UDF has **complex branching logic** that doesn't vectorize
- The data volume is small (< 10K rows) — overhead not worth it

### Conversion Pattern: Scalar UDF → pandas_udf

```python
# BEFORE (scalar UDF — processes one row at a time):
@F.udf(returnType=DoubleType())
def calculate_risk(age, diagnosis_count, chronic_flag):
    if age is None or diagnosis_count is None:
        return None
    base = age * 0.1 + diagnosis_count * 5.0
    if chronic_flag:
        base *= 1.5
    return min(base, 100.0)

df = df.withColumn("risk", calculate_risk("age", "diagnosis_count", "chronic_flag"))

# AFTER (pandas_udf — processes entire column as pandas Series):
import pandas as pd

@F.pandas_udf(DoubleType())
def calculate_risk(age: pd.Series, diagnosis_count: pd.Series, chronic_flag: pd.Series) -> pd.Series:
    base = age * 0.1 + diagnosis_count * 5.0
    base = base.where(~chronic_flag.astype(bool), base * 1.5)
    return base.clip(upper=100.0)

df = df.withColumn("risk", calculate_risk("age", "diagnosis_count", "chronic_flag"))
```

### Conversion Pattern: map with Arrow for Complex Logic

```python
# For complex per-row logic that doesn't vectorize well,
# use mapInArrow (available in Spark 3.4+):

def process_batch(batch_iter):
    for batch in batch_iter:
        # batch is a pyarrow.RecordBatch
        pdf = batch.to_pandas()
        # Complex per-row processing
        pdf["result"] = pdf.apply(lambda row: complex_logic(row), axis=1)
        yield pa.RecordBatch.from_pandas(pdf)

result_df = df.mapInArrow(process_batch, result_schema)
```

---

## Step 5: Build the requirements.txt

### Format

```
# /Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
#
# Serverless Environment v4 (Python 3.12)
# Only include packages NOT pre-installed on serverless.
# Pin versions for reproducibility.

openpyxl==3.1.2
xlsxwriter==3.1.9
phonenumbers==8.13.24
# Add others as needed
```

### Rules

1. **Pin all versions** — `package==x.y.z`, not `package>=x.y.z`
2. **Don't include pre-installed packages** — they'll conflict or be ignored
3. **Verify cp312 wheel availability** before adding:
   ```bash
   pip index versions <package> --python-version 3.12
   ```
4. **Test the requirements file** on a serverless notebook before deploying to jobs
5. **One shared file** at the Volume path — all serverless jobs reference the same file
6. **No git URLs or local paths** — only PyPI packages or custom index URLs

### Verifying Compatibility

```python
# Run this on a serverless notebook to test all dependencies
import subprocess
result = subprocess.run(
    ["pip", "install", "--dry-run", "-r", "/Volumes/catalog/schema/volume/requirements.txt"],
    capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    print("ERRORS:")
    print(result.stderr)
```

---

## Decision Flowchart

For each dependency found in a notebook:

```
Is it a JVM/JAR library?
├── YES → Must be rewritten in Python (Category D)
│         └── Is there a native Spark equivalent?
│              ├── YES → Use it (Category C)
│              └── NO → Write a Python UDF or use a PyPI package
└── NO → Is it pre-installed on serverless?
         ├── YES → Remove from install (Category A)
         │         └── Version pinned? Check for breaking changes in new version
         └── NO → Can it be replaced with native Spark?
                  ├── YES → Replace it (Category C)
                  │         └── Generate recommendation for developer
                  └── NO → Is it Python 3.12 compatible?
                           ├── YES → Add to requirements.txt (Category B)
                           │         └── Consider pandas_udf for performance (Category E)
                           └── NO → Find an alternative or file a request
```

---

## Reporting Template

For each notebook, produce a dependency report:

```
Notebook: /path/to/notebook
═══════════════════════════════════════

Dependencies Found: 8

REMOVE (pre-installed on serverless):
  1. pandas==1.5.3 → Available as pandas 2.x on serverless
     ⚠ Version bump 1.x → 2.x: check for deprecated APIs (append, inplace)
  2. numpy → Available on serverless

REPLACE WITH NATIVE SPARK:
  3. hashlib (in UDF line 45) → F.sha2()
     Recommendation: Replace UDF with F.sha2(F.concat_ws("|", ...), 256)
  4. json (in UDF line 72) → F.get_json_object() / F.from_json()
     Recommendation: Replace UDF with native JSON parsing
  5. re (in UDF line 88) → F.regexp_extract()
     Recommendation: Replace UDF with F.regexp_extract(col, pattern, group)

ADD TO REQUIREMENTS.TXT:
  6. phonenumbers==8.13.24 → No Spark equivalent for full validation
     cp312 compatible: YES
     Consider pandas_udf: YES (high row count, vectorizable)

MUST REWRITE (JVM library):
  7. com.crealytics:spark-excel → pandas + openpyxl
     See replacement pattern in this guide

INTERNAL MODULE:
  8. custom_utils (via %run) → Migrate to shared Python module or inline
```
