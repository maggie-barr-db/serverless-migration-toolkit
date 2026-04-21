# Skill: Serverless Migration

Self-contained reference for migrating Databricks jobs from classic compute (DBR 13.3 LTS or 15.4 LTS) to serverless general compute. Covers migration Paths B, C, and D.

---

## 1. Overview

Serverless general compute differs from classic compute in these fundamental ways:

| Aspect | Classic Compute | Serverless |
|--------|----------------|------------|
| Environment | DBR 13.3 / 15.4 LTS | Environment version 4 (DBR 16.4 equivalent) |
| Python | 3.10 | 3.12 |
| ANSI mode | `false` (default) | `true` (mandatory, cannot disable) |
| Resource management | Manual (cluster config) | Fully managed, auto-scaled |
| Shuffle partitions | 200 (static default) | Auto-tuned by engine |
| Default data format | `parquet` | `delta` |
| Session timezone | JVM default (varies) | `Etc/UTC` |
| Delta optimizeWrite | `false` | `true` |
| Delta autoCompact | `false` | `true` |
| Compression codec | `snappy` | `zstd` |
| Dependencies | Init scripts, cluster libs, %pip | requirements.txt on Unity Catalog Volume |
| Caching | Manual .cache()/.persist() | Managed by engine |

**Key principle:** Serverless manages infrastructure. Remove all resource configs, manual tuning, and caching. Fix code for ANSI mode. Migrate dependencies to requirements.txt.

---

## 2. Unsupported Operations

Every operation below must be detected and fixed before a job can run on serverless.

### 2.1 Cache and Persist

**Detection regex:**
```
\.persist\(\)
\.cache\(\)
\.unpersist\(\)
(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b
(?i)\bUNCACHE\s+TABLE\b
```

**Action:** Remove all cache/persist/unpersist calls and CACHE TABLE/UNCACHE TABLE statements. Serverless manages its own caching. If a DataFrame is reused many times and performance degrades, materialize to a temp table instead:
```python
# Instead of df.cache(), write to a temp table if truly needed:
df.write.mode("overwrite").saveAsTable("temp_catalog.schema.temp_table")
```

### 2.2 REFRESH TABLE

**Detection regex:**
```
(?i)\bREFRESH\s+TABLE\b
```

**Action:** Remove. Serverless auto-handles table metadata refresh. For external tables needing partition discovery, use `ALTER TABLE ... ADD PARTITION`.

### 2.3 MSCK REPAIR TABLE

**Detection regex:**
```
(?i)\bMSCK\s+REPAIR\s+TABLE\b
```

**Action:** Remove. For external tables needing partition discovery, use `ALTER TABLE ... ADD PARTITION` instead.

### 2.4 Materialized Views

**Detection regex:**
```
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b
```

**Action:** Materialized views cannot be created or refreshed on serverless general compute. Two options:
1. Move the statement to a SQL Warehouse task in the workflow.
2. Replace with a regular table that gets rebuilt:
```sql
-- Replace materialized view with regular table
CREATE OR REPLACE TABLE catalog.schema.summary AS
SELECT col1, COUNT(*) as cnt FROM catalog.schema.source GROUP BY col1;
```

### 2.5 Global Temp Views

**Detection regex:**
```
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?GLOBAL\s+TEMP(?:ORARY)?\s+VIEW\b
(?i)\bglobal_temp\.
```

**Action:** Convert to session-scoped temp views:
```sql
-- BEFORE:
CREATE GLOBAL TEMP VIEW my_view AS SELECT ...;
SELECT * FROM global_temp.my_view;

-- AFTER:
CREATE OR REPLACE TEMP VIEW my_view AS SELECT ...;
SELECT * FROM my_view;
```

### 2.6 RDD APIs

**Detection regex:**
```
\.rdd\.
\.rdd\b
spark\.sparkContext\.parallelize
sc\.parallelize
\.mapPartitions\(
\.flatMap\((?!.*F\.)
\.map\((?!.*F\.)(?!.*lambda)
\.reduceByKey\(
\.groupByKey\(
\.aggregateByKey\(
```

**Action:** Rewrite as DataFrame operations. RDD APIs are not supported on serverless.
```python
# BEFORE (RDD):
rdd = df.rdd.map(lambda row: (row["id"], row["amount"] * 1.1))
result = spark.createDataFrame(rdd, ["id", "adjusted_amount"])

# AFTER (DataFrame):
result = df.select(
    F.col("id"),
    (F.col("amount") * 1.1).alias("adjusted_amount")
)
```

### 2.7 dbutils.library.install

**Detection regex:**
```
dbutils\.library\.install\s*\(
dbutils\.library\.restartPython\s*\(
dbutils\.library\.installPyPI\s*\(
```

**Action:** Remove. Move all package dependencies to the shared requirements.txt. See Section 5.

### 2.8 %pip install

**Detection regex:**
```
(?m)^%pip\s+install\s+
(?m)^%pip\s+uninstall\s+
```

**Action:** Remove from notebooks. Move all packages to requirements.txt. See Section 5.

### 2.9 ThreadPoolExecutor

**Detection regex:**
```
ThreadPoolExecutor
concurrent\.futures
from\s+concurrent\s+import
import\s+concurrent
threading\.Thread
```

**Action:** Multi-threading is an anti-pattern on serverless. Replace with workflow-level parallelism:
- Use Databricks Workflows **for-each task** for parallel iteration over a list.
- Use separate workflow tasks with dependencies for parallel independent operations.

### 2.10 SparkContext Direct Access

**Detection regex:**
```
spark\.sparkContext\.
sc\.getConf
sc\.setLogLevel
sc\.addFile
sc\.addPyFile
spark\.sparkContext\.getConf
spark\.sparkContext\.setLocalProperty
```

**Action:** SparkContext access is limited on serverless. Remove `sc.getConf.set()` calls (use `spark.conf.set()` for supported configs). Remove `sc.setLogLevel()`. Remove `sc.addFile()`/`sc.addPyFile()` (use requirements.txt).

---

## 3. Spark Config Migration Map

### 3.1 Detection Regex for Spark Configs

```
spark\.conf\.set\s*\(
spark\.conf\.get\s*\(
(?i)^\s*SET\s+spark\.
(?i)^\s*SET\s+"spark\.
(?i)^\s*RESET\s+spark\.
sc\.getConf\.set\s*\(
spark\.driver\.extraJavaOptions
spark\.executor\.extraJavaOptions
```

### 3.2 Supported Configs (keep or adjust)

| Config | Classic Default | Serverless Default | Action |
|--------|----------------|-------------------|--------|
| `spark.sql.ansi.enabled` | `false` | `true` (mandatory) | Remove SET. Fix code with TRY_CAST, TRY_DIVIDE, null guards. |
| `spark.sql.shuffle.partitions` | `200` | Auto-tuned | Remove. Serverless auto-tunes. |
| `spark.sql.sources.default` | `parquet` | `delta` | Add explicit `.format("parquet")` if code expects parquet. |
| `spark.sql.adaptive.enabled` | `true` | `true` | Remove explicit set. |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Remove explicit set. |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Remove explicit set. |
| `spark.sql.caseSensitive` | `false` | `false` | Keep if needed. |
| `spark.sql.crossJoin.enabled` | `true` | `true` | Keep if needed. |
| `spark.sql.legacy.timeParserPolicy` | `EXCEPTION` | `EXCEPTION` | Can set to `LEGACY` for migration. |
| `spark.sql.session.timeZone` | JVM default | `Etc/UTC` | Set explicitly if code depends on specific TZ. |
| `spark.sql.parquet.datetimeRebaseModeInRead` | `EXCEPTION` | `EXCEPTION` | Can set to `LEGACY` for old Parquet files. |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | `EXCEPTION` | `EXCEPTION` | Can set to `LEGACY` if needed. |
| `spark.sql.parquet.int96RebaseModeInRead` | `EXCEPTION` | `EXCEPTION` | Can set. |
| `spark.sql.parquet.int96RebaseModeInWrite` | `EXCEPTION` | `EXCEPTION` | Can set. |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` | Remove. Already on. |
| `spark.databricks.delta.autoCompact.enabled` | `false` | `true` | Remove. Already on. |
| `spark.databricks.delta.schema.autoMerge.enabled` | `false` | `false` | Keep if needed. |
| `spark.databricks.delta.merge.enableLowShuffle` | N/A | `true` | Available on serverless. |
| `spark.databricks.delta.properties.defaults.enableDeletionVectors` | `false` | `true` | Deletion vectors default on for new tables. |
| `spark.databricks.delta.retentionDurationCheck.enabled` | `true` | `true` | Keep if needed (use with caution). |
| `spark.sql.parquet.compression.codec` | `snappy` | `zstd` | Set explicitly only if downstream needs snappy. |
| `spark.sql.orc.compression.codec` | `snappy` | `zstd` | Set explicitly only if needed. |
| `spark.sql.jsonGenerator.ignoreNullFields` | `true` | `true` | Keep if needed. |

### 3.3 Unsupported Configs (remove)

| Config | Category | Why |
|--------|----------|-----|
| `spark.executor.memory` | Resource | Serverless auto-scales memory |
| `spark.executor.cores` | Resource | Serverless manages cores |
| `spark.executor.instances` | Resource | Serverless auto-scales |
| `spark.executor.memoryOverhead` | Resource | Managed |
| `spark.driver.memory` | Resource | Managed |
| `spark.driver.cores` | Resource | Managed |
| `spark.driver.maxResultSize` | Resource | Managed |
| `spark.driver.extraJavaOptions` | Resource | No JVM access |
| `spark.executor.extraJavaOptions` | Resource | No JVM access |
| `spark.driver.extraClassPath` | Resource | Use requirements.txt |
| `spark.executor.extraClassPath` | Resource | Use requirements.txt |
| `spark.dynamicAllocation.enabled` | Scaling | Serverless has own scaling |
| `spark.dynamicAllocation.minExecutors` | Scaling | Managed |
| `spark.dynamicAllocation.maxExecutors` | Scaling | Managed |
| `spark.dynamicAllocation.initialExecutors` | Scaling | Managed |
| `spark.dynamicAllocation.executorIdleTimeout` | Scaling | Managed |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | Scaling | Managed |
| `spark.shuffle.service.enabled` | Shuffle | Not applicable |
| `spark.shuffle.compress` | Shuffle | Managed |
| `spark.shuffle.spill.compress` | Shuffle | Managed |
| `spark.network.timeout` | Network | Managed |
| `spark.rpc.message.maxSize` | Network | Managed |
| `spark.kryoserializer.buffer.max` | Serialization | Managed |
| `spark.serializer` | Serialization | Managed |
| `spark.kryo.registrationRequired` | Serialization | Not applicable |
| `spark.kryo.classesToRegister` | Serialization | Not applicable |
| `spark.sql.warehouse.dir` | Storage | Managed by Unity Catalog |
| `spark.hadoop.fs.defaultFS` | Storage | Not applicable |
| `spark.hadoop.*` | Storage | Hadoop configs not applicable |
| `fs.azure.account.key.*` | Storage | Use Unity Catalog external locations |
| `fs.azure.account.oauth2.*` | Storage | Use Unity Catalog |
| `spark.databricks.cluster.profile` | Cluster | Not applicable |
| `spark.databricks.passthrough.enabled` | Cluster | Use UC instead |
| `spark.databricks.pyspark.enableProcessIsolation` | Cluster | Managed |
| `spark.databricks.repl.allowedLanguages` | Cluster | Not applicable |
| `spark.databricks.acl.dfAclsEnabled` | Cluster | Use UC permissions |
| `spark.databricks.cluster.usageTags.*` | Cluster | Not applicable |

### 3.4 Config Decision Logic

```
Is the config in the Supported table?
  YES -> Did the default change?
    YES -> Does code depend on old default?
      YES -> Set explicitly to old value OR fix code
      NO  -> Remove the explicit set
    NO  -> Remove the explicit set (use new default)
  NO  -> Is it in the Unsupported table?
    YES -> Remove it
    NOT LISTED -> Test on serverless. It will error if unsupported.
```

---

## 4. Environment Variable Migration

Classic compute jobs often pass parameters via environment variables. Serverless does not support `os.environ` for job parameters. Migrate to widget parameters.

**Detection regex:**
```
os\.environ\.get\s*\(
os\.environ\[
os\.getenv\s*\(
```

**Action:** Replace with `dbutils.widgets.get()`:
```python
# BEFORE (classic compute):
import os
env = os.environ.get("ENV", "dev")
catalog = f"{env}_catalog"

# AFTER (serverless):
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
catalog = f"{env}_catalog"
```

In the job JSON, pass parameters via the `base_parameters` field on the task (see Section 7).

---

## 5. Package Dependency Patterns

### 5.1 Key JVM-to-Python Replacements

#### spark-excel (com.crealytics) to pandas + openpyxl

**Detection regex:**
```
com\.crealytics\.spark\.excel
com\.crealytics:spark-excel
format\(\s*["']com\.crealytics\.spark\.excel["']\s*\)
```

**Replacement pattern:**
```python
# BEFORE (classic compute with spark-excel JAR):
df = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .load("abfss://container@account.dfs.core.windows.net/file.xlsx"))

# AFTER (serverless -- pandas + openpyxl):
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

pdf = pd.read_excel(
    "/Volumes/catalog/schema/volume/file.xlsx",
    sheet_name="Sheet1",
    header=0
)
schema = StructType([
    StructField("col1", StringType()),
    StructField("col2", DoubleType()),
])
df = spark.createDataFrame(pdf, schema=schema)
```

#### Other JVM Library Replacements

| JVM Library | Purpose | Python Replacement |
|-------------|---------|-------------------|
| `com.databricks:spark-xml` | XML files | `spark.read.format("xml")` (built-in since DBR 14.3) |
| `com.databricks:spark-csv` | CSV files | `spark.read.csv()` (built-in since Spark 2.0) |
| `com.databricks:spark-avro` | Avro files | `spark.read.format("avro")` (built-in) |
| Custom Scala JARs | Business logic | Rewrite as PySpark operations or Python UDFs |
| `spark-monitoring` | Metrics/logging | System Tables + Azure Diagnostic Settings |

### 5.2 Python Packages Replaceable with Native Spark

| Package / Module | Common Use | Native Spark Replacement |
|-----------------|-----------|-------------------------|
| `koalas` | pandas-like API | `import pyspark.pandas as ps` (built-in) |
| `hashlib` (in UDFs) | Hashing | `F.sha2(F.concat_ws("\|", ...), 256)` |
| `json` (in UDFs) | JSON parsing | `F.get_json_object()` / `F.from_json()` |
| `re` (in UDFs) | Regex | `F.regexp_extract()` / `F.regexp_replace()` |
| `uuid` (in UDFs) | UUID generation | `F.expr("uuid()")` |
| `base64` (in UDFs) | Base64 encoding | `F.base64()` / `F.unbase64()` |
| `datetime` (in UDFs) | Date math | `F.date_add()` / `F.datediff()` / `F.months_between()` |
| `decimal` (in UDFs) | Precision rounding | `F.bround()` / `CAST(x AS DECIMAL(p,s))` |
| `collections.Counter` | Value counting | `F.groupBy().count()` |
| `dateutil` | Date parsing | `F.to_date(col, "MM/dd/yyyy")` |
| `fuzzywuzzy`/`rapidfuzz` | Fuzzy matching | `F.levenshtein()` / `F.soundex()` |
| `email-validator` | Email validation | `F.rlike(r"^[^@]+@[^@]+\\.[^@]+$")` |

### 5.3 requirements.txt Format and Rules

Location: `/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`

```
# Serverless Environment v4 (Python 3.12)
# Only include packages NOT pre-installed on serverless.
# Pin exact versions for reproducibility.

openpyxl==3.1.2
xlsxwriter==3.1.9
phonenumbers==8.13.24
```

**Rules:**
1. Pin all versions exactly: `package==x.y.z` (not `>=`)
2. Do NOT include pre-installed packages (pandas, numpy, pyarrow, requests, scipy, matplotlib, scikit-learn, mlflow, cryptography, boto3, azure-storage-blob, openpyxl, xlrd)
3. All packages must have a cp312 wheel or be pure Python
4. No git URLs or local paths -- only PyPI packages
5. No JVM/JAR libraries
6. No system-level C library dependencies
7. One shared file -- all serverless jobs reference the same requirements.txt
8. Test on a serverless notebook before deploying to jobs

---

## 6. Performance Patterns

Apply these during migration. They are safe behavioral equivalents that improve performance on serverless.

### 6.1 .count() > 0 to .first() is not None

**Detection regex:**
```
\.count\(\)\s*>\s*0
\.count\(\)\s*==\s*0
\.count\(\)\s*!=\s*0
if\s+.*\.count\(\)
```

**Fix:**
```python
# BEFORE (full table scan):
if df.count() > 0:
    process(df)

# AFTER (stops at first row):
if df.first() is not None:
    process(df)
```

### 6.2 Remove Manual Shuffle Partitions

**Detection regex:**
```
spark\.conf\.set.*shuffle\.partitions
(?i)SET\s+spark\.sql\.shuffle\.partitions
```

**Fix:** Remove the setting. Serverless auto-tunes shuffle partitions.

### 6.3 Remove .repartition() with Numeric Argument

**Detection regex:**
```
\.repartition\(\d+\)
\.coalesce\(\d+\)
\.repartition\(\d+\)\.write
\.coalesce\(\d+\)\.write
```

**Fix:** Remove numeric repartition/coalesce. Serverless auto-tunes partition counts.
```python
# BEFORE:
df = df.repartition(200)
df.write.mode("overwrite").saveAsTable("output")

# AFTER:
df.write.mode("overwrite").saveAsTable("output")

# EXCEPTION: .repartition() BY COLUMN for write partitioning is still valid:
df.repartition("date_col").write.partitionBy("date_col").mode("overwrite").saveAsTable("output")
```

### 6.4 /tmp to /local_disk0/tmp

**Detection regex:**
```
["']/tmp/
["']/tmp\b
\.save\(.*["']/tmp
open\(.*["']/tmp
```

**Fix:** Serverless uses `/local_disk0/tmp` for local temporary files:
```python
# BEFORE:
temp_path = "/tmp/staging_file.csv"

# AFTER:
temp_path = "/local_disk0/tmp/staging_file.csv"
```

### 6.5 Chained .withColumn() to .withColumns()

**Detection regex:**
```
\.withColumn\(.*\)\s*\.\s*withColumn\(
```

**Fix:** Merge chained `.withColumn()` calls into a single `.withColumns()` (available in Spark 3.4+):
```python
# BEFORE (creates new plan node per call):
df = (df
    .withColumn("col_a", F.upper(F.col("col_a")))
    .withColumn("col_b", F.trim(F.col("col_b")))
    .withColumn("col_c", F.coalesce(F.col("col_c"), F.lit(0)))
)

# AFTER (single plan node):
df = df.withColumns({
    "col_a": F.upper(F.col("col_a")),
    "col_b": F.trim(F.col("col_b")),
    "col_c": F.coalesce(F.col("col_c"), F.lit(0)),
})
```

### 6.6 Schema Inference -- Provide Explicit StructType

**Detection regex:**
```
inferSchema.*true
inferSchema.*True
\.option\(\s*["']inferSchema["']\s*,\s*["']true["']\s*\)
```

**Fix:** Replace schema inference with explicit StructType definitions. Inference requires an extra pass over the data:
```python
# BEFORE (extra pass to infer schema):
df = spark.read.option("inferSchema", "true").csv("/path/to/file.csv")

# AFTER (explicit schema, no extra pass):
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType()),
])
df = spark.read.schema(schema).csv("/path/to/file.csv")
```

### 6.7 Collect-Then-Loop Anti-Pattern

**Detection regex:**
```
\.collect\(\).*for\s+
for\s+\w+\s+in\s+.*\.collect\(\)
\.toPandas\(\).*\.apply\(
\.toPandas\(\).*\.iterrows\(
```

**Fix:** Rewrite as distributed DataFrame operations:
```python
# BEFORE (single-threaded):
rows = df.select("id", "amount").collect()
totals = {}
for row in rows:
    totals[row["id"]] = totals.get(row["id"], 0) + (row["amount"] or 0)

# AFTER (distributed):
result = df.groupBy("id").agg(
    F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total")
)
```

---

## 7. Job JSON Template

### 7.1 Classic Compute Job JSON (before)

```json
{
  "name": "example_job",
  "tasks": [
    {
      "task_key": "main_task",
      "notebook_task": {
        "notebook_path": "/Repos/project/notebooks/main",
        "base_parameters": {
          "env": "dev"
        }
      },
      "existing_cluster_id": "0123-456789-abcdefgh",
      "timeout_seconds": 3600,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      }
    }
  ],
  "format": "MULTI_TASK"
}
```

### 7.2 Serverless Job JSON (after)

```json
{
  "name": "example_job",
  "tasks": [
    {
      "task_key": "main_task",
      "notebook_task": {
        "notebook_path": "/Repos/project/notebooks/main",
        "base_parameters": {
          "env": "dev"
        }
      },
      "environment_key": "Default",
      "timeout_seconds": 3600,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      }
    }
  ],
  "environments": [
    {
      "environment_key": "Default",
      "spec": {
        "client": "4",
        "dependencies": [
          "/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
        ]
      }
    }
  ],
  "format": "MULTI_TASK",
  "queue": {
    "enabled": true
  },
  "performance_target": "PERFORMANCE_OPTIMIZED"
}
```

### 7.3 Transformation Rules (classic to serverless)

Apply these transformations to every job JSON:

| Field | Action |
|-------|--------|
| `existing_cluster_id` | **Remove** from each task |
| `new_cluster` | **Remove** from each task (if present) |
| `job_cluster_key` | **Remove** from each task (if present) |
| `job_clusters` | **Remove** from top level (if present) |
| `environment_key` | **Add** `"Default"` to each task |
| `environments` | **Add** top-level array with environment spec |
| `environments[0].spec.client` | Set to `"4"` |
| `environments[0].spec.dependencies` | Set to requirements.txt Volume path |
| `queue.enabled` | **Add** top-level, set to `true` |
| `performance_target` | **Add** top-level, set to `"PERFORMANCE_OPTIMIZED"` |
| `notebook_task.base_parameters` | **Keep** -- pass job parameters here |
| `timeout_seconds` | **Keep** -- no change needed |
| `email_notifications` | **Keep** -- no change needed |

### 7.4 Multi-Task Job Transformation

For jobs with multiple tasks, every task gets `environment_key` and the `environments` array is defined once at the top level:

```json
{
  "tasks": [
    {
      "task_key": "bronze",
      "environment_key": "Default",
      "notebook_task": { "notebook_path": "/Repos/project/notebooks/bronze" }
    },
    {
      "task_key": "silver",
      "environment_key": "Default",
      "notebook_task": { "notebook_path": "/Repos/project/notebooks/silver" },
      "depends_on": [{ "task_key": "bronze" }]
    },
    {
      "task_key": "gold",
      "environment_key": "Default",
      "notebook_task": { "notebook_path": "/Repos/project/notebooks/gold" },
      "depends_on": [{ "task_key": "silver" }]
    }
  ],
  "environments": [
    {
      "environment_key": "Default",
      "spec": {
        "client": "4",
        "dependencies": [
          "/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
        ]
      }
    }
  ],
  "queue": { "enabled": true },
  "performance_target": "PERFORMANCE_OPTIMIZED"
}
```

---

## 8. Complete Detection Regex Reference

All regex patterns consolidated for scanning an entire codebase in a single pass.

### Unsupported Operations
```
# Cache/Persist
\.persist\(\)
\.cache\(\)
\.unpersist\(\)
(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b
(?i)\bUNCACHE\s+TABLE\b

# Table maintenance (unsupported on serverless)
(?i)\bREFRESH\s+TABLE\b
(?i)\bMSCK\s+REPAIR\s+TABLE\b

# Materialized views (must use SQL Warehouse)
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b

# Global temp views
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?GLOBAL\s+TEMP(?:ORARY)?\s+VIEW\b
(?i)\bglobal_temp\.

# RDD APIs
\.rdd\.
\.rdd\b
spark\.sparkContext\.parallelize
sc\.parallelize

# Deprecated library management
dbutils\.library\.install\s*\(
dbutils\.library\.restartPython\s*\(
dbutils\.library\.installPyPI\s*\(

# %pip in notebooks
(?m)^%pip\s+install\s+
(?m)^%pip\s+uninstall\s+

# Threading anti-pattern
ThreadPoolExecutor
concurrent\.futures

# SparkContext direct access
spark\.sparkContext\.
sc\.getConf
sc\.setLogLevel
sc\.addFile
sc\.addPyFile
```

### Spark Config Detection
```
spark\.conf\.set\s*\(
spark\.conf\.get\s*\(
(?i)^\s*SET\s+spark\.
(?i)^\s*RESET\s+spark\.
sc\.getConf\.set\s*\(
```

### Environment Variables
```
os\.environ\.get\s*\(
os\.environ\[
os\.getenv\s*\(
```

### JVM/JAR Dependencies
```
com\.crealytics\.spark\.excel
com\.crealytics:spark-excel
com\.databricks:spark-xml
com\.databricks:spark-csv
com\.databricks:spark-avro
(?m)^\s*%scala
```

### Performance Anti-Patterns
```
# Count for existence check
\.count\(\)\s*>\s*0
\.count\(\)\s*==\s*0
\.count\(\)\s*!=\s*0

# Manual shuffle tuning
spark\.conf\.set.*shuffle\.partitions
(?i)SET\s+spark\.sql\.shuffle\.partitions

# Numeric repartition
\.repartition\(\d+\)
\.coalesce\(\d+\)

# Local /tmp path
["']/tmp/

# Schema inference
inferSchema.*[Tt]rue

# Chained withColumn
\.withColumn\(.*\)\s*\.\s*withColumn\(

# Collect-then-loop
for\s+\w+\s+in\s+.*\.collect\(\)
\.toPandas\(\).*\.iterrows\(
```
