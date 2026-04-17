# Serverless Eligibility Screening

Quick go/no-go screening for determining whether a Databricks job can run on serverless compute. Run this checklist against each job before starting migration work.

---

## Hard Blockers (No Serverless Path)

These make a job **ineligible for serverless general compute**. The job must stay on classic compute or be restructured.

| # | Blocker | Detection | Resolution Path |
|---|---------|-----------|----------------|
| H1 | **Scala notebooks** | Language = Scala in notebook header | Convert to PySpark (Path B) OR use JAR task (Public Preview) |
| H2 | **R notebooks** | Language = R in notebook header | Not supported. Stay on classic compute. |
| H3 | **Streaming jobs** | `readStream`, `writeStream`, `trigger`, `spark.readStream` | Not supported on serverless general compute. Use classic compute or DLT. |
| H4 | **GPU workloads** | `.cuda()`, `torch.device("cuda")`, `tensorflow.*GPU`, `cudf`, `cuml` | Requires GPU clusters. Stay on classic compute. |
| H5 | **Distributed training** | `horovod`, `TorchDistributor`, `spark_tensorflow_distributor` | Requires ML runtime clusters. Stay on classic. |
| H6 | **Non-Unity Catalog tables** | Hive metastore only, no UC catalog references | Must migrate to Unity Catalog first. |
| H7 | **Custom Docker containers** | Cluster config references Docker image | Not supported on serverless. |

### Detection Regex — Hard Blockers

```regex
# H1: Scala
(?m)^//\s*Databricks\s+notebook\s+source
(?m)^%scala

# H2: R
(?m)^#\s*Databricks\s+notebook\s+source.*\bR\b
(?m)^%r\b

# H3: Streaming
(?i)readStream|writeStream|\.trigger\s*\(|spark\.readStream|streaming\.stateStore

# H4: GPU
(?i)\.cuda\(\)|torch\.device\s*\(\s*["']cuda|tensorflow.*GPU|with\s+tf\.device.*GPU|cudf|cuml

# H5: Distributed training
(?i)horovod|TorchDistributor|spark_tensorflow_distributor|SparkTorchDistributor

# H7: Docker
(?i)docker_image|custom_container
```

---

## Soft Blockers (Requires Changes Before Serverless)

These require code or config changes before the job can run on serverless. They don't disqualify the job — they're work items.

| # | Blocker | Detection | Resolution | Effort |
|---|---------|-----------|------------|--------|
| S1 | **Init scripts** | Job config references init scripts | Move all functionality to requirements.txt or notebook code | Low-Med |
| S2 | **JAR libraries in notebooks** | `%jar`, Maven coordinates, `.jar` in library config | Rewrite in Python or move to JAR task | High |
| S3 | **com.crealytics.spark.excel** | `com.crealytics.spark.excel` in code | Replace with pandas + openpyxl pattern | Medium |
| S4 | **.persist() / .cache()** | `.persist()`, `.cache()`, `CACHE TABLE`, `UNCACHE TABLE` | Remove — serverless auto-manages | Low |
| S5 | **REFRESH TABLE** | `REFRESH TABLE` in SQL | Remove — serverless auto-refreshes | Low |
| S6 | **MSCK REPAIR TABLE** | `MSCK REPAIR TABLE` in SQL | Remove | Low |
| S7 | **CREATE MATERIALIZED VIEW** | `CREATE MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW` | Must use SQL Warehouse, not serverless general compute | Medium |
| S8 | **Global temp views** | `createOrReplaceGlobalTempView`, `global_temp.` | Convert to session-scoped temp views or tables | Low |
| S9 | **RDD APIs** | `sc.textFile`, `sc.parallelize`, `rdd.map`, `rdd.filter` | Rewrite as DataFrame operations | Medium |
| S10 | **Unsupported Spark configs** | `spark.executor.*`, `spark.driver.extra*`, `spark.dynamicAllocation.*` | Remove — serverless manages these. See config reference (05) | Low |
| S11 | **ANSI-unsafe code** | `CAST()`, `/ divisor`, `array[i]`, `boolean = 1`, `to_timestamp()` on invalid data | Apply ANSI-safe fixes. See ANSI reference (07) | Med-High |
| S12 | **Environment variables** | `os.environ.get()`, `os.environ[]`, `sys.argv` | Migrate to `dbutils.widgets.get()` | Low |
| S13 | **ThreadPoolExecutor** | `concurrent.futures`, `ThreadPoolExecutor`, `multiprocessing` | Warn: performance degrades on serverless. Use workflow for-each tasks. | Medium |
| S14 | **Incompatible wheels** | Wheel files not built for cp312 (Python 3.12) | Rebuild wheels on DBR 16.4 | Medium |
| S15 | **spark.sql.caseSensitive** | `spark.sql.caseSensitive` in config | Not supported. Rewrite code. | Medium |
| S16 | **SELECT * with row filters** | `SELECT *` on tables with row filters | Use explicit column lists | Low |
| S17 | **dbutils.library.install** | `dbutils.library.install`, `dbutils.library.restartPython` | Move to requirements.txt | Low |
| S18 | **/tmp file path** | Writes to `/tmp/` | Use `/local_disk0/tmp/` instead | Low |
| S19 | **Long-running jobs (>4hr)** | Job history shows >4hr runtime | Risk of session timeout. May need restructuring. | High |
| S20 | **Multiple chained withColumn** | 50+ `.withColumn()` calls in sequence | Replace with single `.withColumns()` call to avoid RecursionError | Medium |

### Detection Regex — Soft Blockers

```regex
# S1: Init scripts
init_scripts|dbfs:/.*\.sh|/Volumes/.*\.sh

# S2: JAR libraries
(?i)%jar|\.jar\b|maven.*coordinates

# S3: spark-excel
com\.crealytics\.spark\.excel

# S4: Persist/Cache
\.persist\(\)|\.cache\(\)|(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b|(?i)\bUNCACHE\s+TABLE\b

# S5: REFRESH TABLE
(?i)\bREFRESH\s+TABLE\b

# S6: MSCK REPAIR TABLE
(?i)\bMSCK\s+REPAIR\s+TABLE\b

# S7: Materialized Views
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b|(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b

# S8: Global temp views
(?i)createOrReplaceGlobalTempView|global_temp\.

# S9: RDD APIs
sc\.textFile|sc\.parallelize|\.rdd\.|rdd\.map|rdd\.filter|rdd\.flatMap

# S10: Unsupported configs
spark\.executor\.|spark\.driver\.extra|spark\.dynamicAllocation\.|spark\.shuffle\.service|spark\.serializer

# S11: ANSI patterns (see 07-ansi-compliance-reference.md for full list)
(?i)\bCAST\s*\(|\.cast\s*\(|(?i)\bto_timestamp\s*\(|(?i)\bto_date\s*\(

# S12: Environment variables
os\.environ\.get|os\.environ\[|sys\.argv

# S13: Multithreading
concurrent\.futures|ThreadPoolExecutor|multiprocessing\.Pool

# S14: Wheel files (check names)
\.whl\b

# S16: SELECT *
(?i)\bSELECT\s+\*\s+FROM\b

# S17: dbutils.library
dbutils\.library\.install|dbutils\.library\.restartPython

# S18: /tmp path
["']/tmp/

# S20: Chained withColumn (heuristic — many consecutive .withColumn calls)
\.withColumn\s*\(
```

---

## Screening Workflow

```
For each job in the manifest:
1. Check all Hard Blockers (H1-H7)
   ├── ANY hard blocker found?
   │   ├── YES → Job stays on classic OR needs restructuring
   │   │         Flag which blocker and resolution path
   │   └── NO → Continue to soft blockers
   │
2. Check all Soft Blockers (S1-S20)
   ├── Count and categorize findings
   │   ├── Total effort = sum of individual efforts
   │   ├── LOW effort total → ready for migration
   │   ├── MEDIUM effort total → migration with code changes
   │   └── HIGH effort total → needs design review before migration
   │
3. Produce eligibility report:
   - ELIGIBLE: No hard blockers, soft blockers identified with effort estimate
   - ELIGIBLE WITH CHANGES: No hard blockers, significant soft blockers
   - NOT ELIGIBLE: Hard blockers present, needs restructuring
   - RECOMMEND PATH CHANGE: e.g., prescribed Path C but has Scala → must be Path B
```

---

## Quick Eligibility Check (One-Liner per Job)

For a fast pass, check these three things:

1. **Language** — Is it Scala? → Needs conversion (Path B) or stays classic (Path A)
2. **Streaming** — Does it use readStream/writeStream? → Stays classic
3. **GPU/ML training** — Does it use CUDA/Horovod? → Stays classic

If none of those apply, the job is **likely eligible** for serverless with code changes.
