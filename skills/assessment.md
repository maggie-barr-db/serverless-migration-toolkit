# Serverless Migration Assessment Skill

Self-contained skill for evaluating a Databricks job's readiness for serverless migration. Everything needed to run a complete assessment is in this file. Fix code examples are in companion skills (ansi_fixes, dbr_upgrade, etc.) -- this skill detects and reports only.

---

## 1. Overview

**Purpose:** Evaluate a single Databricks job and produce a migration readiness report with findings, severity, evidence, and recommendations.

**Input:**
- Job ID (required) -- or notebook path for single-notebook assessment
- Prescribed migration path (optional): A, B, C, or D

**Output:**
- Structured assessment report (Section 6)
- Eligibility: PASS, FAIL, or PASS WITH CHANGES
- Recommended migration path with justification
- Effort estimate: LOW, MEDIUM, or HIGH

**Migration Paths:**
- **Path A** -- Scala to Scala on DBR 16.4 (classic compute)
- **Path B** -- Scala to PySpark on Serverless
- **Path C** -- PySpark/SQL to Serverless (most common)
- **Path D** -- SQL to DBSQL Serverless (SQL Warehouse)

**Assessment Phases:**

| Phase | What | How |
|-------|------|-----|
| 1 | Job Classification | Jobs API config, language, compute, Scala complexity |
| 2 | Eligibility Screening | Hard blockers + high-effort checks |
| 3 | Code Scan | Regex scan of all notebooks for 45 check patterns |
| 4 | Repo-Side | Job JSON, CI/CD script, variable group checks |
| 5 | Data Compatibility | Per-table protocol, properties, datetime, layout |

**Design Rules:**
1. NEVER recommend `spark.sql.ansi.enabled = false` or `SET ANSI_MODE = false`. ANSI mode cannot be disabled on serverless. Fix code with ANSI-safe functions instead.
2. Do NOT flag `abfss://` paths as non-Unity-Catalog. External ADLS paths registered in UC are valid.
3. Do NOT suggest Volumes as replacement for registered `abfss://` paths.
4. `USE CATALOG {{env}}_catalog` with 2-part table names is valid UC usage. Do NOT flag.
5. GA features only -- never recommend Public Preview features.

---

## 2. Job Classification

Pull job config via `GET /api/2.1/jobs/get?job_id=<id>`.

**Fields to extract:**

| Field | API Path |
|-------|----------|
| Job name | `settings.name` |
| DBR version | `settings.job_clusters[*].new_cluster.spark_version` |
| Language | Notebook headers + magic commands |
| Cluster spec | `settings.job_clusters[*].new_cluster` |
| Init scripts | `settings.job_clusters[*].new_cluster.init_scripts` |
| Libraries | `settings.job_clusters[*].new_cluster.libraries` |
| Spark configs | `settings.job_clusters[*].new_cluster.spark_conf` |
| Tasks | `settings.tasks` |
| Notebook paths | `settings.tasks[*].notebook_task.notebook_path` |
| Schedule | `settings.schedule` |
| Tags | `settings.tags` |

**Classify:**

| Dimension | Values | Detection |
|-----------|--------|-----------|
| Language | Python, SQL, Scala, R, Mixed | Notebook header + `%scala`/`%r`/`%sql`/`%python` magic |
| Compute | Classic job, Classic all-purpose, Interactive | Job config structure |
| Streaming | Yes / No | `readStream`, `writeStream`, `.trigger(` |
| GPU | Yes / No | `cuda`, `cudf`, `cuml`, GPU node types in cluster spec |

### Scala Complexity Analysis

For Scala notebooks, count heavy indicators to classify complexity.

**Heavy Scala indicators** (each occurrence adds 1 to score):

| Indicator | Regex |
|-----------|-------|
| case class definitions | `case\s+class\s+\w+` |
| Typed Dataset `.as[T]` | `\.as\[\w+\]` |
| UDF definitions | `udf\s*\(` |
| Option/Some/None | `Option\s*\(\|Some\s*\(\|\.getOrElse\|\.orElse` |
| Pattern matching | `match\s*\{\|case\s+Some\s*\(\|case\s+None` |
| Try/Success/Failure | `Try\s*\{\|Success\s*\(\|Failure\s*\(` |
| Typed literal maps | `typedLit\s*\(\s*Map\s*\(` |
| BigDecimal/RoundingMode | `BigDecimal\|RoundingMode` |
| SimpleDateFormat | `SimpleDateFormat` |
| Implicit vals | `implicit\s+val` |
| Scala collection ops | `\.map\s*\(\|\.filter\s*\(\|\.flatMap\s*\(\|\.foreach\s*\(` (on non-DataFrame) |
| Scala imports | `import\s+scala\.` |
| Typed var declarations | `var\s+\w+\s*:\s*\w+` |

**Light Scala indicators:**
- Cells are mostly `spark.sql("...")` calls
- Minimal DataFrame ops (just `spark.read` / `df.write`)
- No UDFs, no case classes, no pattern matching, no Option/Try
- Business logic lives in SQL strings

**Classification:**
- **0 heavy** = Light Scala (SQL wrapper) -- easy conversion
- **1-3 heavy** = Moderate Scala -- feasible conversion, careful UDF translation
- **4+ heavy** = Heavy Scala -- significant effort, consider Path A

---

## 3. Path Routing Logic

```
IF language = Scala:
    score = count_heavy_indicators()

    IF score = 0 (Light Scala):
        IF all business logic in spark.sql() strings:
            RECOMMEND Path D (DBSQL Serverless)
            ALSO OFFER Path B
        ELSE:
            RECOMMEND Path B (Scala to PySpark Serverless)

    ELIF score <= 3 (Moderate Scala):
        RECOMMEND Path B (Scala to PySpark Serverless)
        ALSO OFFER Path A (Scala 16.4) if conversion effort is concern

    ELSE (Heavy Scala, 4+):
        RECOMMEND Path A (Scala 16.4) -- stay Scala, upgrade DBR
        ALSO OFFER Path B -- flag as HIGH effort

ELIF language IN (Python, SQL, Mixed Python/SQL):
    IF all cells SQL AND no PySpark logic:
        RECOMMEND Path D (DBSQL Serverless)
    ELSE:
        RECOMMEND Path C (PySpark/SQL Serverless)

ELIF language = R:
    INELIGIBLE -- R not supported on serverless. Stay classic.
```

When recommending, include complexity analysis in report:
```
SCALA COMPLEXITY ANALYSIS:
  Heavy indicators found: <count>
    - case class definitions: <count>
    - UDF definitions: <count>
    - Option/Some/None: <count>
    - Pattern matching: <count>
    - BigDecimal/RoundingMode: <count>
    - SimpleDateFormat in UDFs: <count>
    - Typed Dataset .as[T]: <count>
  Light indicators:
    - spark.sql() calls: <count>
    - DataFrame read/write only: <yes/no>
    - SQL magic cells (%sql): <count>
  Classification: <Light / Moderate / Heavy>
  RECOMMENDED PATH: <A / B / D> -- <justification>
  ALTERNATIVE PATH: <X> -- <when better>
```

---

## 4. Eligibility Screening

### Hard Blockers -- Job CANNOT run on serverless

| ID | Blocker | Detection Regex | Resolution |
|----|---------|-----------------|------------|
| H1 | Scala notebooks | `(?m)^//\s*Databricks\s+notebook\s+source` or `(?m)^%scala\b` | Path A or Path B |
| H2 | R notebooks | `(?m)^#\s*Databricks\s+notebook\s+source.*\bR\b` or `(?m)^%r\b` | Stay classic |
| H3 | GPU workloads | `(?i)\.cuda\s*\(` `(?i)torch\.device\s*\(\s*["']cuda` `(?i)\bcudf\b` `(?i)\bcuml\b` `(?i)\bcupyx?\b` | Stay classic ML Runtime |
| H4 | Structured streaming (complex stateful) | `(?i)\.flatMapGroupsWithState` `(?i)\.mapGroupsWithState\s*\(` `(?i)streaming\.stateStore` | Classic or DLT |
| H5 | Non-UC data (legacy Hive only) | `(?i)hive_metastore\.` `(?i)spark\.databricks\.passthrough\.enabled` `(?i)fs\.azure\.account\.key\.` `(?i)fs\.azure\.account\.oauth2\.` | Migrate to UC first |
| H6 | Global temp views | `(?i)createOrReplaceGlobalTempView\s*\(` `(?i)createGlobalTempView\s*\(` `(?i)\bglobal_temp\.` | Session-scoped views or tables |
| H7 | Distributed training | `(?i)horovod` `(?i)TorchDistributor` `(?i)spark_tensorflow_distributor` | Classic ML Runtime |
| H8 | Custom Docker containers | `(?i)docker_image` `(?i)custom_container` | Not supported on serverless |

**Important:** Do NOT flag `abfss://` paths or `USE CATALOG {{env}}_catalog` as non-UC.

### Soft Blockers -- Requires changes before serverless

| ID | Blocker | Detection Regex | Fix | Effort |
|----|---------|-----------------|-----|--------|
| S1 | Init scripts | `init_scripts` `dbfs:/.*\.sh` `/Volumes/.*\.sh` | Move to requirements.txt | Low-Med |
| S2 | JAR libraries | `(?i)%jar\b` `(?i)\.jar\b` `(?i)maven.*coordinates` | Rewrite in Python or JAR task | High |
| S3 | spark.excel JAR | `com\.crealytics\.spark\.excel` | pandas + openpyxl pattern | Medium |
| S4 | .persist()/.cache() | `\.persist\s*\(` `\.cache\s*\(` `(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b` | Remove | Low |
| S5 | REFRESH TABLE | `(?i)\bREFRESH\s+TABLE\b` | Remove | Low |
| S6 | MSCK REPAIR TABLE | `(?i)\bMSCK\s+REPAIR\s+TABLE\b` | Remove | Low |
| S7 | Materialized views | `(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b` | SQL Warehouse only | Medium |
| S8 | RDD APIs | `sc\.textFile\s*\(` `sc\.parallelize\s*\(` `\.rdd\.` `rdd\.map\s*\(` | DataFrame ops | Medium |
| S9 | Unsupported configs | `spark\.executor\.` `spark\.driver\.extra` `spark\.dynamicAllocation\.` | Remove | Low |
| S10 | ANSI-unsafe code | See Section 5, checks 10-22 | ANSI-safe functions | Med-High |
| S11 | Environment vars | `os\.environ\.get` `os\.environ\[` `sys\.argv` | `dbutils.widgets.get()` | Low |
| S12 | ThreadPoolExecutor | `concurrent\.futures` `ThreadPoolExecutor` `multiprocessing\.Pool` | Workflows for-each | Medium |
| S13 | Incompatible wheels | `\.whl\b` (check for cp312) | Rebuild for Python 3.12 | Medium |
| S14 | dbutils.library | `dbutils\.library\.install` `dbutils\.library\.restartPython` | requirements.txt | Low |
| S15 | /tmp file path | `["']/tmp/` | `/local_disk0/tmp/` | Low |
| S16 | SELECT * with row filters | `(?i)\bSELECT\s+\*\s+FROM\b` | Explicit column lists | Low |
| S17 | Chained withColumn (>20) | `\.withColumn\s*\(` (count per cell) | `.withColumns()` | Medium |
| S18 | spark.sql.caseSensitive | `spark\.sql\.caseSensitive` | Rewrite code | Medium |
| S19 | Long-running jobs (>4hr) | Check job history | Session timeout risk | High |

### Screening Workflow

```
1. Check all Hard Blockers (H1-H8)
   ANY found? --> FAIL (stays classic or needs restructuring)
   NONE found? --> Continue

2. Check all Soft Blockers (S1-S19)
   Count and categorize:
   - LOW total effort --> Ready for migration
   - MEDIUM total effort --> Migration with code changes
   - HIGH total effort --> Design review needed

3. Eligibility:
   - PASS: No hard blockers, minimal soft blockers
   - PASS WITH CHANGES: No hard blockers, soft blockers need work
   - FAIL: Hard blockers present
```

---

## 5. Code Scan Checks

Run ALL patterns below against every notebook referenced by the job. Report matches with notebook path, cell number, line number, matched text, and recommended fix.

### ANSI Compliance (CRITICAL -- serverless enforces ANSI, cannot be disabled)

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 10 | CRIT | CAST to numeric/date/timestamp | `(?i)\bCAST\s*\(\s*\S+\s+AS\s+(?:INT\|INTEGER\|BIGINT\|SMALLINT\|TINYINT\|FLOAT\|DOUBLE\|DECIMAL\|NUMERIC\|DATE\|TIMESTAMP\|BOOLEAN)\s*\)` | TRY_CAST (SQL) or `F.expr("TRY_CAST(col AS TYPE)")` |
| 10b | CRIT | PySpark .cast() | `.cast\s*\(\s*["'](?:int\|integer\|bigint\|smallint\|tinyint\|float\|double\|decimal\|date\|timestamp\|boolean)["']\s*\)` | TRY_CAST via F.expr() |
| 10c | CRIT | PySpark .cast(Type()) | `.cast\s*\(\s*(?:IntegerType\|LongType\|ShortType\|ByteType\|FloatType\|DoubleType\|DecimalType\|DateType\|TimestampType\|BooleanType)\s*\(\s*\)\s*\)` | TRY_CAST via F.expr() |
| 11 | CRIT | Division (PySpark) | `F\.col\([^)]+\)\s*/\s*F\.col` | TRY_DIVIDE(a, b) or null guard |
| 11b | CRIT | Division (.divide) | `\.divide\s*\(` | TRY_DIVIDE(a, b) |
| 12 | CRIT | Modulo/remainder | `(?i)\bPMOD\s*\(` | `value % NULLIF(divisor, 0)` |
| 13 | CRIT | BOOLEAN = INT (=1) | `(?i)\b\w+\s*=\s*1\b(?![\.\d])` | `col IS TRUE` (cross-ref schema) |
| 13b | CRIT | BOOLEAN = INT (=0) | `(?i)\b\w+\s*=\s*0\b(?![\.\d])` | `col IS NOT TRUE` (cross-ref schema) |
| 13c | CRIT | PySpark BOOLEAN == int | `F\.col\s*\(\s*["'][^"']+["']\s*\)\s*==\s*[01]\b` | `col IS TRUE` via F.expr() |
| 14 | CRIT | Array bracket access | `\w+\[\d+\]` | TRY_ELEMENT_AT (1-indexed) |
| 14b | CRIT | PySpark .getItem(int) | `\.getItem\s*\(\d+\)` | TRY_ELEMENT_AT |
| 14c | CRIT | F.split()[i] | `F\.split\s*\([^)]+\)\s*\[\d+\]` | TRY_ELEMENT_AT |
| 14d | CRIT | F.element_at() | `F\.element_at\s*\(` | Wrap with TRY_ELEMENT_AT |
| 15 | CRIT | Map string key access | `\w+\['[^']+'\]` | TRY_ELEMENT_AT(map, 'key') |
| 15b | CRIT | Map double-quote key | `\w+\["[^"]+"\]` | TRY_ELEMENT_AT(map, 'key') |
| 15c | CRIT | PySpark map .getItem | `\.getItem\s*\(\s*["']` | TRY_ELEMENT_AT |
| 16 | CRIT | to_date() SQL | `(?i)\bto_date\s*\(` | try_to_date |
| 16b | CRIT | to_timestamp() SQL | `(?i)\bto_timestamp\s*\(` | try_to_timestamp |
| 16c | CRIT | F.to_date() PySpark | `F\.to_date\s*\(` | `F.expr("try_to_date(col, 'fmt')")` |
| 16d | CRIT | F.to_timestamp() PySpark | `F\.to_timestamp\s*\(` | `F.expr("try_to_timestamp(col, 'fmt')")` |
| 17 | CRIT | make_timestamp | `(?i)\bmake_timestamp\s*\(` | try_make_timestamp |
| 17b | CRIT | make_date | `(?i)\bmake_date\s*\(` | try_make_date |
| 18 | CRIT | SUM on INT (overflow) | `(?i)\bSUM\s*\(\s*\w+\s*\)` | `SUM(CAST(col AS BIGINT))` (cross-ref schema) |
| 18b | CRIT | SUM on backtick col | `(?i)\bSUM\s*\(\s*` `` `[^`]+` `` `\s*\)` | `SUM(CAST(col AS BIGINT))` |
| 19 | HIGH | INT overflow arithmetic | `F\.col\([^)]+\)\s*[*+]\s*F\.col` | CAST to BIGINT before arithmetic |
| 20 | CRIT | F.lit() wrapping format string | `F\.to_date\s*\([^,]+,\s*F\.lit\s*\(` | Remove F.lit() -- plain string |
| 20b | CRIT | F.lit() in to_timestamp | `F\.to_timestamp\s*\([^,]+,\s*F\.lit\s*\(` | Remove F.lit() -- plain string |
| 20c | CRIT | f.lit() in to_date (lowercase) | `f\.to_date\s*\([^,]+,\s*f\.lit\s*\(` | Remove f.lit() -- plain string |
| 20d | CRIT | f.lit() in to_timestamp (lowercase) | `f\.to_timestamp\s*\([^,]+,\s*f\.lit\s*\(` | Remove f.lit() -- plain string |
| 20e | CRIT | F.lit() in try_to_timestamp | `F\.try_to_timestamp\s*\([^,]+,\s*F\.lit\s*\(` | Remove F.lit() -- plain string |
| 21 | HIGH | Implicit string-to-numeric | Manual: check JOIN conditions with mismatched types | Explicit TRY_CAST in joins |
| 22 | MED | parse_url on invalid URLs | `(?i)\bparse_url\s*\(` | try_parse_url |
| 22b | MED | url_decode | `(?i)\burl_decode\s*\(` | try_parse_url |

### Unsupported Operations

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 23 | HIGH | .persist() | `\.persist\s*\(` | Remove |
| 23b | HIGH | .cache() | `\.cache\s*\(` | Remove |
| 23c | HIGH | .unpersist() | `\.unpersist\s*\(` | Remove |
| 23d | HIGH | CACHE TABLE | `(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b` | Remove |
| 23e | HIGH | UNCACHE TABLE | `(?i)\bUNCACHE\s+TABLE\b` | Remove |
| 24 | HIGH | REFRESH TABLE | `(?i)\bREFRESH\s+TABLE\b` | Remove -- auto-refreshes |
| 25 | HIGH | MSCK REPAIR TABLE | `(?i)\bMSCK\s+REPAIR\s+TABLE\b` | Remove |
| 26 | HIGH | CREATE MATERIALIZED VIEW | `(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b` | Must use SQL Warehouse |
| 26b | HIGH | REFRESH MATERIALIZED VIEW | `(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b` | Must use SQL Warehouse |
| 27 | MED | Temp view in loops | `(?i)createOrReplaceTempView\s*\(` + loop context: `(?i)\bfor\s+\w+\s+in\b` or `(?i)\bwhile\b` | Verify not created/consumed in tight loops |

### Spark Config Issues

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 28 | CRIT | spark.conf.set() | `spark\.conf\.set\s*\(` | Check key against unsupported list below |
| 28b | CRIT | SQL SET spark.* | `(?i)^\s*SET\s+spark\.` | Check key against unsupported list below |
| 29 | CRIT | ANSI disabled | `(?i)spark\.sql\.ansi\.enabled.*false` | Remove. Fix code. NEVER disable ANSI. |
| 29b | CRIT | SET ANSI false | `(?i)SET\s+spark\.sql\.ansi\.enabled\s*=\s*false` | Remove. Fix code. |
| 29c | CRIT | SET ANSI_MODE false | `(?i)SET\s+ANSI_MODE\s*=\s*false` | Remove. Fix code. |

**Unsupported config keys** (match against any spark.conf.set or SET statement):

| Config Key | Action | Replacement |
|------------|--------|-------------|
| `spark.sql.storeAssignmentPolicy` | Remove | Fix code with ANSI-safe patterns |
| `spark.network.timeout` | Remove | `spark.databricks.execution.timeout` if needed |
| `spark.sql.parquet.int96RebaseModeInRead` | Evaluate | Can set LEGACY for old parquet |
| `spark.sql.parquet.int96RebaseModeInWrite` | Evaluate | Can set LEGACY if needed |
| `spark.sql.parquet.datetimeRebaseModeInRead` | Evaluate | Can set LEGACY for Spark 2.x files |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | Evaluate | Can set LEGACY if needed |
| `spark.databricks.safespark.externalUDF.plan.limit` | Remove | Not available |
| `spark.driver.extraJavaOptions` | Remove | No JVM access |
| `spark.databricks.delta.retentionDurationCheck.enabled` | Remove | Use `ALTER TABLE SET TBLPROPERTIES` |
| `spark.databricks.delta.schema.autoMerge.enabled` | Remove | `MERGE WITH SCHEMA EVOLUTION` |
| `spark.databricks.delta.optimizeWrite.enabled` | Remove | Default ON on serverless |
| `spark.databricks.delta.autoCompact.enabled` | Remove | Default ON on serverless |
| `spark.sql.broadcastTimeout` | Remove | Managed internally |
| `spark.sql.caseSensitive` | Remove | Rewrite code |
| `spark.sql.streaming.stateStore.stateSchemaCheck` | Remove | Not available |
| `spark.executor.*` | Remove | Managed by serverless |
| `spark.driver.memory` / `spark.driver.cores` | Remove | Managed by serverless |
| `spark.dynamicAllocation.*` | Remove | Auto-scaling built in |
| `spark.shuffle.service.*` | Remove | Not applicable |
| `spark.serializer` | Remove | Serverless serialization |
| `spark.sql.warehouse.dir` | Remove | Managed by UC |
| `spark.hadoop.*` | Remove | Not applicable |
| `fs.azure.*` | Remove | Use UC external locations |
| `spark.databricks.cluster.*` | Remove | Not applicable |
| `spark.databricks.passthrough.enabled` | Remove | Use UC permissions |

### Environment and Dependencies

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 30 | HIGH | os.environ.get() | `os\.environ\.get\s*\(` | `dbutils.widgets.get()` |
| 30b | HIGH | os.environ[] | `os\.environ\[` | `dbutils.widgets.get()` |
| 30c | HIGH | os.getenv() | `os\.getenv\s*\(` | `dbutils.widgets.get()` |
| 30d | HIGH | sys.argv | `sys\.argv` | `dbutils.widgets.get()` |
| 30e | HIGH | Non-standard spark.conf | `spark\.conf\.(?:get\|set)\s*\(\s*["'](?!spark\.\|delta\.)` | `dbutils.widgets.get()` |
| 31 | MED | Init scripts in config | `init_scripts` | Move to requirements.txt |
| 31b | MED | DBFS shell scripts | `dbfs:/.*\.sh` | Move to requirements.txt |
| 31c | MED | Volume shell scripts | `/Volumes/.*\.sh` | Move to requirements.txt |
| 32 | MED | %pip install | `(?m)^%pip\s+install\b` | Centralized requirements.txt |
| 33 | HIGH | dbutils.library.install | `dbutils\.library\.install\s*\(` | requirements.txt |
| 33b | HIGH | dbutils.library.restartPython | `dbutils\.library\.restartPython\s*\(` | requirements.txt |
| 34 | LOW | Unpinned dependencies | Review `%pip install` and requirements.txt for missing `==X.Y.Z` | Pin all versions |
| 35 | HIGH | com.crealytics.spark.excel | `com\.crealytics\.spark\.excel` | pandas + openpyxl pattern |
| 35b | HIGH | spark.excel format() | `\.format\s*\(\s*["']com\.crealytics\.spark\.excel["']\s*\)` | pandas + openpyxl pattern |
| 36 | MED | Incompatible wheels | `\.whl\b` (check filename for cp312) | Rebuild for Python 3.12 |

### Performance Patterns

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 37 | LOW | .count() > 0 | `\.count\s*\(\s*\)\s*>\s*0` | `.first() is not None` |
| 37b | LOW | .count() == 0 | `\.count\s*\(\s*\)\s*==\s*0` | `.first() is None` |
| 37c | LOW | .count() != 0 | `\.count\s*\(\s*\)\s*!=\s*0` | `.first() is not None` |
| 37d | LOW | if .count() | `if\s+.*\.count\s*\(\s*\)` | `.first() is not None` or `.isEmpty` |
| 38 | MED | concurrent.futures | `concurrent\.futures` | Workflows for-each tasks |
| 38b | MED | ThreadPoolExecutor | `ThreadPoolExecutor` | Workflows for-each tasks |
| 38c | MED | multiprocessing.Pool | `multiprocessing\.Pool` | Workflows for-each tasks |
| 38d | MED | multiprocessing.Process | `multiprocessing\.Process` | Workflows for-each tasks |
| 39 | MED | Chained .withColumn() | `\.withColumn\s*\(` (flag if >20 per cell) | Single `.withColumns()` call |
| 40 | MED | SELECT * FROM | `(?i)\bSELECT\s+\*\s+FROM\b` | Explicit column lists |
| 40b | MED | INSERT...SELECT * | `(?i)\bINSERT\s+(?:INTO\|OVERWRITE)\s+.*?\bSELECT\s+\*` | Explicit column lists |

### File Path and Schema Issues

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 41 | MED | /tmp file path (string) | `["']/tmp/[^"']*["']` | `/local_disk0/tmp/` |
| 41b | MED | /tmp file path (open) | `open\s*\(\s*["']/tmp/` | `/local_disk0/tmp/` |
| 42 | MED | createDataFrame no schema | `spark\.createDataFrame\s*\([^,)]+\)(?!\s*,)` | Provide explicit StructType |
| 43 | HIGH | JSON/API without schema | `spark\.createDataFrame\s*\(.*?(?:response\|json\|api\|result\|data)\b` | Define explicit StructType |
| 43b | HIGH | requests.get (API call) | `(?i)requests\.get\s*\(` | Check if followed by createDataFrame without schema |
| 43c | HIGH | requests.post (API call) | `(?i)requests\.post\s*\(` | Check if followed by createDataFrame without schema |

### Metadata and Optimization

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| 44 | HIGH | _metadata column ref | `(?i)['"]_metadata['"]` | Check row tracking; disable or rename |
| 44b | HIGH | col("_metadata") | `(?i)col\s*\(\s*['"]_metadata['"]` | Check row tracking |
| 44c | HIGH | ._metadata accessor | `(?i)\._metadata\b` | Check row tracking |
| 44d | HIGH | backtick _metadata | `` (?i)`_metadata` `` | Check row tracking |
| 45 | INFO | Pure SQL notebook | All cells SQL, no `%python`, no `spark.` calls, no `dbutils.` (except widgets) | Recommend Path D (DBSQL) |

### DBR 13-to-16 Breaking Changes (additional patterns from resource 15)

| ID | Sev | Pattern Name | Regex | Fix Reference |
|----|-----|-------------|-------|---------------|
| BC1 | CRIT | Scala .cast(Type) no parens | `.cast\s*\(\s*(?:IntegerType\|LongType\|ShortType\|ByteType\|FloatType\|DoubleType\|DecimalType)\s*\)` | TRY_CAST |
| BC2 | CRIT | Scala division $ syntax | `\$"[^"]+"\s*/\s*\$"` | TRY_DIVIDE |
| BC3 | MED | String concat with \|\| | `(?i)\|\|(?!\|)` | CONCAT_WS or COALESCE for null safety |
| BC4 | MED | ROUND negative scale | `(?i)\bROUND\s*\([^,]+,\s*-\d+\)` | Verify behavior manually |
| BC5 | MED | CSV write null handling | `\.write.*\.format\s*\(\s*["']csv["']\)` | `.option("nullValue","").option("quote","")` |
| BC6 | MED | CSV write shorthand | `\.write\.csv\s*\(` | Check null handling options |
| BC7 | LOW | ZORDER BY | `(?i)\bZORDER\s+BY\b` | Consider Liquid Clustering (16.4+) |
| BC8 | LOW | Manual shuffle partitions | `spark\.sql\.shuffle\.partitions` | Serverless auto-tunes; remove |
| BC9 | LOW | .repartition(N) | `\.repartition\s*\(\d+\)` | Serverless auto-tunes |
| BC10 | LOW | .coalesce(N) | `\.coalesce\s*\(\d+\)` | Evaluate if still needed |
| BC11 | LOW | .partitionBy on write | `\.partitionBy\s*\(` | Evaluate; Liquid Clustering may be better |
| BC12 | LOW | display(.count()) | `display\s*\(.*\.count\s*\(\s*\)` | Remove or use approximation |
| BC13 | LOW | print(.count()) | `print\s*\(.*\.count\s*\(\s*\)` | Remove or use approximation |

### Repo-Side Checks (Phase 4)

| ID | Sev | Pattern Name | What to Check | Fix Reference |
|----|-----|-------------|---------------|---------------|
| 46 | REQ | Job JSON transformation | `job_clusters` present (remove), `environments` absent (add), `environment_key` absent (add), `queue` absent (add), `performance_optimized` absent (add) | Apply serverless template |
| 47 | REQ | PowerShell %env_name% | `\.Replace\s*\(\s*"%env_name%"` in deploy script | Add `.Replace("%env_name%","$env_name")` |
| 48 | REQ | Variable group env_name | Verify `env_name` in ADO variable group per env (dev/uat/prod) | Add if missing |
| 49 | REQ | Job parameters | `parameters` block in JSON with `PATH_LANDING` and `PATH_DATALAKE` using `%env_name%` substitution | Add parameters block |

**Serverless JSON template (Check 46):**

After transformation, job JSON must have:
```json
{
  "environments": [{
    "environment_key": "serverless_environment_v1",
    "spec": {
      "client": "4",
      "dependencies": [
        "-r /Volumes/%env_name%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
      ]
    }
  }],
  "queue": { "enabled": true },
  "performance_optimized": true,
  "parameters": [
    { "name": "PATH_LANDING", "default": "abfss://<container>@<storage_account_%env_name%>.dfs.core.windows.net/" },
    { "name": "PATH_DATALAKE", "default": "abfss://<container>@<storage_account_%env_name%>.dfs.core.windows.net/" }
  ]
}
```

Remove: `job_clusters`, `job_cluster_key` from tasks.
Add: `environment_key: "serverless_environment_v1"` to each task.

### Data Compatibility Checks (Phase 5)

For every output table written by the job:

| Check | Risk Trigger | Fix |
|-------|-------------|-----|
| Table type (managed vs external) | External = no Predictive Optimization | Schedule manual OPTIMIZE + ANALYZE |
| Delta protocol (reader/writer version) | Writer v1-2 = likely auto-upgrade on new DBR | Record versions; coordinate timing |
| Row tracking (`delta.enableRowTracking`) | Enabled AND code references `_metadata` | Disable row tracking or rename column |
| Retention (`delta.deletedFileRetentionDuration`) | < 7 days | Extend before migration |
| Datetime columns | `'00000000'`, invalid ISO, pre-1582 dates in string cols | Ensure TRY_CAST/try_to_date in code |
| BOOLEAN columns | BOOLEAN type + `= 1` comparisons in code | IS TRUE / IS NOT TRUE |
| Parquet writers | Spark 2.x writes + no rebase mode set | Set `delta.parquet.datetimeRebaseModeInRead = LEGACY` |
| File layout | >1000 files < 32MB (especially external) | OPTIMIZE before migration |
| Partition scheme | High cardinality or multi-level | Document for post-migration optimization |

**Pre-migration action priority:**

| Priority | Trigger | Action |
|----------|---------|--------|
| P0 | External table >1000 small files | `OPTIMIZE catalog.schema.table` |
| P0 | Row tracking + _metadata conflict | Disable row tracking or rename in code |
| P0 | Invalid date strings in data | Ensure code uses TRY_CAST / try_to_date |
| P1 | Spark 2.x parquet, no rebase mode | `ALTER TABLE SET TBLPROPERTIES ('delta.parquet.datetimeRebaseModeInRead' = 'LEGACY')` |
| P1 | BOOLEAN columns with INT comparison | Fix comparisons in code |
| P2 | Short retention duration | Extend before migration |
| P2 | External tables missing statistics | `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` |

---

## 6. Report Template

Produce this structured output for every assessed job.

```
JOB ASSESSMENT REPORT
===========================================================================
Job: <job_name> (<job_id>)
Current: <language> on DBR <version> (<compute_type>)
Prescribed Path: <path>
Recommended Path: <path> (with justification if different)
Eligibility: PASS | FAIL | PASS WITH CHANGES
Assessment Date: <date>
===========================================================================

HARD BLOCKERS: <count>
---------------------------------------------------------------------------
  [Check <ID>] <check_name>
    Evidence: <what was found, with notebook path / cell / line>
    Impact: <why this blocks serverless>
    Recommendation: <specific fix or alternative path>

HIGH EFFORT: <count>
---------------------------------------------------------------------------
  [Check <ID>] <check_name>
    Evidence: <what was found>
    Impact: <effort required>
    Recommendation: <specific fix>

ANSI COMPLIANCE: <count> findings
---------------------------------------------------------------------------
  [Check <ID>] <pattern_name>
    Location: <notebook_path>, cell <N>, line <N>
    Code: <matched code snippet>
    Fix: <specific ANSI-safe replacement>

UNSUPPORTED OPERATIONS: <count>
---------------------------------------------------------------------------
  [Check <ID>] <operation_name>
    Location: <notebook_path>, cell <N>, line <N>
    Code: <matched code snippet>
    Fix: <replacement pattern>

CONFIG ISSUES: <count>
---------------------------------------------------------------------------
  [Check <ID>] <config_key>
    Current value: <value>
    Action: REMOVE | REPLACE
    Replacement: <new approach or "not needed">

DEPENDENCY ISSUES: <count>
---------------------------------------------------------------------------
  [Check <ID>] <dependency_name>
    Location: <notebook_path> or init script
    Issue: <what is wrong>
    Fix: <move to requirements.txt / rebuild wheel / etc.>

PERFORMANCE RECOMMENDATIONS: <count>
---------------------------------------------------------------------------
  [Check <ID>] <pattern_name>
    Location: <notebook_path>, cell <N>
    Current: <anti-pattern code>
    Recommended: <optimized pattern>

REPO-SIDE CHANGES NEEDED:
---------------------------------------------------------------------------
  [Check 46] Job JSON transformation
    Status: NEEDED | ALREADY DONE
    Changes: <list of JSON changes>
  [Check 47] PowerShell %env_name% replacement
    Status: PRESENT | MISSING
  [Check 48] Variable group env_name
    Status: PRESENT | MISSING
  [Check 49] Job parameters
    Status: PRESENT | MISSING

TABLE FINDINGS: <count> tables checked
---------------------------------------------------------------------------
  <catalog.schema.table_name>
    Type: MANAGED | EXTERNAL
    Protocol: reader=<N>, writer=<N>
    Risk: HIGH | MEDIUM | LOW
    Issues: <comma-separated list>
    Actions: <pre-migration actions needed>

===========================================================================
EFFORT ESTIMATE: LOW | MEDIUM | HIGH
  Based on: <total_findings> total findings
            <code_changes> code changes needed
            <repo_changes> repo-side changes needed
            <table_actions> table pre-migration actions
  Estimated: <hours> hours migration effort

  LOW    = 0 hard blockers, <5 code changes, standard repo transformation
  MEDIUM = 0 hard blockers, 5-20 code changes, some refactoring needed
  HIGH   = hard blockers OR >20 code changes OR major refactoring
===========================================================================
```

---

## Appendix: Quick-Reference Check Index

| Check | Name | Sev | Category |
|-------|------|-----|----------|
| 1 (H1) | Scala notebooks | Hard Blocker | Language |
| 2 (H2) | R notebooks | Hard Blocker | Language |
| 3 (H3/H4) | Streaming / GPU | Hard Blocker | Compute |
| 4 (H5) | Non-UC data access | Hard Blocker | Data Access |
| 5 (H6) | Global temp views | Hard Blocker | Unsupported |
| 6 | JAR libraries | High Effort | Dependencies |
| 7 | RDD APIs | High Effort | API |
| 8 | DBFS mounts | High Effort | Data Access |
| 9 | Non-AvailableNow triggers | High Effort | Streaming |
| 10 | CAST to numeric/date/timestamp | Critical | ANSI |
| 11 | Division by zero | Critical | ANSI |
| 12 | Modulo by zero | Critical | ANSI |
| 13 | BOOLEAN = INT | Critical | ANSI |
| 14 | Array out of bounds | Critical | ANSI |
| 15 | Map key not found | Critical | ANSI |
| 16 | to_date / to_timestamp | Critical | ANSI |
| 17 | make_timestamp / make_date | Critical | ANSI |
| 18 | SUM on INT overflow | Critical | ANSI |
| 19 | Integer overflow arithmetic | High | ANSI |
| 20 | F.lit() wrapping format strings | Critical | ANSI |
| 21 | Implicit string-to-numeric | High | ANSI |
| 22 | parse_url on invalid URLs | Medium | ANSI |
| 23 | .persist() / .cache() | High | Unsupported |
| 24 | REFRESH TABLE | High | Unsupported |
| 25 | MSCK REPAIR TABLE | High | Unsupported |
| 26 | Materialized views | High | Unsupported |
| 27 | Temp view in loops | Medium | Unsupported |
| 28 | Unsupported Spark configs | Critical | Config |
| 29 | ANSI mode disabled | Critical | Config |
| 30 | os.environ / sys.argv | High | Environment |
| 31 | Init script dependencies | Medium | Dependencies |
| 32 | %pip install | Medium | Dependencies |
| 33 | dbutils.library.install | High | Dependencies |
| 34 | Unpinned dependencies | Low | Dependencies |
| 35 | com.crealytics.spark.excel | High | Dependencies |
| 36 | Incompatible wheels | Medium | Dependencies |
| 37 | .count() > 0 | Low | Performance |
| 38 | ThreadPoolExecutor | Medium | Performance |
| 39 | Chained withColumn (>20) | Medium | Performance |
| 40 | SELECT * | Medium | Performance |
| 41 | /tmp file paths | Medium | File Path |
| 42 | createDataFrame no schema | Medium | Schema |
| 43 | JSON/API without schema | High | Schema |
| 44 | _metadata column conflict | High | Metadata |
| 45 | Pure SQL (Path D candidate) | Info | Optimization |
| 46 | Job JSON transformation | Required | Repo |
| 47 | PowerShell %env_name% | Required | Repo |
| 48 | Variable group env_name | Required | Repo |
| 49 | Job parameters | Required | Repo |
