# Comprehensive Job Assessment — Master Assessment Skill

The single document Genie Code loads to evaluate any individual Databricks job for serverless migration readiness. Combines checks from three sources: notebook audit skill, customer feedback, and the customer's Serverless Migration SOP, augmented by our own ANSI compliance, known issues, and data compatibility resources.

**Load this document when assessing any job. It is the canonical reference.**

---

## Customer-Specific Context

| Item | Value |
|------|-------|
| **Cloud** | Azure Databricks |
| **CI/CD** | Azure DevOps with PowerShell deployment scripts |
| **Catalog pattern** | `USE CATALOG {{env}}_catalog` with 2-part table names -- this is VALID UC usage. Do NOT flag. |
| **External paths** | `abfss://` paths are registered in Unity Catalog. Do NOT flag as non-UC. Do NOT suggest Volumes as replacement. |
| **Requirements** | Centralized at `/Volumes/{{env}}_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt` |
| **Job parameters** | `PATH_LANDING` and `PATH_DATALAKE` with `%env_name%` substitution in ADLS URIs |
| **Environment** | Client version `"4"`, Python 3.12 |
| **Migration paths** | A (Scala to Scala 16.4), B (Scala to PySpark Serverless), C (PySpark to Serverless), D (SQL to DBSQL) |
| **Feature policy** | GA features only -- do NOT recommend Public Preview features |

### Design Principles

1. **NEVER recommend `spark.sql.ansi.enabled = false` or `SET ANSI_MODE = false`.** The SOP document recommends this for datatype mismatch issues. We DISAGREE. ANSI mode cannot be disabled on serverless. Disabling it masks data quality issues in healthcare data. The correct approach is to fix code with ANSI-safe functions (TRY_CAST, TRY_DIVIDE, IS TRUE, try_to_timestamp, etc.). See resources/07-ansi-compliance-reference.md.
2. **Do NOT flag `abfss://` paths as non-Unity Catalog access.** Per customer feedback: "All of our external paths in ADLS are already registered in Unity Catalog, so this recommendation is not applicable. The suggestion to use a Volume is incorrect and can be disregarded."
3. **Every output table gets full data validation.** Healthcare data -- silent changes are unacceptable.
4. **Additive CI/CD changes only.** Do not remove existing pipeline variables; only add new ones.

---

## 1. Assessment Overview

**Purpose:** Evaluate a single Databricks job and produce a migration readiness report with findings, severity, evidence, and recommendations.

**Input:**
- Job ID (or notebook path)
- Prescribed migration path (A, B, C, or D)

**Output:**
- Structured assessment report (see Section 7)
- Eligibility determination: PASS, FAIL, or PASS WITH CHANGES
- Recommended migration path (may differ from prescribed, with justification)
- Effort estimate: LOW, MEDIUM, or HIGH

**Assessment phases:**

| Phase | What | How |
|-------|------|-----|
| 1 | Job Classification | Jobs API -- pull config, classify language/compute/streaming/GPU |
| 2 | Eligibility Screening | Hard blockers and high-effort checks against job config and notebook code |
| 3 | Code-Level Audit | Regex scan of every notebook for ANSI, unsupported ops, configs, dependencies, performance |
| 4 | Repo-Side Assessment | Azure DevOps job JSON, PowerShell script, variable group checks |
| 5 | Data Compatibility | Per-table protocol, properties, datetime, boolean, file layout checks |

---

## 2. Phase 1: Job Classification (Automated via Jobs API)

Pull the job configuration using the Databricks Jobs API (`GET /api/2.1/jobs/get?job_id=<id>`).

**Extract these fields:**

| Field | API Path | Purpose |
|-------|----------|---------|
| Job name | `settings.name` | Identification |
| DBR version | `settings.job_clusters[*].new_cluster.spark_version` or cluster policy | Current runtime |
| Language | Notebook headers / `settings.tasks[*].notebook_task` | Classification |
| Cluster spec | `settings.job_clusters[*].new_cluster` | Compute type |
| Init scripts | `settings.job_clusters[*].new_cluster.init_scripts` | Dependency analysis |
| Libraries | `settings.job_clusters[*].new_cluster.libraries` or task libraries | Dependency analysis |
| Spark configs | `settings.job_clusters[*].new_cluster.spark_conf` | Config audit |
| Task definitions | `settings.tasks` | Multi-task structure |
| Notebook paths | `settings.tasks[*].notebook_task.notebook_path` | Code scan targets |
| Schedule | `settings.schedule` | Operational context |
| Tags | `settings.tags` | Metadata |

**Classify the job:**

| Dimension | Values | How to Detect |
|-----------|--------|---------------|
| Language | Python, SQL, Scala, R, Mixed | Notebook header language + `%scala` / `%r` / `%sql` / `%python` magic commands |
| Compute | Classic (job cluster), Classic (all-purpose), Interactive | Job config structure |
| Streaming | Yes / No | `readStream`, `writeStream`, `.trigger(` in code |
| GPU | Yes / No | `cuda`, `cudf`, `cuml`, GPU node types in cluster spec |

**Scala complexity analysis (for Scala notebooks only):**

When a notebook is Scala, scan for these patterns to classify it as Heavy Scala or Light Scala (SQL wrapper):

Heavy Scala indicators (each one found adds to the complexity score):
- `case class` definitions (typed Datasets)
- `.as[CaseClass]` typed Dataset conversions
- `udf(` UDF definitions
- `Option(`, `Some(`, `.getOrElse`, `.orElse` (Option types)
- `match {` or `case Some(` or `case None` (pattern matching)
- `Try {` or `Success(` or `Failure(` (Try/Success/Failure)
- `typedLit(Map(` (typed literal maps)
- `BigDecimal` or `RoundingMode` (precision arithmetic)
- `SimpleDateFormat` (date parsing in UDFs)
- `implicit val` (implicit encoders)
- `.map(`, `.filter(`, `.flatMap(`, `.foreach(` on Scala collections (not DataFrames)
- `import scala.` (Scala-specific imports beyond basic)
- `var ` declarations with type annotations

Light Scala / SQL wrapper indicators:
- Cells are mostly `spark.sql("...")` calls
- Minimal DataFrame operations (just `spark.read` / `df.write`)
- No UDFs
- No case classes
- No pattern matching
- No Option/Try types
- Business logic lives in SQL strings, not Scala code

Classification:
- 0 heavy indicators = **Light Scala (SQL wrapper)** - easy conversion to PySpark or even Path D
- 1-3 heavy indicators = **Moderate Scala** - conversion feasible but needs careful UDF translation
- 4+ heavy indicators = **Heavy Scala** - significant conversion effort, consider Path A unless serverless is required

**Auto-recommend migration path:**

```
IF language = Scala:
    Compute scala_complexity_score (count of heavy indicators above)

    IF scala_complexity_score = 0 (Light Scala / SQL wrapper):
        IF all business logic is in spark.sql() strings:
            RECOMMEND Path D (DBSQL Serverless) -- trivial conversion, just extract SQL
            ALSO OFFER Path B (Scala to PySpark Serverless) -- if they need PySpark features
        ELSE:
            RECOMMEND Path B (Scala to PySpark Serverless) -- light conversion
    ELIF scala_complexity_score <= 3 (Moderate Scala):
        RECOMMEND Path B (Scala to PySpark Serverless) -- feasible conversion
        ALSO OFFER Path A (Scala 16.4) -- if conversion effort is a concern
    ELSE (Heavy Scala, 4+ indicators):
        RECOMMEND Path A (Scala 16.4) -- stay Scala, upgrade DBR only
        ALSO OFFER Path B -- but flag as HIGH effort with specific complexity details

ELIF language IN (Python, SQL, Mixed Python/SQL):
    IF all_cells_are_sql AND no_pyspark_logic:
        RECOMMEND Path D (DBSQL Serverless)
    ELSE:
        RECOMMEND Path C (PySpark/SQL Serverless)

ELIF language = R:
    RECOMMEND: INELIGIBLE -- R not supported on serverless. Stay on classic.
```

When recommending, include the complexity analysis:
```
SCALA COMPLEXITY ANALYSIS:
  Heavy indicators found: <count>
    - case class definitions: <count> (e.g., ClaimRecord, EnrichedClaim)
    - UDF definitions: <count>
    - Option/Some/None usage: <count> occurrences
    - Pattern matching: <count> match blocks
    - BigDecimal/RoundingMode: <count> occurrences
    - SimpleDateFormat in UDFs: <count>
    - Typed Dataset .as[T]: <count>
  
  Light indicators:
    - spark.sql() calls: <count>
    - DataFrame read/write only: <yes/no>
    - SQL magic cells (%sql): <count>

  Classification: <Light / Moderate / Heavy> Scala
  
  RECOMMENDED PATH: <Path A / B / D> -- <justification>
  ALTERNATIVE PATH: <Path X> -- <when this would be better>
```

If the recommended path differs from the prescribed path, include justification in the report.

---

## 3. Phase 2: Eligibility Screening

Run these checks against the job configuration AND notebook code. Each check has an ID, severity, detection method, rationale, and recommended fix.

### Hard Blockers (job CANNOT run on serverless)

If ANY hard blocker is present, the job is ineligible for serverless general compute.

---

#### Check 1: Scala or R Notebooks

- **Severity:** Hard Blocker
- **Detection:**
  ```regex
  # Notebook header language
  (?m)^//\s*Databricks\s+notebook\s+source
  (?m)^#\s*Databricks\s+notebook\s+source.*\bR\b

  # Magic commands in Python/SQL notebooks
  (?m)^%scala\b
  (?m)^%r\b
  ```
  Also check job API: if any task references a Scala or R notebook.
- **Why:** Serverless general compute supports Python and SQL only. Scala and R are not supported.
- **Fix:**
  - Scala: Path A (stay on classic 16.4) or Path B (convert to PySpark then serverless)
  - R: Stay on classic. No serverless path exists.
- **SOP reference:** Section 2 (Language Requirements)

---

#### Check 2: GPU Workloads

- **Severity:** Hard Blocker
- **Detection:**
  ```regex
  (?i)\.cuda\s*\(
  (?i)torch\.device\s*\(\s*["']cuda
  (?i)tensorflow.*GPU
  (?i)with\s+tf\.device.*GPU
  (?i)\bcudf\b
  (?i)\bcuml\b
  (?i)\bcupyx?\b
  ```
  Also check cluster spec for GPU node types (e.g., `Standard_NC` series on Azure).
- **Why:** Serverless does not provide GPU instances.
- **Fix:** Stay on classic compute with ML Runtime.
- **SOP reference:** Section 2 (Compute Requirements)

---

#### Check 3: Structured Streaming with Complex Stateful Operations

- **Severity:** Hard Blocker
- **Detection:**
  ```regex
  (?i)\.readStream\b
  (?i)\.writeStream\b
  (?i)\.trigger\s*\(
  (?i)spark\.readStream\b
  (?i)\.groupByKey\s*\(.*?\.flatMapGroupsWithState
  (?i)\.mapGroupsWithState\s*\(
  (?i)streaming\.stateStore
  ```
- **Why:** Structured streaming with complex stateful operations is not supported on serverless general compute. Simple streaming with `AvailableNow` trigger may work, but complex stateful patterns (flatMapGroupsWithState, mapGroupsWithState) do not.
- **Fix:** Stay on classic compute, or use Lakeflow Declarative Pipelines (DLT).
- **SOP reference:** Section 2 (Streaming Limitations)

---

#### Check 4: Non-Unity Catalog Data Access (Legacy Hive Metastore ONLY)

- **Severity:** Hard Blocker
- **Detection:**
  ```regex
  # Legacy Hive metastore references
  (?i)hive_metastore\.
  (?i)spark\.catalog\.setCurrentDatabase\s*\((?!.*_catalog)
  (?i)USE\s+(?!CATALOG\b)(?!SCHEMA\b)\w+\s*$

  # Instance profile credential passthrough
  (?i)spark\.databricks\.passthrough\.enabled
  (?i)fs\.azure\.account\.key\.
  (?i)fs\.azure\.account\.oauth2\.
  ```
  **IMPORTANT:** Do NOT flag the following as non-UC:
  - `abfss://` paths -- the customer's ADLS paths are registered in Unity Catalog
  - `USE CATALOG {{env}}_catalog` -- this is correct UC usage
  - 2-part table names after `USE CATALOG` -- this is correct UC usage
- **Why:** Serverless requires Unity Catalog. Legacy Hive metastore-only tables cannot be accessed.
- **Fix:** Migrate tables to Unity Catalog first.
- **SOP reference:** Section 3 (Unity Catalog Requirement)

---

#### Check 5: Global Temp Views

- **Severity:** Hard Blocker
- **Detection:**
  ```regex
  (?i)createOrReplaceGlobalTempView\s*\(
  (?i)createGlobalTempView\s*\(
  (?i)\bglobal_temp\.
  ```
- **Why:** Global temp views are not supported on serverless. They rely on a shared SparkContext across sessions, which does not exist on serverless.
- **Fix:** Convert to session-scoped temp views (`createOrReplaceTempView`) or persist to a table/materialized view.
- **SOP reference:** Section 4 (Unsupported Operations)

---

### High Effort (requires significant refactoring)

---

#### Check 6: JAR Libraries in Notebooks

- **Severity:** High Effort
- **Detection:**
  ```regex
  (?i)%jar\b
  (?i)\.jar\b
  (?i)maven.*coordinates
  (?i)com\.crealytics\.spark\.excel
  ```
  Also check job API: `settings.job_clusters[*].new_cluster.libraries` for JAR entries.
- **Why:** Serverless general compute does not support arbitrary JAR libraries from notebooks. JARs must be in a JAR task or rewritten in Python.
- **Fix:** Rewrite JAR-dependent logic in PySpark. For `com.crealytics.spark.excel`, use the pandas + openpyxl pattern (see resources/06-package-dependency-analysis.md).
- **SOP reference:** Section 6 (Library Migration)

---

#### Check 7: RDD APIs

- **Severity:** High Effort
- **Detection:**
  ```regex
  sc\.textFile\s*\(
  sc\.parallelize\s*\(
  sc\.wholeTextFiles\s*\(
  \.rdd\.
  rdd\.map\s*\(
  rdd\.filter\s*\(
  rdd\.flatMap\s*\(
  rdd\.reduce\s*\(
  rdd\.collect\s*\(
  ```
- **Why:** RDD APIs are not available on serverless (Spark Connect). All data operations must use DataFrame/Dataset APIs.
- **Fix:** Rewrite as DataFrame operations. `sc.parallelize()` becomes `spark.createDataFrame()`. `rdd.map()` becomes `withColumn()` or `select()` with UDFs.
- **SOP reference:** Section 4 (Unsupported APIs)

---

#### Check 8: DBFS Mounts and Instance Profiles

- **Severity:** High Effort
- **Detection:**
  ```regex
  (?i)/dbfs/
  (?i)/mnt/
  (?i)dbutils\.fs\.mount\s*\(
  (?i)spark\.databricks\.passthrough\.enabled
  ```
  **IMPORTANT:** Do NOT flag `abfss://` paths. The customer's ADLS paths are UC-registered.
- **Why:** DBFS mounts and instance profile passthrough are not available on serverless. Access must go through Unity Catalog external locations.
- **Fix:** Replace `/dbfs/` and `/mnt/` paths with UC-registered `abfss://` paths or Volume paths.
- **SOP reference:** Section 3 (Data Access)

---

#### Check 9: Streaming Triggers Other Than AvailableNow

- **Severity:** High Effort (if streaming is eligible at all)
- **Detection:**
  ```regex
  (?i)\.trigger\s*\(\s*(?!.*availableNow)
  (?i)\.trigger\s*\(\s*processingTime
  (?i)\.trigger\s*\(\s*continuous
  (?i)\.trigger\s*\(\s*once
  ```
- **Why:** If a streaming job is eligible for serverless (simple stateless), only `AvailableNow` trigger is recommended. `ProcessingTime`, `Continuous`, and `Once` triggers have different serverless behavior.
- **Fix:** Evaluate whether `AvailableNow` is appropriate. If not, stay on classic.
- **SOP reference:** Section 2 (Streaming)

---

## 4. Phase 3: Code-Level Audit

For EVERY notebook referenced by the job, scan the source code for the patterns below. Report matches with notebook path, cell number (if available), line number, matched text, and recommended fix.

---

### ANSI Compliance (CRITICAL -- serverless enforces ANSI mode, cannot be disabled)

**Reference:** resources/07-ansi-compliance-reference.md for full fix patterns with code examples.

---

#### Check 10: CAST to Numeric/Date/Timestamp

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL
  (?i)\bCAST\s*\(\s*\S+\s+AS\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\s*\)

  # PySpark
  \.cast\s*\(\s*["'](?:int|integer|bigint|smallint|tinyint|float|double|decimal|date|timestamp|boolean)["']\s*\)
  \.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*\(\s*\)\s*\)
  ```
- **Why:** Under ANSI mode, `CAST` throws `NumberFormatException`, `DateTimeException`, or `ArithmeticException` on invalid input instead of returning NULL.
- **Fix:** Replace with `TRY_CAST` (SQL) or `F.expr("TRY_CAST(col AS TYPE)")` (PySpark). Add data quality checks for healthcare columns after conversion.
- **SOP reference:** Section 7 (ANSI Mode)
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 1

---

#### Check 11: Division by Zero

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL -- any division
  (?i)\b\w+\s*/\s*\w+

  # PySpark/Scala -- column division
  F\.col\([^)]+\)\s*/\s*F\.col
  \.divide\s*\(
  ```
- **Why:** Division by zero throws `ArithmeticException` under ANSI mode instead of returning NULL.
- **Fix:** `TRY_DIVIDE(a, b)` (SQL), or `F.when(F.col("b") != 0, F.col("a") / F.col("b")).otherwise(F.lit(None))` (PySpark).
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 2

---

#### Check 12: Remainder / Modulo by Zero

- **Severity:** Critical
- **Detection:**
  ```regex
  (?i)\b\w+\s*%\s*\w+
  (?i)\bPMOD\s*\(
  ```
- **Why:** Modulo by zero throws `REMAINDER_BY_ZERO`.
- **Fix:** `value % NULLIF(divisor, 0)` or `CASE WHEN divisor = 0 THEN NULL ELSE value % divisor END`.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 10

---

#### Check 13: BOOLEAN Compared to INT

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL
  (?i)\b\w+\s*=\s*1\b(?![\.\d])
  (?i)\b\w+\s*=\s*0\b(?![\.\d])
  (?i)\bWHEN\b.*?=\s*[01]\b

  # PySpark
  F\.col\s*\(\s*["'][^"']+["']\s*\)\s*==\s*[01]\b
  ```
  **IMPORTANT:** Cross-reference with table schema to confirm the column is actually BOOLEAN type before flagging. Not every `= 1` is a boolean comparison.
- **Why:** ANSI mode rejects implicit BOOLEAN-to-INT comparison. `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` error.
- **Fix:** Replace `col = 1` with `col IS TRUE`. Replace `col = 0` with `col IS NOT TRUE` (or `col IS FALSE` if NULLs should not match).
- **Known customer issues:** Issues 2, 4 in resources/13-serverless-known-issues.md (boolean flag columns used in claims data).
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 6

---

#### Check 14: Array Out of Bounds

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL -- array bracket access
  \w+\[\d+\]

  # PySpark
  \.getItem\s*\(\d+\)
  F\.split\s*\([^)]+\)\s*\[\d+\]
  F\.split\s*\([^)]+\)\.getItem\s*\(\d+\)
  F\.element_at\s*\(
  ```
- **Why:** Array index out of bounds throws `ArrayIndexOutOfBoundsException` under ANSI mode.
- **Fix:** `TRY_ELEMENT_AT(array, index)` (1-indexed in SQL) or bounds check with `F.size()`.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 4

---

#### Check 15: Map Key Not Found

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL
  \w+\['[^']+'\]
  \w+\["[^"]+"\]

  # PySpark
  \.getItem\s*\(\s*["']
  ```
- **Why:** Accessing a map with a nonexistent key throws `NoSuchElementException` under ANSI mode.
- **Fix:** `TRY_ELEMENT_AT(map, 'key')` or `map_contains_key` guard.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 5

---

#### Check 16: to_date / to_timestamp on Invalid Data

- **Severity:** Critical
- **Detection:**
  ```regex
  # SQL
  (?i)\bto_date\s*\(
  (?i)\bto_timestamp\s*\(

  # PySpark
  F\.to_date\s*\(
  F\.to_timestamp\s*\(
  ```
- **Why:** `to_date()` and `to_timestamp()` throw `CANNOT_PARSE_TIMESTAMP` or `DateTimeException` on invalid strings under ANSI mode. Customer data contains `'00000000'` and `'2299-12-34'` sentinel values.
- **Fix:** Replace with `try_to_date` / `try_to_timestamp`. In PySpark, use `F.expr("try_to_date(col, 'format')")` (no native PySpark function exists).
- **Known customer issues:** Issues 3, 5 in resources/13-serverless-known-issues.md.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 8

---

#### Check 17: make_timestamp with Invalid Fields

- **Severity:** Critical
- **Detection:**
  ```regex
  (?i)\bmake_timestamp\s*\(
  (?i)\bmake_date\s*\(
  ```
- **Why:** `make_timestamp()` throws `INVALID_FRACTION_OF_SECOND` or `DATETIME_FIELD_OUT_OF_BOUNDS` if any field is invalid.
- **Fix:** Replace with `try_make_timestamp` / `try_make_date` (where available). Validate field values before construction.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 16

---

#### Check 18: SUM on INT Columns (Overflow Risk)

- **Severity:** Critical
- **Detection:**
  ```regex
  (?i)\bSUM\s*\(\s*\w+\s*\)
  (?i)\bSUM\s*\(\s*`[^`]+`\s*\)
  ```
  Cross-reference with schema: only flag if the column type is INT, SMALLINT, or TINYINT (not already BIGINT).
- **Why:** `SUM()` on integer columns can overflow and throw `ARITHMETIC_OVERFLOW` under ANSI mode.
- **Fix:** `SUM(CAST(int_column AS BIGINT))`.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 18

---

#### Check 19: Integer Overflow in Arithmetic

- **Severity:** High
- **Detection:**
  ```regex
  # INT column multiplication
  (?i)\b(?:amount|count|total|quantity|units)\b.*?[*+]
  F\.col\([^)]+\)\s*[*+]\s*F\.col
  ```
  Cross-reference with schema to confirm column types are narrow integers.
- **Why:** Arithmetic overflow throws `ARITHMETIC_OVERFLOW` or `BINARY_ARITHMETIC_OVERFLOW` under ANSI mode.
- **Fix:** Widen to BIGINT before arithmetic: `CAST(col_a AS BIGINT) * col_b`.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 3

---

#### Check 20: f.lit() Wrapping Format Strings in to_date/to_timestamp (Real Customer Bug)

- **Severity:** Critical
- **Detection:**
  ```regex
  F\.to_date\s*\([^,]+,\s*F\.lit\s*\(
  F\.to_timestamp\s*\([^,]+,\s*F\.lit\s*\(
  f\.to_date\s*\([^,]+,\s*f\.lit\s*\(
  f\.to_timestamp\s*\([^,]+,\s*f\.lit\s*\(
  F\.try_to_timestamp\s*\([^,]+,\s*F\.lit\s*\(
  f\.try_to_timestamp\s*\([^,]+,\s*f\.lit\s*\(
  ```
- **Why:** Format strings must be plain Python strings, not Column expressions. `f.lit("yyyyMMdd")` wraps the string in a Column, causing `UNRESOLVED_COLUMN.WITH_SUGGESTION` error on serverless. Classic compute may have tolerated this.
- **Fix:** Remove the `F.lit()` wrapper. Change `F.to_date(col, F.lit("yyyyMMdd"))` to `F.to_date(col, "yyyyMMdd")`.
- **Known customer issue:** Issue 7 in resources/13-serverless-known-issues.md.

---

#### Check 21: Implicit String-to-Numeric Conversions

- **Severity:** High
- **Detection:**
  ```regex
  # Hard to detect statically -- look for mixed-type operations
  # Cross-reference with schema: string columns used in arithmetic or joins with numeric columns
  (?i)(?:string_col|varchar_col)\s*[+\-*/]\s*\d+
  ```
  Also: review all JOIN conditions where column types differ between left and right sides. Look for STRING-to-INT implicit casts.
- **Why:** Implicit string-to-number conversion throws `CAST_INVALID_INPUT` if the string is not a valid number. Real customer issue: `'ENG'` cast to INT in a join condition (Issue 6).
- **Fix:** Use explicit `TRY_CAST` in join conditions and arithmetic.
- **Known customer issue:** Issue 6 in resources/13-serverless-known-issues.md.

---

#### Check 22: parse_url on Potentially Invalid URLs

- **Severity:** Medium
- **Detection:**
  ```regex
  (?i)\bparse_url\s*\(
  (?i)\burl_decode\s*\(
  ```
- **Why:** `parse_url()` throws `INVALID_URL` on malformed URLs under ANSI mode.
- **Fix:** Replace with `try_parse_url`.
- **Detailed reference:** resources/07-ansi-compliance-reference.md, Pattern 17

---

### Unsupported Operations (CRITICAL on serverless)

---

#### Check 23: .persist() / .cache() / CACHE TABLE / UNCACHE TABLE

- **Severity:** High
- **Detection:**
  ```regex
  \.persist\s*\(
  \.cache\s*\(
  \.unpersist\s*\(
  (?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b
  (?i)\bUNCACHE\s+TABLE\b
  ```
- **Why:** Serverless manages memory and caching automatically. `.persist()` and `.cache()` are not supported (Spark Connect limitation).
- **Fix:** Remove. If the DataFrame is used multiple times and performance is critical, materialize to a temp table.
- **Known customer issue:** Issue 16 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 4 (Unsupported Operations)

---

#### Check 24: REFRESH TABLE

- **Severity:** High
- **Detection:**
  ```regex
  (?i)\bREFRESH\s+TABLE\b
  ```
- **Why:** `REFRESH TABLE` is explicitly not supported on serverless. Throws `NOT_SUPPORTED_WITH_SERVERLESS`.
- **Fix:** Remove. Serverless auto-refreshes metadata cache.
- **Known customer issue:** Issue 17 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 4

---

#### Check 25: MSCK REPAIR TABLE

- **Severity:** High
- **Detection:**
  ```regex
  (?i)\bMSCK\s+REPAIR\s+TABLE\b
  ```
- **Why:** Hive-style partition repair is not supported on serverless.
- **Fix:** Remove. Use `ALTER TABLE ... ADD PARTITION` if explicit partition discovery is needed.
- **Known customer issue:** Issue 18 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 4

---

#### Check 26: CREATE MATERIALIZED VIEW / REFRESH MATERIALIZED VIEW

- **Severity:** High
- **Detection:**
  ```regex
  (?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
  (?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b
  ```
- **Why:** Materialized views cannot be created or refreshed from serverless general compute. They require a SQL Warehouse.
- **Fix:** Route this job to a Serverless SQL Warehouse. Grant Service Principal access to the warehouse.
- **Known customer issue:** Issue 19 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 4

---

#### Check 27: Temp View Reuse in Loops (Spark Connect Lazy Evaluation)

- **Severity:** Medium
- **Detection:**
  ```regex
  # Look for temp view creation inside loops
  (?i)createOrReplaceTempView\s*\(
  # AND one of these loop patterns nearby:
  (?i)\bfor\s+\w+\s+in\b
  (?i)\bwhile\b
  ```
  Flag if a temp view is created inside a loop body AND the same view name is referenced by subsequent Spark actions within the same loop iteration.
- **Why:** On serverless (Spark Connect), lazy evaluation and session state differ from classic. Temp views created in tight loops may not be immediately visible to subsequent queries in the same iteration.
- **Fix:** Use explicit `spark.sql()` calls or materialize to tables. Avoid tight loop patterns with temp views.
- **SOP reference:** Section 4

---

### Spark Configuration Issues

**Reference:** resources/05-spark-config-classic-to-serverless.md for the complete config migration matrix.

---

#### Check 28: Unsupported Spark Configs (SOP Section 7)

- **Severity:** Critical (will throw CONFIG_NOT_AVAILABLE)
- **Detection:**
  ```regex
  # Scan for spark.conf.set and SQL SET statements
  spark\.conf\.set\s*\(
  (?i)^\s*SET\s+spark\.
  (?i)^\s*SET\s+\"spark\.
  ```
  Then match the config key against this unsupported list:

  | Config | Action | Replacement |
  |--------|--------|-------------|
  | `spark.sql.storeAssignmentPolicy` | Remove | Fix code with ANSI-safe patterns. Do NOT replace with `ansi.enabled=false`. |
  | `spark.network.timeout` | Remove | `spark.databricks.execution.timeout` (if truly needed) |
  | `spark.sql.parquet.int96RebaseModeInRead` | Evaluate | Can set to `LEGACY` if needed for old parquet files |
  | `spark.sql.parquet.int96RebaseModeInWrite` | Evaluate | Can set to `LEGACY` if needed |
  | `spark.sql.parquet.datetimeRebaseModeInRead` | Evaluate | Can set to `LEGACY` for old Spark 2.x files |
  | `spark.sql.parquet.datetimeRebaseModeInWrite` | Evaluate | Can set to `LEGACY` if needed |
  | `spark.databricks.safespark.externalUDF.plan.limit` | Remove | Not available on serverless |
  | `spark.driver.extraJavaOptions` | Remove | No JVM option access on serverless |
  | `spark.databricks.delta.retentionDurationCheck.enabled` | Remove | Use table property: `ALTER TABLE SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '...')` |
  | `spark.databricks.delta.schema.autoMerge.enabled` | Remove | Use `MERGE WITH SCHEMA EVOLUTION` SQL syntax |
  | `spark.databricks.delta.optimizeWrite.enabled` | Remove | Default ON on serverless |
  | `spark.databricks.delta.autoCompact.enabled` | Remove | Default ON on serverless |
  | `spark.sql.broadcastTimeout` | Remove | Serverless manages broadcast internally |
  | `spark.sql.caseSensitive` | Remove | Not supported. Rewrite code to not depend on case sensitivity. |
  | `spark.sql.streaming.stateStore.stateSchemaCheck` | Remove | Not available. Only remove if job is not actually streaming. |
  | `spark.executor.*` | Remove | Serverless manages executor resources |
  | `spark.driver.memory` / `spark.driver.cores` | Remove | Serverless manages driver resources |
  | `spark.dynamicAllocation.*` | Remove | Serverless has its own auto-scaling |
  | `spark.shuffle.service.*` | Remove | External shuffle service not applicable |
  | `spark.serializer` | Remove | Serverless uses its own serialization |
  | `spark.sql.warehouse.dir` | Remove | Managed by Unity Catalog |
  | `spark.hadoop.*` | Remove | Hadoop configs not applicable |
  | `fs.azure.*` | Remove | Use Unity Catalog external locations |
  | `spark.databricks.cluster.*` | Remove | Not applicable on serverless |
  | `spark.databricks.passthrough.enabled` | Remove | Use Unity Catalog permissions |

- **Known customer issues:** Issues 8-15 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 7 (Spark Configuration)
- **Detailed reference:** resources/05-spark-config-classic-to-serverless.md

---

#### Check 29: spark.sql.ansi.enabled Set to False

- **Severity:** Critical
- **Detection:**
  ```regex
  (?i)spark\.sql\.ansi\.enabled.*false
  (?i)SET\s+spark\.sql\.ansi\.enabled\s*=\s*false
  (?i)SET\s+ANSI_MODE\s*=\s*false
  ```
- **Why:** This config is a no-op on serverless -- ANSI mode is always ON and cannot be disabled. Code that relies on ANSI being off will fail. The SOP recommends setting this to false as a workaround; we DISAGREE. The correct fix is ANSI-safe code.
- **Fix:** Remove the config setting entirely. Fix all ANSI-unsafe code patterns (Checks 10-22). Never recommend disabling ANSI mode.
- **SOP reference:** Section 7 (we override SOP on this point)

---

### Environment and Dependencies

---

#### Check 30: os.environ.get() / os.environ[]

- **Severity:** High
- **Detection:**
  ```regex
  os\.environ\.get\s*\(
  os\.environ\[
  os\.getenv\s*\(
  sys\.argv
  ```
  Also check for custom `spark.conf.get()` / `spark.conf.set()` calls with non-standard keys (not `spark.*` or `delta.*`):
  ```regex
  spark\.conf\.(?:get|set)\s*\(\s*["'](?!spark\.|delta\.)
  ```
- **Why:** Environment variables set at the cluster level are not available on serverless. Custom spark.conf keys used as env vars (e.g., `spark.conf.get("yrmo_latest.date")`) also fail with `CONFIG_NOT_AVAILABLE`.
- **Fix:** Use `dbutils.widgets.get("param_name")` with job-level parameters. For parent-to-child notebook communication, use `dbutils.widgets.text()` in the parent and `dbutils.widgets.get()` in the child.
- **Known customer issue:** Issue 15 in resources/13-serverless-known-issues.md.
- **SOP reference:** Section 5 (Environment Variables)

---

#### Check 31: Init Script Dependencies

- **Severity:** Medium
- **Detection:**
  ```regex
  init_scripts
  dbfs:/.*\.sh
  /Volumes/.*\.sh
  ```
  Also check job API: `settings.job_clusters[*].new_cluster.init_scripts`.
- **Why:** Init scripts are not available on serverless. All dependencies must be in requirements.txt.
- **Fix:** Move all `pip install` commands from init scripts to the centralized requirements.txt at `/Volumes/{{env}}_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`.
- **SOP reference:** Section 6

---

#### Check 32: %pip install in Notebooks

- **Severity:** Medium
- **Detection:**
  ```regex
  (?m)^%pip\s+install\b
  ```
- **Why:** While `%pip install` works on serverless, it executes at runtime and adds startup latency. The centralized requirements.txt is preferred for consistent dependency management across environments.
- **Fix:** Move dependencies to the centralized requirements.txt. Pin all versions.
- **SOP reference:** Section 6

---

#### Check 33: dbutils.library.install

- **Severity:** High
- **Detection:**
  ```regex
  dbutils\.library\.install\s*\(
  dbutils\.library\.restartPython\s*\(
  ```
- **Why:** `dbutils.library.install` is deprecated and fails on serverless.
- **Fix:** Move to requirements.txt.
- **SOP reference:** Section 6

---

#### Check 34: Unpinned Python Dependencies

- **Severity:** Low
- **Detection:**
  ```regex
  # In requirements.txt or %pip install lines:
  # Look for packages without version pins
  (?m)^(?:pip\s+install\s+)?([a-zA-Z0-9_-]+)\s*$
  ```
- **Why:** Unpinned dependencies may resolve to different versions across environments, causing test/prod divergence.
- **Fix:** Pin all versions: `package==1.2.3`.
- **SOP reference:** Section 6

---

#### Check 35: com.crealytics.spark.excel

- **Severity:** High
- **Detection:**
  ```regex
  com\.crealytics\.spark\.excel
  \.format\s*\(\s*["']com\.crealytics\.spark\.excel["']\s*\)
  ```
- **Why:** JAR-based data sources are not available on serverless. `DATA_SOURCE_NOT_FOUND` error.
- **Fix:** Replace with pandas + openpyxl pattern. For reading: load via `spark.read.format("binaryFile")`, write bytes to `/local_disk0/tmp/`, read with `pd.read_excel()`, convert to Spark DataFrame. For writing: convert to pandas, write with `openpyxl`, copy from `/local_disk0/tmp/` to target path via `dbutils.fs.cp`.
- **Known customer issues:** Issues 21-22 in resources/13-serverless-known-issues.md.
- **Detailed reference:** resources/06-package-dependency-analysis.md

---

#### Check 36: Incompatible Wheel Files

- **Severity:** Medium
- **Detection:**
  ```regex
  \.whl\b
  ```
  Check wheel filenames for Python version compatibility. Must contain `cp312` for Python 3.12.
- **Why:** Serverless runs Python 3.12 on DBR 16.4. Wheels built for older Python versions will fail to install.
- **Fix:** Rebuild wheels on DBR 16.4 / Python 3.12. Verify filename contains `cp312`.
- **Known customer issue:** Issue 24 in resources/13-serverless-known-issues.md.

---

### Performance Patterns

---

#### Check 37: .count() > 0 for Existence Checks

- **Severity:** Low
- **Detection:**
  ```regex
  \.count\s*\(\s*\)\s*>\s*0
  \.count\s*\(\s*\)\s*==\s*0
  \.count\s*\(\s*\)\s*!=\s*0
  if\s+.*\.count\s*\(\s*\)
  ```
- **Why:** `.count()` triggers a full table scan. On serverless (no persistent cluster cache), this is especially expensive.
- **Fix:** Replace with `.first() is not None` (existence check) or `df.isEmpty` (DBR 14+).
- **Known customer issue:** Issue 32 in resources/13-serverless-known-issues.md.

---

#### Check 38: ThreadPoolExecutor / concurrent.futures

- **Severity:** Medium
- **Detection:**
  ```regex
  concurrent\.futures
  ThreadPoolExecutor
  multiprocessing\.Pool
  multiprocessing\.Process
  ```
- **Why:** Serverless manages parallelism internally. External threading adds overhead and contention, causing performance degradation (runtime can increase 2-4x). This is documented in the SOP as an anti-pattern.
- **Fix:** For batch operations, use Databricks Workflows with for-each tasks instead of in-notebook threading. Note: some workloads may still be slower on serverless; benchmark before committing.
- **Known customer issue:** Issue 20 in resources/13-serverless-known-issues.md (1hr to 4hr with ThreadPoolExecutor).
- **SOP reference:** Section 8 (Performance Patterns)

---

#### Check 39: Chained .withColumn() (>20 in Sequence)

- **Severity:** Medium
- **Detection:**
  ```regex
  \.withColumn\s*\(
  ```
  Count occurrences per cell/block. Flag if >20 consecutive `.withColumn()` calls.
- **Why:** Deep chains cause `RecursionError: maximum recursion depth exceeded` on serverless due to different default recursion limits.
- **Fix:** Replace with a single `.withColumns()` call that applies all transformations at once.
- **Known customer issue:** Issue 34 in resources/13-serverless-known-issues.md.

---

#### Check 40: SELECT * Usage

- **Severity:** Medium
- **Detection:**
  ```regex
  (?i)\bSELECT\s+\*\s+FROM\b
  (?i)\bINSERT\s+(?:INTO|OVERWRITE)\s+.*?\bSELECT\s+\*
  ```
- **Why:** On serverless, `SELECT *` with row filters can cause `MISSING_ATTRIBUTES` errors due to different logical execution plan construction. Also, column ordering may differ.
- **Fix:** Use explicit column lists. Especially critical for `INSERT INTO ... SELECT *` patterns.
- **Known customer issues:** Issues 43-44 in resources/13-serverless-known-issues.md.

---

#### Check 41: /tmp File Paths

- **Severity:** Medium
- **Detection:**
  ```regex
  ["']/tmp/[^"']*["']
  open\s*\(\s*["']/tmp/
  ```
- **Why:** `/tmp/` is not writable on serverless. `PermissionError: Permission denied`.
- **Fix:** Replace with `/local_disk0/tmp/`. Then copy to Volume or `abfss://` path as needed.
- **Known customer issues:** Issues 23, 45 in resources/13-serverless-known-issues.md.

---

#### Check 42: spark.createDataFrame() Without Explicit Schema

- **Severity:** Medium
- **Detection:**
  ```regex
  spark\.createDataFrame\s*\([^,)]+\)(?!\s*,)
  ```
  Specifically flag when the data source is an API response, parsed JSON, or dictionary with nested fields.
- **Why:** Schema inference differs between classic and serverless (Spark Connect). Complex/nested fields may fail with `CANNOT_INFER_TYPE_FOR_FIELD`.
- **Fix:** Provide explicit `StructType` schema as the second argument.
- **Known customer issues:** Issues 26-27 in resources/13-serverless-known-issues.md.

---

### Schema Inference

---

#### Check 43: Complex Nested JSON/API Responses Without Schema

- **Severity:** High
- **Detection:**
  ```regex
  spark\.createDataFrame\s*\(.*?(?:response|json|api|result|data)\b
  ```
  Also look for:
  ```regex
  (?i)requests\.get\s*\(
  (?i)requests\.post\s*\(
  (?i)json\.loads\s*\(
  ```
  followed by `spark.createDataFrame()` without a schema argument.
- **Why:** API responses often have optional/nullable nested fields. Serverless schema inference cannot always determine the type, throwing `CANNOT_INFER_TYPE_FOR_FIELD`.
- **Fix:** Define explicit `StructType` for all fields. For Jobs API responses specifically, predefine the expected schema.
- **Known customer issues:** Issues 26-27 in resources/13-serverless-known-issues.md.

---

### _metadata Column

---

#### Check 44: Code Referencing _metadata Column

- **Severity:** High
- **Detection:**
  ```regex
  (?i)['"]_metadata['"]
  (?i)col\s*\(\s*['"]_metadata['"]
  (?i)\._metadata\b
  (?i)`_metadata`
  (?i)_metadata\s+(?:STRING|STRUCT)
  ```
- **Why:** Tables with row tracking enabled have an internal `_metadata` column that conflicts with user code referencing `_metadata` (common in healthcare data for audit trails and file-level metadata from `spark.read`). Causes `UNRESOLVED_COLUMN` error.
- **Fix:** Check if target tables have row tracking enabled (`delta.enableRowTracking = true`). If so, either disable row tracking on the table or rename the user-facing `_metadata` references in code.
- **Known customer issue:** Issue 28 in resources/13-serverless-known-issues.md.
- **Detailed reference:** resources/17-data-compatibility-checks.md, Section 4a

---

### SQL Warehouse Recommendation (from SOP)

---

#### Check 45: Pure SQL Notebooks (Path D Candidate)

- **Severity:** Informational (cost optimization)
- **Detection:** Check every cell in the notebook:
  - ALL cells are `%sql` or SQL-language cells
  - NO `%python` or PySpark logic
  - NO `%scala` or `%r` cells
  - NO `spark.` calls in any cell
  - NO `dbutils.` calls (except possibly `dbutils.widgets.get()`)
- **Why:** Pure SQL jobs are more cost-effective and performant on Serverless SQL Warehouse than on Serverless General Compute.
- **Fix:** Recommend Path D (DBSQL Serverless) for better performance and cost. Route to SQL Warehouse instead of general compute.
- **Known customer issue:** Issue 38 in resources/13-serverless-known-issues.md (SQL job cost higher on general compute).
- **SOP reference:** Section 9 (SQL Optimization)

---

## 5. Phase 4: Repo-Side Assessment

These checks apply to the Azure DevOps repository, not the Databricks workspace. They are executed by reviewing the job JSON template and CI/CD pipeline configuration.

**Reference:** resources/16-cicd-change-guide.md for full before/after JSON examples and PowerShell script changes.

---

#### Check 46: Job JSON Needs Serverless Transformation

- **Severity:** Required (repo-side change)
- **Detection:** Read the job JSON template for the job. Check for:
  - `"job_clusters"` section present (must be REMOVED)
  - `"job_cluster_key"` in task definitions (must be REMOVED)
  - `"environments"` section absent (must be ADDED)
  - `"environment_key"` absent from tasks (must be ADDED)
  - `"queue"` absent (must be ADDED)
  - `"performance_optimized"` absent (must be ADDED)
  - `"parameters"` absent (must be ADDED for PATH_LANDING, PATH_DATALAKE)
- **Fix:** Apply the serverless JSON template transformation. See resources/16-cicd-change-guide.md, Section 1 for complete before/after examples.

**After transformation, the JSON should have:**
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

---

#### Check 47: %env_name% Replacement in PowerShell Deployment Script

- **Severity:** Required (repo-side change, one-time)
- **Detection:** Read the PowerShell deployment script (e.g., `deploy_workflow_jobs.ps1`). Search for:
  ```regex
  \.Replace\s*\(\s*"%env_name%"
  ```
  If this replacement is MISSING, it must be added.
- **Fix:** Add this line after the existing `.Replace()` calls:
  ```powershell
  $bodyJson = $bodyJson.Replace("%env_name%","$env_name")
  ```
- **Detailed reference:** resources/16-cicd-change-guide.md, Section 2

---

#### Check 48: Variable Group Has env_name Variable

- **Severity:** Required (validation only)
- **Detection:** Verify the Azure DevOps variable group for each environment contains `env_name`:
  - DEV: `env_name = dev`
  - UAT: `env_name = uat`
  - PROD: `env_name = prod`
- **Fix:** This variable should already exist. If not, add it to the variable group.
- **Detailed reference:** resources/16-cicd-change-guide.md, Section 2

---

#### Check 49: Job Parameters (PATH_LANDING, PATH_DATALAKE)

- **Severity:** Required (repo-side change)
- **Detection:** Check the job JSON for a `"parameters"` block. Verify `PATH_LANDING` and `PATH_DATALAKE` are present with correct `%env_name%` substitution patterns.
- **Fix:** Add the parameters block if absent. See Check 46 for the template.
- **Detailed reference:** resources/16-cicd-change-guide.md, Section 1

---

## 6. Phase 5: Data Compatibility (Per Output Table)

**Reference:** resources/17-data-compatibility-checks.md for full SQL queries, Python helpers, and the complete notebook template.

For every output table written by the job, run these checks:

| Check | What | Risk Trigger | Fix |
|-------|------|-------------|-----|
| **Table type** | Managed vs external | External = no Predictive Optimization | Schedule manual OPTIMIZE + ANALYZE |
| **Delta protocol** | Reader/writer version | Writer v1-2 = likely auto-upgrade on new DBR | Record versions; coordinate upgrade timing |
| **Row tracking** | `delta.enableRowTracking` property | Enabled AND code references `_metadata` | Disable row tracking or rename column |
| **Retention** | `delta.deletedFileRetentionDuration` | Very short (< 7 days) | Extend before migration to preserve time travel |
| **Datetime columns** | Invalid date strings in string columns | `'00000000'`, invalid ISO dates, pre-1582 dates | Ensure code uses TRY_CAST/try_to_date |
| **BOOLEAN columns** | BOOLEAN type columns | Cross-reference with code for `= 1` comparisons | Fix comparisons to IS TRUE / IS NOT TRUE |
| **Parquet writers** | Table history for Spark 2.x writers | Spark 2.x writes + no rebase mode | Set `delta.parquet.datetimeRebaseModeInRead = LEGACY` |
| **File layout** | File count and average size | >1000 files < 32MB (especially external tables) | Run OPTIMIZE before migration |
| **Partition scheme** | Partition columns and cardinality | High cardinality or multi-level | Document for Phase 2 optimization |

### Pre-Migration Actions (from data checks)

| Priority | Trigger | Action |
|----------|---------|--------|
| **P0** | External table with >1000 small files | `OPTIMIZE catalog.schema.table` before migration |
| **P0** | Row tracking + _metadata conflict | Disable row tracking or rename column in code |
| **P0** | Invalid date strings in data | Ensure code uses TRY_CAST / try_to_date everywhere |
| **P1** | Spark 2.x parquet files, no rebase mode | `ALTER TABLE SET TBLPROPERTIES ('delta.parquet.datetimeRebaseModeInRead' = 'LEGACY')` |
| **P1** | BOOLEAN columns with INT comparison in code | Fix comparisons in code |
| **P2** | Short retention duration | Extend before migration |
| **P2** | External tables missing statistics | `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` |

---

## 7. Assessment Report Template

Genie Code produces this structured output for every assessed job.

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
    Issues: <comma-separated list of finding categories>
    Actions: <pre-migration actions needed>

  <catalog.schema.table_name>
    ...

===========================================================================
EFFORT ESTIMATE: LOW | MEDIUM | HIGH
  Based on: <total_findings> total findings
            <code_changes> code changes needed
            <repo_changes> repo-side changes needed
            <table_actions> table pre-migration actions
  Estimated: <hours> hours migration effort

  LOW  = 0 hard blockers, <5 code changes, standard repo transformation
  MEDIUM = 0 hard blockers, 5-20 code changes, some refactoring needed
  HIGH = hard blockers OR >20 code changes OR major refactoring (RDD, JAR, streaming)
===========================================================================
```

---

## 8. Cross-References

All related toolkit resources, indexed by number. Genie Code should load the relevant resource when it needs detailed fix patterns, SQL queries, or code examples beyond what is summarized in this document.

| Resource | File | What It Covers | When to Load |
|----------|------|---------------|-------------|
| **01** | `01-scala-13.3-to-scala-16.4.md` | Path A: Scala DBR upgrade patterns | Job is Scala, staying on classic |
| **02** | `02-scala-13.3-to-pyspark-serverless.md` | Path B: Scala-to-PySpark conversion | Job is Scala, converting to PySpark |
| **03** | `03-pyspark-sql-13.3-to-pyspark-sql-serverless.md` | Path C: PySpark/SQL serverless migration | Most common path for customer jobs |
| **04** | `04-sql-to-dbsql-serverless.md` | Path D: SQL to DBSQL Serverless | Pure SQL jobs routed to SQL Warehouse |
| **05** | `05-spark-config-classic-to-serverless.md` | Complete Spark config migration matrix | Check 28 details: which configs to remove, replace, or keep |
| **06** | `06-package-dependency-analysis.md` | Package dependency audit and migration | Checks 31-36: library/dependency issues |
| **07** | `07-ansi-compliance-reference.md` | Complete ANSI mode reference with all fix patterns | Checks 10-22: every ANSI-safe fix with SQL and PySpark code examples |
| **08** | `08-testing-validation-framework.md` | Post-migration output validation | After migration: verify data equivalence |
| **09** | `09-performance-optimization-patterns.md` | Performance tuning for serverless | Checks 37-42: performance anti-patterns |
| **10** | `10-ml-runtime-migration.md` | ML Runtime migration considerations | Jobs using ML libraries or training |
| **11** | `11-archiving-workflow.md` | Job archival process | Jobs that are retired instead of migrated |
| **12** | `12-documentation-links.md` | Official Databricks documentation links | External reference lookup |
| **13** | `13-serverless-known-issues.md` | 48 confirmed customer issues with resolutions | Error lookup: match error messages to known issues |
| **14** | `14-serverless-blockers.md` | Eligibility screening checklist | Phase 2: hard and soft blocker reference |
| **15** | `15-breaking-changes-13-to-16-regex.md` | 33 regex scan patterns by severity | Phase 3: machine-readable scan patterns for code audit |
| **16** | `16-cicd-change-guide.md` | Azure DevOps CI/CD changes | Phase 4: job JSON transformation, PowerShell changes |
| **17** | `17-data-compatibility-checks.md` | Pre-migration table analysis with SQL queries and Python helpers | Phase 5: data-level risk assessment |
| **18** | `18-batch-assessment-workflow.md` | Batch processing workflow for 12-100 jobs | Running assessment at scale across a batch |

---

## Appendix A: Complete Check Index

Quick-reference table of all 49 checks, sorted by ID.

| Check | Name | Phase | Severity | Category |
|-------|------|-------|----------|----------|
| 1 | Scala or R notebooks | 2 | Hard Blocker | Language |
| 2 | GPU workloads | 2 | Hard Blocker | Compute |
| 3 | Structured streaming (complex stateful) | 2 | Hard Blocker | Streaming |
| 4 | Non-UC data access (legacy Hive only) | 2 | Hard Blocker | Data Access |
| 5 | Global temp views | 2 | Hard Blocker | Unsupported |
| 6 | JAR libraries in notebooks | 2 | High Effort | Dependencies |
| 7 | RDD APIs | 2 | High Effort | API |
| 8 | DBFS mounts / instance profiles | 2 | High Effort | Data Access |
| 9 | Streaming triggers (not AvailableNow) | 2 | High Effort | Streaming |
| 10 | CAST to numeric/date/timestamp | 3 | Critical | ANSI |
| 11 | Division by zero | 3 | Critical | ANSI |
| 12 | Remainder / modulo by zero | 3 | Critical | ANSI |
| 13 | BOOLEAN compared to INT | 3 | Critical | ANSI |
| 14 | Array out of bounds | 3 | Critical | ANSI |
| 15 | Map key not found | 3 | Critical | ANSI |
| 16 | to_date / to_timestamp on invalid data | 3 | Critical | ANSI |
| 17 | make_timestamp with invalid fields | 3 | Critical | ANSI |
| 18 | SUM on INT columns (overflow) | 3 | Critical | ANSI |
| 19 | Integer overflow in arithmetic | 3 | High | ANSI |
| 20 | f.lit() wrapping format strings | 3 | Critical | ANSI (customer bug) |
| 21 | Implicit string-to-numeric conversions | 3 | High | ANSI |
| 22 | parse_url on invalid URLs | 3 | Medium | ANSI |
| 23 | .persist() / .cache() / CACHE TABLE | 3 | High | Unsupported Ops |
| 24 | REFRESH TABLE | 3 | High | Unsupported Ops |
| 25 | MSCK REPAIR TABLE | 3 | High | Unsupported Ops |
| 26 | CREATE/REFRESH MATERIALIZED VIEW | 3 | High | Unsupported Ops |
| 27 | Temp view reuse in loops | 3 | Medium | Unsupported Ops |
| 28 | Unsupported Spark configs | 3 | Critical | Config |
| 29 | spark.sql.ansi.enabled = false | 3 | Critical | Config |
| 30 | os.environ / sys.argv | 3 | High | Environment |
| 31 | Init script dependencies | 3 | Medium | Dependencies |
| 32 | %pip install in notebooks | 3 | Medium | Dependencies |
| 33 | dbutils.library.install | 3 | High | Dependencies |
| 34 | Unpinned Python dependencies | 3 | Low | Dependencies |
| 35 | com.crealytics.spark.excel | 3 | High | Dependencies |
| 36 | Incompatible wheel files | 3 | Medium | Dependencies |
| 37 | .count() > 0 for existence checks | 3 | Low | Performance |
| 38 | ThreadPoolExecutor / concurrent.futures | 3 | Medium | Performance |
| 39 | Chained .withColumn() (>20) | 3 | Medium | Performance |
| 40 | SELECT * usage | 3 | Medium | Performance |
| 41 | /tmp file paths | 3 | Medium | File Path |
| 42 | spark.createDataFrame() without schema | 3 | Medium | Schema |
| 43 | Complex nested JSON without schema | 3 | High | Schema |
| 44 | _metadata column conflict | 3 | High | Metadata |
| 45 | Pure SQL notebook (Path D candidate) | 3 | Info | Optimization |
| 46 | Job JSON serverless transformation | 4 | Required | Repo |
| 47 | PowerShell %env_name% replacement | 4 | Required | Repo |
| 48 | Variable group env_name | 4 | Required | Repo |
| 49 | Job parameters (PATH_LANDING, PATH_DATALAKE) | 4 | Required | Repo |

---

## Appendix B: Regex Quick Reference (All Checks)

Machine-readable flat list for automated scanning. Run all patterns against every notebook.

```json
{
  "assessment_checks": [
    {
      "id": 1,
      "name": "Scala or R notebooks",
      "severity": "HARD_BLOCKER",
      "regex": ["(?m)^//\\s*Databricks\\s+notebook\\s+source", "(?m)^%scala\\b", "(?m)^%r\\b"],
      "fix": "Path A (Scala 16.4) or Path B (Scala to PySpark)"
    },
    {
      "id": 2,
      "name": "GPU workloads",
      "severity": "HARD_BLOCKER",
      "regex": ["(?i)\\.cuda\\s*\\(", "(?i)torch\\.device\\s*\\(\\s*[\"']cuda", "(?i)\\bcudf\\b", "(?i)\\bcuml\\b"],
      "fix": "Stay on classic compute with ML Runtime"
    },
    {
      "id": 3,
      "name": "Structured streaming",
      "severity": "HARD_BLOCKER",
      "regex": ["(?i)\\.readStream\\b", "(?i)\\.writeStream\\b", "(?i)\\.trigger\\s*\\(", "(?i)\\.flatMapGroupsWithState", "(?i)\\.mapGroupsWithState"],
      "fix": "Stay on classic or use DLT"
    },
    {
      "id": 4,
      "name": "Non-UC data access",
      "severity": "HARD_BLOCKER",
      "regex": ["(?i)hive_metastore\\.", "(?i)spark\\.databricks\\.passthrough\\.enabled", "(?i)fs\\.azure\\.account\\.key\\."],
      "fix": "Migrate tables to Unity Catalog first"
    },
    {
      "id": 5,
      "name": "Global temp views",
      "severity": "HARD_BLOCKER",
      "regex": ["(?i)createOrReplaceGlobalTempView\\s*\\(", "(?i)createGlobalTempView\\s*\\(", "(?i)\\bglobal_temp\\."],
      "fix": "Convert to session-scoped temp views or tables"
    },
    {
      "id": 6,
      "name": "JAR libraries",
      "severity": "HIGH_EFFORT",
      "regex": ["(?i)%jar\\b", "(?i)\\.jar\\b", "(?i)com\\.crealytics\\.spark\\.excel"],
      "fix": "Rewrite in Python or use JAR task"
    },
    {
      "id": 7,
      "name": "RDD APIs",
      "severity": "HIGH_EFFORT",
      "regex": ["sc\\.textFile\\s*\\(", "sc\\.parallelize\\s*\\(", "\\.rdd\\.", "rdd\\.map\\s*\\(", "rdd\\.filter\\s*\\(", "rdd\\.flatMap\\s*\\("],
      "fix": "Rewrite as DataFrame operations"
    },
    {
      "id": 8,
      "name": "DBFS mounts",
      "severity": "HIGH_EFFORT",
      "regex": ["(?i)/dbfs/", "(?i)/mnt/", "(?i)dbutils\\.fs\\.mount\\s*\\("],
      "fix": "Replace with UC-registered abfss:// paths or Volumes"
    },
    {
      "id": 9,
      "name": "Non-AvailableNow triggers",
      "severity": "HIGH_EFFORT",
      "regex": ["(?i)\\.trigger\\s*\\(\\s*(?!.*availableNow)", "(?i)\\.trigger\\s*\\(\\s*processingTime", "(?i)\\.trigger\\s*\\(\\s*continuous"],
      "fix": "Evaluate AvailableNow or stay on classic"
    },
    {
      "id": 10,
      "name": "CAST to numeric/date/timestamp",
      "severity": "CRITICAL",
      "regex": ["(?i)\\bCAST\\s*\\(\\s*\\S+\\s+AS\\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\\s*\\)", "\\.cast\\s*\\(\\s*[\"'](?:int|integer|bigint|smallint|tinyint|float|double|decimal|date|timestamp|boolean)[\"']\\s*\\)"],
      "fix": "TRY_CAST or F.expr('TRY_CAST(col AS TYPE)')",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 11,
      "name": "Division by zero",
      "severity": "CRITICAL",
      "regex": ["F\\.col\\([^)]+\\)\\s*/\\s*F\\.col", "\\.divide\\s*\\("],
      "fix": "TRY_DIVIDE(a, b) or null guard",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 12,
      "name": "Remainder / modulo by zero",
      "severity": "CRITICAL",
      "regex": ["(?i)\\b\\w+\\s*%\\s*\\w+", "(?i)\\bPMOD\\s*\\("],
      "fix": "value % NULLIF(divisor, 0)",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 13,
      "name": "BOOLEAN = INT comparison",
      "severity": "CRITICAL",
      "regex": ["(?i)\\b\\w+\\s*=\\s*1\\b(?![\\d\\.])", "(?i)\\b\\w+\\s*=\\s*0\\b(?![\\d\\.])"],
      "fix": "col IS TRUE / col IS NOT TRUE (cross-ref with schema)",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 14,
      "name": "Array out of bounds",
      "severity": "CRITICAL",
      "regex": ["\\w+\\[\\d+\\]", "\\.getItem\\s*\\(\\d+\\)", "F\\.element_at\\s*\\("],
      "fix": "TRY_ELEMENT_AT(array, index) -- 1-indexed",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 15,
      "name": "Map key not found",
      "severity": "CRITICAL",
      "regex": ["\\w+\\['[^']+'\\]", "\\w+\\[\"[^\"]+\"\\]", "\\.getItem\\s*\\(\\s*[\"']"],
      "fix": "TRY_ELEMENT_AT(map, 'key')",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 16,
      "name": "to_date / to_timestamp",
      "severity": "CRITICAL",
      "regex": ["(?i)\\bto_date\\s*\\(", "(?i)\\bto_timestamp\\s*\\(", "F\\.to_date\\s*\\(", "F\\.to_timestamp\\s*\\("],
      "fix": "try_to_date / try_to_timestamp via F.expr()",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 17,
      "name": "make_timestamp / make_date",
      "severity": "CRITICAL",
      "regex": ["(?i)\\bmake_timestamp\\s*\\(", "(?i)\\bmake_date\\s*\\("],
      "fix": "try_make_timestamp / try_make_date",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 18,
      "name": "SUM on INT columns",
      "severity": "CRITICAL",
      "regex": ["(?i)\\bSUM\\s*\\(\\s*\\w+\\s*\\)", "(?i)\\bSUM\\s*\\(\\s*`[^`]+`\\s*\\)"],
      "fix": "SUM(CAST(col AS BIGINT))",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 19,
      "name": "Integer overflow in arithmetic",
      "severity": "HIGH",
      "regex": ["F\\.col\\([^)]+\\)\\s*[*+]\\s*F\\.col"],
      "fix": "CAST to BIGINT before arithmetic",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 20,
      "name": "f.lit() wrapping format strings",
      "severity": "CRITICAL",
      "regex": ["F\\.to_date\\s*\\([^,]+,\\s*F\\.lit\\s*\\(", "F\\.to_timestamp\\s*\\([^,]+,\\s*F\\.lit\\s*\\(", "f\\.to_date\\s*\\([^,]+,\\s*f\\.lit\\s*\\(", "f\\.to_timestamp\\s*\\([^,]+,\\s*f\\.lit\\s*\\(", "F\\.try_to_timestamp\\s*\\([^,]+,\\s*F\\.lit\\s*\\("],
      "fix": "Remove F.lit() -- format strings must be plain Python strings",
      "ref": "13-serverless-known-issues.md Issue 7"
    },
    {
      "id": 21,
      "name": "Implicit string-to-numeric",
      "severity": "HIGH",
      "regex": ["Manual review: check JOIN conditions with mismatched types"],
      "fix": "Use explicit TRY_CAST in joins and arithmetic"
    },
    {
      "id": 22,
      "name": "parse_url on invalid URLs",
      "severity": "MEDIUM",
      "regex": ["(?i)\\bparse_url\\s*\\(", "(?i)\\burl_decode\\s*\\("],
      "fix": "try_parse_url",
      "ref": "07-ansi-compliance-reference.md"
    },
    {
      "id": 23,
      "name": ".persist() / .cache()",
      "severity": "HIGH",
      "regex": ["\\.persist\\s*\\(", "\\.cache\\s*\\(", "\\.unpersist\\s*\\(", "(?i)\\bCACHE\\s+(?:LAZY\\s+)?TABLE\\b", "(?i)\\bUNCACHE\\s+TABLE\\b"],
      "fix": "Remove"
    },
    {
      "id": 24,
      "name": "REFRESH TABLE",
      "severity": "HIGH",
      "regex": ["(?i)\\bREFRESH\\s+TABLE\\b"],
      "fix": "Remove"
    },
    {
      "id": 25,
      "name": "MSCK REPAIR TABLE",
      "severity": "HIGH",
      "regex": ["(?i)\\bMSCK\\s+REPAIR\\s+TABLE\\b"],
      "fix": "Remove"
    },
    {
      "id": 26,
      "name": "CREATE/REFRESH MATERIALIZED VIEW",
      "severity": "HIGH",
      "regex": ["(?i)\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?MATERIALIZED\\s+VIEW\\b", "(?i)\\bREFRESH\\s+MATERIALIZED\\s+VIEW\\b"],
      "fix": "Must use SQL Warehouse"
    },
    {
      "id": 27,
      "name": "Temp view reuse in loops",
      "severity": "MEDIUM",
      "regex": ["(?i)createOrReplaceTempView\\s*\\("],
      "fix": "Verify temp views are not created/consumed in tight loops"
    },
    {
      "id": 28,
      "name": "Unsupported Spark configs",
      "severity": "CRITICAL",
      "regex": ["spark\\.databricks\\.delta\\.retentionDurationCheck\\.enabled", "spark\\.databricks\\.delta\\.schema\\.auto[Mm]erge\\.enabled", "spark\\.databricks\\.delta\\.optimizeWrite\\.enabled", "spark\\.databricks\\.delta\\.autoCompact\\.enabled", "spark\\.sql\\.broadcastTimeout", "spark\\.sql\\.streaming\\.stateStore\\.stateSchemaCheck", "spark\\.sql\\.caseSensitive", "spark\\.executor\\.", "spark\\.driver\\.extra", "spark\\.dynamicAllocation\\.", "spark\\.shuffle\\.service\\.", "spark\\.sql\\.warehouse\\.dir", "spark\\.hadoop\\.", "fs\\.azure\\."],
      "fix": "See per-config replacement in resource 05",
      "ref": "05-spark-config-classic-to-serverless.md"
    },
    {
      "id": 29,
      "name": "spark.sql.ansi.enabled = false",
      "severity": "CRITICAL",
      "regex": ["(?i)spark\\.sql\\.ansi\\.enabled.*false", "(?i)SET\\s+spark\\.sql\\.ansi\\.enabled\\s*=\\s*false", "(?i)SET\\s+ANSI_MODE\\s*=\\s*false"],
      "fix": "Remove. Fix code with ANSI-safe patterns. NEVER disable ANSI."
    },
    {
      "id": 30,
      "name": "os.environ / sys.argv",
      "severity": "HIGH",
      "regex": ["os\\.environ\\.get\\s*\\(", "os\\.environ\\[", "os\\.getenv\\s*\\(", "sys\\.argv"],
      "fix": "dbutils.widgets.get()"
    },
    {
      "id": 31,
      "name": "Init script dependencies",
      "severity": "MEDIUM",
      "regex": ["init_scripts", "dbfs:/.*\\.sh", "/Volumes/.*\\.sh"],
      "fix": "Move to requirements.txt"
    },
    {
      "id": 32,
      "name": "%pip install in notebooks",
      "severity": "MEDIUM",
      "regex": ["(?m)^%pip\\s+install\\b"],
      "fix": "Move to centralized requirements.txt"
    },
    {
      "id": 33,
      "name": "dbutils.library.install",
      "severity": "HIGH",
      "regex": ["dbutils\\.library\\.install\\s*\\(", "dbutils\\.library\\.restartPython\\s*\\("],
      "fix": "Move to requirements.txt"
    },
    {
      "id": 34,
      "name": "Unpinned dependencies",
      "severity": "LOW",
      "regex": ["Review %pip install lines and requirements.txt for missing version pins"],
      "fix": "Pin all versions: package==1.2.3"
    },
    {
      "id": 35,
      "name": "com.crealytics.spark.excel",
      "severity": "HIGH",
      "regex": ["com\\.crealytics\\.spark\\.excel"],
      "fix": "pandas + openpyxl pattern (see resource 06)"
    },
    {
      "id": 36,
      "name": "Incompatible wheel files",
      "severity": "MEDIUM",
      "regex": ["\\.whl\\b"],
      "fix": "Rebuild for cp312 (Python 3.12)"
    },
    {
      "id": 37,
      "name": ".count() > 0 existence check",
      "severity": "LOW",
      "regex": ["\\.count\\s*\\(\\s*\\)\\s*>\\s*0", "\\.count\\s*\\(\\s*\\)\\s*==\\s*0", "\\.count\\s*\\(\\s*\\)\\s*!=\\s*0"],
      "fix": ".first() is not None"
    },
    {
      "id": 38,
      "name": "ThreadPoolExecutor",
      "severity": "MEDIUM",
      "regex": ["concurrent\\.futures", "ThreadPoolExecutor", "multiprocessing\\.Pool", "multiprocessing\\.Process"],
      "fix": "Use Workflows for-each tasks"
    },
    {
      "id": 39,
      "name": "Chained .withColumn() (>20)",
      "severity": "MEDIUM",
      "regex": ["\\.withColumn\\s*\\("],
      "fix": "Single .withColumns() call"
    },
    {
      "id": 40,
      "name": "SELECT * usage",
      "severity": "MEDIUM",
      "regex": ["(?i)\\bSELECT\\s+\\*\\s+FROM\\b", "(?i)\\bINSERT\\s+(?:INTO|OVERWRITE)\\s+.*?\\bSELECT\\s+\\*"],
      "fix": "Use explicit column lists"
    },
    {
      "id": 41,
      "name": "/tmp file paths",
      "severity": "MEDIUM",
      "regex": ["[\"']/tmp/[^\"']*[\"']", "open\\s*\\(\\s*[\"']/tmp/"],
      "fix": "/local_disk0/tmp/"
    },
    {
      "id": 42,
      "name": "spark.createDataFrame without schema",
      "severity": "MEDIUM",
      "regex": ["spark\\.createDataFrame\\s*\\([^,)]+\\)(?!\\s*,)"],
      "fix": "Provide explicit StructType schema"
    },
    {
      "id": 43,
      "name": "Complex nested JSON without schema",
      "severity": "HIGH",
      "regex": ["spark\\.createDataFrame\\s*\\(.*?(?:response|json|api|result|data)\\b"],
      "fix": "Define explicit StructType for API/JSON responses"
    },
    {
      "id": 44,
      "name": "_metadata column conflict",
      "severity": "HIGH",
      "regex": ["(?i)['\"]_metadata['\"]", "(?i)col\\s*\\(\\s*['\"]_metadata['\"]", "(?i)\\._metadata\\b", "(?i)`_metadata`"],
      "fix": "Check table row tracking; disable or rename column"
    },
    {
      "id": 45,
      "name": "Pure SQL notebook (Path D candidate)",
      "severity": "INFO",
      "regex": ["Analyze: all cells are SQL, no PySpark logic"],
      "fix": "Recommend DBSQL Serverless (Path D)"
    },
    {
      "id": 46,
      "name": "Job JSON transformation",
      "severity": "REQUIRED",
      "regex": ["Check JSON for job_clusters, environment_key, environments"],
      "fix": "Apply serverless JSON template (resource 16)"
    },
    {
      "id": 47,
      "name": "PowerShell %env_name% replacement",
      "severity": "REQUIRED",
      "regex": ["\\.Replace\\s*\\(\\s*\"%env_name%\""],
      "fix": "Add .Replace(\"%env_name%\",\"$env_name\") to deploy script"
    },
    {
      "id": 48,
      "name": "Variable group env_name",
      "severity": "REQUIRED",
      "regex": ["Verify env_name in variable group"],
      "fix": "Add env_name if missing"
    },
    {
      "id": 49,
      "name": "Job parameters",
      "severity": "REQUIRED",
      "regex": ["Check JSON for PATH_LANDING, PATH_DATALAKE parameters"],
      "fix": "Add parameters block to job JSON (resource 16)"
    }
  ]
}
```

---

## Appendix C: SOP Section Cross-Reference

Where each SOP section maps to checks in this assessment:

| SOP Section | Topic | Assessment Checks |
|-------------|-------|-------------------|
| Section 2 | Language & Compute Requirements | 1, 2, 3 |
| Section 3 | Unity Catalog / Data Access | 4, 8 |
| Section 4 | Unsupported Operations | 5, 23, 24, 25, 26, 27 |
| Section 5 | Environment Variables | 30 |
| Section 6 | Library & Dependency Migration | 6, 31, 32, 33, 34, 35, 36 |
| Section 7 | Spark Configuration | 28, 29 (we override SOP on ANSI) |
| Section 8 | Performance Patterns | 37, 38, 39, 40, 41, 42 |
| Section 9 | SQL Optimization | 45 |
| CI/CD Guide | Repo-side changes | 46, 47, 48, 49 |
