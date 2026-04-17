# Serverless & DBR Migration Toolkit

**Date:** 2026-04-17
**Author:** Maggie Barrett
**Status:** Built, ready for execution
**Repo:** https://github.com/maggie-barr-db/serverless-migration-toolkit (dev branch)


## 1. Scope

~5,000 Databricks jobs at Molina Healthcare need to be migrated across four paths. Jobs will be processed in batches of 12-100. A manifest will specify each job's prescribed outcome.

### Migration Paths

| Path | From | To | What Changes |
|------|------|----|-------------|
| **A - DBR Upgrade Only** | Scala on 13.3 | Scala on 16.4 | Spark configs, ANSI-safe code fixes, deprecated API updates |
| **B - Convert + Upgrade** | Scala on 13.3 | PySpark on Serverless | Language conversion + DBR upgrade + serverless compute changes |
| **C - Upgrade + Serverless** | PySpark/SQL on 13.3 | PySpark/SQL on Serverless | DBR upgrade + serverless JSON/config/code changes |
| **D - SQL to DBSQL** | SQL-only or PySpark-SQL | DBSQL Serverless Current Channel | Notebook format conversion + SQL dialect changes |

Any path ending in serverless requires PySpark or SQL - Scala cannot run on serverless notebooks (no GA serverless option for Scala). Paths B and C are compound migrations.

### Two Execution Tracks

Every migration has changes in two places:

1. **Databricks-side** - notebook code, spark configs, table metadata. Genie Code assesses and assists with these.
2. **Repo-side (Azure DevOps)** - job JSON templates, CI/CD pipeline variables, PowerShell deployment scripts. These must be changed in the repository by a developer (manually or via VS Code + Copilot).


## 2. Toolkit Contents

The toolkit has three layers that work together. **Resources** are reference documents containing the knowledge base: migration patterns, code fix examples, known issues, regex scan patterns, and configuration maps. They are what Genie Code (or a developer) reads to understand what needs to change and why. **Skills** are instructional documents that teach Genie Code how to perform a specific task end-to-end, like upgrading a DBR version or validating a migration. Skills reference resources for detailed patterns but add workflow logic, ordering, and decision-making on top. **Prompts** are the executable entry points that a user runs to kick off a workflow. A prompt ties together one or more skills and resources into a step-by-step process for a specific goal like assessing a batch of jobs or validating a migration.

### 2.1 Resources (19 files)

**Scenario Guides:**

| # | File | Purpose |
|---|------|---------|
| 01 | `scala-13.3-to-scala-16.4.md` | Path A - Scala DBR upgrade. ANSI patterns, deprecated configs, Delta changes, ML runtime, scan checklist. |
| 02 | `scala-13.3-to-pyspark-serverless.md` | Path B - Language + DBR + compute. Order of operations, all serverless restrictions. |
| 03 | `pyspark-sql-13.3-to-pyspark-sql-serverless.md` | Path C - PySpark/SQL to serverless. ANSI, Python 3.12 changes, config migration. |
| 04 | `sql-to-dbsql-serverless.md` | Path D - SQL to DBSQL Serverless. Notebook conversion, SQL dialect, parameters. |

**Cross-Cutting References:**

| # | File | Purpose |
|---|------|---------|
| 05 | `spark-config-classic-to-serverless.md` | Complete config map: supported, unsupported, changed defaults. |
| 06 | `package-dependency-analysis.md` | Package audit, Spark-native replacements, requirements.txt rules. |
| 07 | `ansi-compliance-reference.md` | All 25+ ANSI error conditions, 20 patterns, safe alternatives. |
| 08 | `testing-validation-framework.md` | Environment promotion workflow, cross-catalog validation, 4-gate sign-off. |
| 09 | `performance-optimization-patterns.md` | Distributed patterns, count anti-patterns, cache removal, Arrow UDFs. |
| 10 | `ml-runtime-migration.md` | ML library version matrix, per-library breaking changes. |
| 11 | `archiving-workflow.md` | Git-based archiving (primary) + workspace staging (secondary). |
| 12 | `documentation-links.md` | Consolidated Databricks doc links by topic. |

**Operational References:**

| # | File | Purpose |
|---|------|---------|
| 13 | `serverless-known-issues.md` | 48 real Molina issues indexed by error message with resolutions. |
| 14 | `serverless-blockers.md` | Go/no-go eligibility screening with hard and soft blockers. |
| 15 | `breaking-changes-13-to-16-regex.md` | 33 regex scan patterns organized by severity for automated scanning. |
| 16 | `cicd-change-guide.md` | Azure DevOps pipeline changes, job JSON template, %env_name% fix. |
| 17 | `data-compatibility-checks.md` | Pre-migration table analysis with runnable SQL checks. |
| 18 | `batch-assessment-workflow.md` | Manifest-driven batch assessment with report templates. |
| 19 | `comprehensive-job-assessment.md` | Master assessment: 49 checks across 6 phases per job. |

### 2.2 Skills - Detailed Feature Specifications

#### DBR Upgrade Skill (13.3 to 16.4)

**Purpose:** Complete reference for upgrading Databricks Runtime from 13.3 LTS to 16.4 LTS. Applies to both Scala and PySpark code.

Features:

1. ANSI compliance fix patterns
    1. TRY_CAST for every unsafe CAST operation (numeric, date, timestamp, boolean)
    2. TRY_DIVIDE and null guards for division by zero
    3. TRY_ELEMENT_AT for array and map out-of-bounds access
    4. IS TRUE / IS NOT TRUE for boolean-to-integer comparisons
    5. try_to_date and try_to_timestamp for invalid date/time strings
    6. Type widening for integer overflow (CAST to BIGINT before arithmetic)

2. Deprecated config detection and removal
    1. Configs removed between 13.3 and 16.4 with exact replacement actions
    2. Detection regex patterns for spark.conf.set and SQL SET statements
    3. Init script configs that need migration

3. Changed default detection
    1. spark.sql.ansi.enabled: false to true (most impactful change)
    2. spark.sql.sources.default: parquet to delta
    3. AQE behavior changes (more aggressive partition coalescing)
    4. spark.sql.session.timeZone differences

4. Deprecated API detection
    1. SQL functions deprecated or with changed behavior
    2. DataFrame method changes
    3. UDF registration pattern changes
    4. Legacy mode flags that no longer exist

5. New features available in 16.4
    1. Liquid Clustering (GA, replaces ZORDER for new tables)
    2. Predictive I/O (automatic, no code change)
    3. IDENTIFIER() clause for dynamic SQL without injection
    4. Default column values
    5. Python UDF performance improvements (3-5x faster)

6. Delta Lake changes
    1. Protocol version auto-upgrade risks (irreversible)
    2. Deletion vectors (default for new tables)
    3. Row tracking availability (_metadata column conflicts)
    4. Column mapping defaults

7. PySpark-specific changes
    1. Arrow-based UDF defaults in 16.4
    2. pandas_udf improvements and stability
    3. Simplified traceback for better UDF error messages

8. Scala-specific changes
    1. Scala 2.12.15 to 2.12.18 compatibility (minor)
    2. Dataset API deprecations
    3. Type inference changes

9. Structured regex patterns for automated scanning
    1. 33 patterns organized by severity (Critical, High, Medium, Low)
    2. Machine-readable JSON format for tooling integration
    3. References `resources/15-breaking-changes-13-to-16-regex.md`

10. Scan checklist
    1. Pass 1 (Critical): CAST, division, boolean, array/map, to_date/to_timestamp, configs, REFRESH/MSCK
    2. Pass 2 (High): persist/cache, RDD APIs, env vars, libraries, ANSI=false
    3. Pass 3 (Medium): SELECT *, threading, /tmp paths, schema inference, chained withColumn
    4. Pass 4 (Low): count anti-patterns, ZORDER awareness, manual partition configs

#### Scala to PySpark Skill

**Purpose:** Complete reference for converting Databricks Scala notebooks to PySpark.

Features:

1. Import translations
    1. org.apache.spark.sql.functions._ to from pyspark.sql import functions as F
    2. org.apache.spark.sql.types._ to from pyspark.sql.types import *
    3. Always use F. prefix convention (never import * from functions)

2. Column reference syntax
    1. $"col" to F.col("col")
    2. .as("alias") to .alias("alias")
    3. === to ==, =!= to !=
    4. && to & with parentheses, || to | with parentheses

3. Case class to StructType / dataclass conversion
    1. Typed Dataset .as[CaseClass] patterns removed entirely
    2. Schema definition converted to StructType with StructField
    3. Data containers converted to Python dataclass or namedtuple

4. Pattern matching to if/elif or dictionary
    1. Simple value matching to dictionary lookup
    2. Nested pattern matching to if/elif chains
    3. Pattern matching inside UDFs to Python conditionals

5. Option/Some/None to Python None handling
    1. .getOrElse(default) to x if x is not None else default
    2. .map(f) to f(x) if x is not None else None
    3. .isDefined/.isEmpty to is not None / is None

6. Try/Success/Failure to try/except
    1. Try block to try/except with specific exception types
    2. Success/Failure matching to result/exception handling

7. UDF conversion
    1. Explicit returnType required in PySpark UDFs
    2. None handling for every code path (null in Scala to None in Python)
    3. @F.udf decorator preferred over F.udf() wrapper
    4. SQL-registered UDFs via spark.udf.register with returnType

8. Array/map access sugar
    1. column(index) to column[index] or .getItem(index) - never column(index)
    2. typedLit(Map(...)) to F.create_map() with F.element_at() for lookups
    3. split(...)(n) to F.split(...).getItem(n) or F.split(...)[n]

9. Boolean operator precedence
    1. && to & and || to | require parentheses around BOTH operands
    2. ! (not) to ~ with parentheses
    3. Python & and | have higher precedence than comparison operators

10. Numeric precision differences
    1. BigDecimal HALF_UP rounding to Decimal with ROUND_HALF_UP (not Python round())
    2. Integer division: Scala / is integer division, Python / is float division (use //)
    3. Floating-point comparison tolerance (1e-6) for aggregates

11. Date parsing edge cases
    1. SimpleDateFormat lenient mode rolls invalid dates forward; Python strptime raises ValueError
    2. Thread safety differences (SimpleDateFormat is not thread-safe)
    3. Timezone handling: JVM default TZ vs Python naive datetime

12. Silent data difference patterns
    1. Null propagation: .getOrElse(null) must map to None, not "" or 0
    2. Regex escaping: Java regex in Spark SQL functions vs Python re module
    3. Empty collection behavior: .head throws different exceptions in each language

13. Non-determinism warnings
    1. Sort order with nulls may differ between runs
    2. dropDuplicates row selection is non-deterministic
    3. HashMap iteration order (non-deterministic in Scala, insertion-ordered in Python 3.7+)
    4. Floating-point aggregation order can produce tiny differences

14. Comprehensive conversion checklist
    1. Column references, type system, UDFs, boolean operators
    2. Null handling, numeric precision, date parsing
    3. Variable names (Python reserved words), regex, collections

#### Conversion Validator Skill

**Purpose:** Validate that migrated code produces output identical to the original pipeline. 18-check framework organized from fast/cheap to slow/thorough.

Features:

1. Schema comparison
    1. Column names must match exactly
    2. Column types must match exactly
    3. Nullable mismatches flagged as warnings

2. Row count comparison
    1. Exact count match required
    2. Orphan row detection via full outer join on primary key
    3. Zero orphans in either direction

3. Null count comparison
    1. Per-column null counts must match exactly
    2. First signal of UDF conversion issues
    3. Differences indicate changed null handling

4. Aggregate statistics comparison
    1. SUM, AVG, MIN, MAX per numeric column
    2. Tolerance-based comparison (1e-6 for floating-point)
    3. Zero count and negative count must match exactly

5. Distinct value set comparison
    1. Per string/categorical column
    2. Identical distinct value sets required
    3. New or missing values indicate transformation changes

6. Row-by-row data comparison
    1. Uses eqNullSafe (null == null is true, not null)
    2. Zero mismatches expected across all columns
    3. Sample mismatched rows shown for debugging

7. Date/timestamp deep validation
    1. Value-by-value date comparison
    2. Timezone offset detection (dates off by 1 day, timestamps off by hours)
    3. Null vs epoch zero confusion (null becoming 1970-01-01)
    4. Date boundary cases (leap year, month-end, year rollover)
    5. Date type comparison (DATE vs TIMESTAMP in schema)
    6. String date format comparison for bronze-layer columns
    7. Identifies all date/timestamp columns including string dates by name pattern

8. Null vs empty string confusion detection
    1. Detects null becoming "" or "" becoming null
    2. Detects null becoming a default string value
    3. Critical for joins (null != null but "" == "")

9. Null vs zero/default numeric confusion detection
    1. Detects null becoming 0 or 0 becoming null
    2. Affects SUM and AVG calculations (null is excluded, 0 is included)

10. Boolean/flag equivalence check
    1. Case differences ("NORMAL" vs "Normal")
    2. Whitespace differences
    3. Boolean column true/false/null distribution comparison

11. Numeric precision and rounding difference detection
    1. Categorizes diffs: floating_point_noise (<1e-10), rounding_difference (<0.01), small (<1.0), large (>=1.0)
    2. Detects banker's rounding vs HALF_UP pattern (values differing by ~0.01)
    3. Floating-point noise is acceptable; rounding differences need investigation

12. UDF output consistency check
    1. Targeted checks per UDF-produced column
    2. Categorizes mismatches as null-related vs value-different
    3. Shows input values that caused mismatches for debugging

13. Edge case data patterns
    1. Negative number count comparison
    2. Empty string vs null distribution
    3. Very large numbers (potential overflow)
    4. Special string values (N/A, null, None, NaN)
    5. NaN count comparison
    6. Duplicate primary key detection (validates join correctness)

14. Non-determinism detection
    1. Identifies columns where differences may be from non-deterministic behavior
    2. Checks if mismatched rows correlate with duplicate-like data
    3. Floating-point aggregation order analysis (relative diff < 1e-10 is noise)

15. Serverless compute verification
    1. Confirms job ran on serverless via run details API
    2. Every task must have environment_key, no cluster_id
    3. Detects if job accidentally ran on classic compute

16. Config compliance check
    1. Scans run logs for CONFIG_NOT_AVAILABLE errors
    2. Verifies no unsupported configs were set in notebook code
    3. References supported config list from resource 05

17. Environment key verification
    1. Confirms environment_key on all tasks in job config
    2. Verifies environments block has client "4"
    3. Checks for unresolved %env_name% placeholders in dependencies path

18. Performance comparison
    1. Task-level duration comparison: classic vs serverless
    2. Flags any task exceeding 2x classic runtime as regression
    3. Informational, not a blocking failure

#### Conversion Report Skill

**Purpose:** Generate audit trail documenting every code change and its rationale.

Features:

1. Executive summary
    1. Total change count
    2. Breakdown by category: syntax, API, ANSI compliance, runtime, UDF, behavioral
    3. Risk assessment: how many changes are purely syntactic vs potentially behavior-altering

2. Notebook-by-notebook diff
    1. Every change with notebook name, cell/line number
    2. Category, original code, converted code, reason for change
    3. Risk level (None, Low, Medium, High) and behavioral impact (Yes/No/Possible)

3. ANSI compliance audit section
    1. Every ANSI-sensitive pattern found in original code
    2. How it was addressed (TRY_CAST, TRY_DIVIDE, IS TRUE, etc.)
    3. Risk assessment per fix

4. UDF conversion detail
    1. Side-by-side Scala vs PySpark code for every UDF
    2. Null handling analysis per UDF (every code path traced)
    3. Edge case behavior comparison

5. Date/timestamp conversion detail
    1. Every date operation analyzed for timezone, format, and precision differences
    2. Cross-referenced with validation results from Check 7
    3. SimpleDateFormat lenient mode behavior documented

6. Risk summary table
    1. All changes ranked by risk (High, Medium, Low, None)
    2. High-risk changes include mitigation steps
    3. Validation references for each risk item

7. Serverless-specific change section
    1. Job JSON transformation details (before/after)
    2. Environment variable migration (os.environ.get to dbutils.widgets.get)
    3. Spark config removals with rationale per config
    4. Unsupported operation removals (REFRESH TABLE, MSCK REPAIR, .persist())

8. Library replacement section
    1. Each replaced library with original usage and new approach
    2. Before/after code examples
    3. Why the change was needed (JAR not supported, etc.)
    4. Risk level and testing notes

9. Repo-side change manifest
    1. Files changed in Azure DevOps repo (job JSON, PowerShell script)
    2. Variable group changes (if any)
    3. Deployment verification checklist

### 2.3 Prompts (6 files)

#### Assess Databricks (assess_databricks.txt)

**Runs in:** Genie Code (Databricks workspace)
**Purpose:** Assess one or more jobs for migration readiness. Produces a structured report and change manifest per job.

Steps:

1. Extract job configuration via Jobs API
    1. Pull DBR version, language, cluster spec, init scripts, libraries, spark configs
    2. Identify all notebook paths and %run dependencies
    3. Classify: language, compute type, streaming, GPU
    4. Auto-recommend migration path based on classification

2. Eligibility screening
    1. Check hard blockers (Scala, R, GPU, streaming, non-UC)
    2. Check soft blockers (init scripts, JARs, RDD APIs, unsupported configs)
    3. Flag impossible combinations (e.g., serverless prescribed for Scala without conversion)

3. Code-level audit
    1. Scan all notebooks for ANSI compliance issues (25+ patterns)
    2. Scan for unsupported serverless operations (persist, REFRESH TABLE, MSCK REPAIR)
    3. Scan for unsupported Spark configs
    4. Scan for environment variable usage, library issues, performance anti-patterns

4. Data compatibility check
    1. Check table type (managed vs external) for every output table
    2. Check Delta protocol versions, row tracking, datetime columns, file layout
    3. Produce per-table risk rating (HIGH/MEDIUM/LOW)

5. Produce change manifest
    1. Cell/line-level diffs with before/after code for every finding
    2. Classify each finding as Databricks-side or repo-side
    3. Format for developer to apply to repo source files

6. Produce assessment report
    1. Structured report with finding counts by severity
    2. Effort estimate (LOW/MEDIUM/HIGH)
    3. Batch summary if multiple jobs assessed

#### Assess Repo (assess_repo.txt)

**Runs in:** VS Code/Copilot or manual (Azure DevOps access required)
**Purpose:** Verify and apply repo-side changes for job JSON templates, deployment scripts, and variable groups. Genie Code cannot access Azure DevOps, so this runs outside Databricks.

Steps:

1. Audit job JSON templates
    1. Verify job_clusters section removed
    2. Verify environment_key added to each task
    3. Verify environments block with client "4" and requirements.txt path
    4. Verify parameters block (PATH_LANDING, PATH_DATALAKE) with %env_name%
    5. Verify queue and performance_optimized settings
    6. Flag any hardcoded environment values that should be %env_name%

2. Audit PowerShell deployment script
    1. Verify %env_name% replacement line exists
    2. Verify all placeholders in job JSONs have matching .Replace() calls
    3. Verify job create vs reset logic (preserves job ID)

3. Audit variable groups
    1. Verify env_name variable exists in all environment-specific groups
    2. Confirm no new variables needed

4. Produce repo change report with checklist

#### Migrate to Serverless (migrate_to_serverless.txt)

**Runs in:** Genie Code (Databricks workspace)
**Purpose:** Execute the Databricks-side migration for a single job. Creates staging copies, applies fixes, produces change manifest.

Steps:

1. Create staging copies
    1. Copy each notebook to /Workspace/Migration/staging/{job_name}/
    2. Preserve folder structure
    3. Never modify original deployed notebooks

2. Apply ANSI-safe fixes
    1. TRY_CAST, TRY_DIVIDE, IS TRUE, try_to_date, try_to_timestamp
    2. Array/map bounds checks
    3. Type widening for overflow

3. Remove unsupported operations
    1. .persist()/.cache(), REFRESH TABLE, MSCK REPAIR TABLE
    2. Convert materialized views to SQL Warehouse note
    3. Convert global temp views to session-scoped

4. Migrate Spark configs
    1. Remove unsupported configs
    2. Remove spark.sql.ansi.enabled = false

5. Migrate environment variables
    1. Replace os.environ.get() with dbutils.widgets.get()

6. Migrate dependencies
    1. Remove %pip install, dbutils.library.install
    2. Replace com.crealytics.spark.excel with pandas + openpyxl

7. Scala to PySpark conversion (Path B only)
    1. Apply scala_to_pyspark skill before other fixes

8. SQL to DBSQL conversion (Path D only)
    1. Extract SQL from spark.sql() calls
    2. Convert parameters/widgets

9. Apply safe performance recommendations
    1. .count() > 0 to .first() is not None
    2. Remove manual shuffle partition settings

10. Produce change manifest
    1. Cell/line diffs for every change made
    2. Save as change_manifest.md in staging folder

11. Create test job pointing to staging notebooks

#### Validate Migration (validate_migration.txt)

**Runs in:** Genie Code (Databricks workspace)
**Purpose:** Run the full validation suite against all output tables and produce a structured pass/fail report.

Steps:

1. Setup
    1. Load original (baseline) and migrated DataFrames
    2. Identify primary keys and exclude metadata columns
    3. Classify columns by type

2. Structural checks (seconds)
    1. Schema comparison
    2. Row count + orphan detection

3. Statistical checks (seconds to minutes)
    1. Null counts per column
    2. Aggregate stats with tolerance
    3. Distinct value sets

4. Row-level checks (minutes)
    1. Row-by-row comparison with eqNullSafe
    2. Date/timestamp deep validation (7 sub-checks)

5. Semantic checks (minutes)
    1. Null vs empty string confusion
    2. Null vs zero/default confusion
    3. Boolean/flag equivalence
    4. Numeric precision/rounding
    5. UDF output consistency

6. Edge case checks (minutes)
    1. Edge case patterns (negatives, special strings, NaN)
    2. Non-determinism detection

7. Serverless-specific checks (Paths B, C)
    1. Confirm ran on serverless compute
    2. Config compliance (no CONFIG_NOT_AVAILABLE errors)
    3. Environment key verification
    4. Performance comparison (flag >2x regressions)

8. DBSQL-specific checks (Path D)
    1. Warehouse execution verification
    2. Parameter passing verification
    3. SQL syntax compatibility

9. Produce validation report
    1. Per-table pass/fail with check details
    2. Performance comparison table
    3. Failure root cause analysis with fix references
    4. Recommendation: approved / needs fixes / needs investigation

#### Scala Upgrade (prompt_scala_upgrade.txt)

**Runs in:** Genie Code (Databricks workspace)
**Purpose:** Upgrade a Scala pipeline from 13.3 to 16.4 while keeping it in Scala.

Steps:

1. Record baseline Delta table versions
2. Copy pipeline notebooks to new folder
3. Apply dbr_upgrade skill (ANSI fixes, deprecated configs, API changes)
4. Run upgraded pipeline on 16.4 cluster
5. Validate using conversion_validator (time travel mode)
6. Generate conversion_report

#### Scala to PySpark Upgrade (prompt_scala_to_pyspark_upgrade.txt)

**Runs in:** Genie Code (Databricks workspace)
**Purpose:** Convert a Scala pipeline to PySpark and upgrade to 16.4.

Steps:

1. Confirm baseline Delta table versions
2. Copy and convert Scala notebooks to PySpark (scala_to_pyspark skill)
3. Apply dbr_upgrade skill to PySpark notebooks (ANSI fixes, configs)
4. Run converted pipeline on 16.4 cluster
5. Validate using conversion_validator (time travel: original Scala 13.3 vs PySpark 16.4)
6. Generate conversion_report
7. If validation fails, isolate: language conversion vs runtime upgrade


## 3. Execution Workflow

### Phase 1: Assessment (per batch of 12-100 jobs)

```
                    ┌──────────────────┐
                    │  Job Manifest     │
                    │  (ID + path)      │
                    └────────┬─────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
    ┌───────────▼──────────┐  ┌──────────▼───────────┐
    │  Databricks Assessment│  │  Repo Assessment      │
    │  (Genie Code)         │  │  (manual / VS Code)   │
    │                       │  │                       │
    │ - Job configs (API)  │  │ - Job JSON templates │
    │ - Notebook code scan │  │ - PowerShell scripts │
    │ - Table metadata     │  │ - Variable groups    │
    │ - Data spot checks   │  │ - %env_name% fix     │
    │                       │  │                       │
    │  Output: Change       │  │  Output: Repo change  │
    │  manifest per job     │  │  checklist            │
    └───────────┬──────────┘  └──────────┬───────────┘
                │                         │
                └────────────┬────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Review with team  │
                    └──────────────────┘
```

### Phase 2: Execution

**Databricks-side (Genie Code in UAT workspace):**
1. Create staging copies of notebooks
2. Apply ANSI-safe code fixes (TRY_CAST, TRY_DIVIDE, IS TRUE, etc.)
3. Replace os.environ.get to dbutils.widgets.get
4. Remove unsupported operations (REFRESH TABLE, MSCK REPAIR, .persist())
5. Remove/replace unsupported configs
6. Replace unsupported libraries (spark.excel to pandas+openpyxl)
7. ScalatoPySpark conversion (Path B only)
8. Produce change manifest for developer

**Repo-side (Developer in Azure DevOps):**
1. Apply change manifest to repo source files (preserving %placeholder% tokens)
2. Update job JSON templates (remove job_clusters, add environments block)
3. Add %env_name% replacement to PowerShell deploy script
4. Commit to feature branch

### Phase 3: Validation

```
Gate 1: UAT Staging (Genie Code changes in workspace)
  to Run migrated job from staging copies
  to Validate output against baseline

Gate 2: Repo Commit (Developer applies changes)
  to Feature branch committed with parameterized source files

Gate 3: CI/CD Round-Trip (Confirms repo version works)
  to CI/CD deploys feature branch to UAT
  to Re-run validation (confirms CI/CD version matches staging)

Gate 4: Production Cutover
  to PR merged to CI/CD deploys to PROD
  to First run monitored, post-deployment validation
```

**Key constraint:** Genie Code operates in Databricks only. Workspace notebooks have hardcoded environment values (from CI/CD deployment). The path back to the repo is always through the **change manifest** applied to parameterized source files - never by exporting workspace notebooks.


## 4. Design Principles

1. **ANSI-safe fixes, not blanket flags.** Never recommend `spark.sql.ansi.enabled = false`. Always generate the specific ANSI-safe code fix. ANSI mode is mandatory on serverless and cannot be disabled.

2. **Every output table gets checked.** Regulated healthcare pipelines. No shortcuts on data validation.

3. **Additive changes to CI/CD.** Do not remove existing pipeline variables or modify deployment script structure. Only add the new `%env_name%` replacement and update job JSON templates.

4. **Managed vs external table awareness.** External tables need explicit OPTIMIZE, VACUUM, ANALYZE. No Predictive Optimization.

5. **Two-track execution.** Databricks-side (Genie Code) and repo-side (developer/VS Code). Never conflate.

6. **Env-based catalog awareness.** Molina uses `USE CATALOG {{env}}_catalog` with 2-part table names. Do not flag as non-UC. ADLS `abfss://` paths are registered in Unity Catalog - do not flag as non-UC.

7. **Batch-oriented design.** Everything works for 12-100 jobs at a time.

8. **GA features only.** Do not recommend Public Preview features (VACUUM LITE, serverless JAR tasks). Only recommend GA features.

9. **Change manifests, not file exports.** Genie Code produces change manifests (cell/line diffs). Developers apply these to repo source files. Never export workspace notebooks back to the repo (they contain hardcoded environment values).

10. **UAT-first workflow.** All migration work happens in UAT. Changes promote to PROD only after CI/CD round-trip validation passes.

