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
    - TRY_CAST for every unsafe CAST operation (numeric, date, timestamp, boolean)
    - TRY_DIVIDE and null guards for division by zero
    - TRY_ELEMENT_AT for array and map out-of-bounds access
    - IS TRUE / IS NOT TRUE for boolean-to-integer comparisons
    - try_to_date and try_to_timestamp for invalid date/time strings
    - Type widening for integer overflow (CAST to BIGINT before arithmetic)

2. Deprecated config detection and removal
    - Configs removed between 13.3 and 16.4 with exact replacement actions
    - Detection regex patterns for spark.conf.set and SQL SET statements
    - Init script configs that need migration

3. Changed default detection
    - spark.sql.ansi.enabled: false to true (most impactful change)
    - spark.sql.sources.default: parquet to delta
    - AQE behavior changes (more aggressive partition coalescing)
    - spark.sql.session.timeZone differences

4. Deprecated API detection
    - SQL functions deprecated or with changed behavior
    - DataFrame method changes
    - UDF registration pattern changes
    - Legacy mode flags that no longer exist

5. New features available in 16.4
    - Liquid Clustering (GA, replaces ZORDER for new tables)
    - Predictive I/O (automatic, no code change)
    - IDENTIFIER() clause for dynamic SQL without injection
    - Default column values
    - Python UDF performance improvements (3-5x faster)

6. Delta Lake changes
    - Protocol version auto-upgrade risks (irreversible)
    - Deletion vectors (default for new tables)
    - Row tracking availability (_metadata column conflicts)
    - Column mapping defaults

7. PySpark-specific changes
    - Arrow-based UDF defaults in 16.4
    - pandas_udf improvements and stability
    - Simplified traceback for better UDF error messages

8. Scala-specific changes
    - Scala 2.12.15 to 2.12.18 compatibility (minor)
    - Dataset API deprecations
    - Type inference changes

9. Structured regex patterns for automated scanning
    - 33 patterns organized by severity (Critical, High, Medium, Low)
    - Machine-readable JSON format for tooling integration
    - References `resources/15-breaking-changes-13-to-16-regex.md`

10. Scan checklist
    - Pass 1 (Critical): CAST, division, boolean, array/map, to_date/to_timestamp, configs, REFRESH/MSCK
    - Pass 2 (High): persist/cache, RDD APIs, env vars, libraries, ANSI=false
    - Pass 3 (Medium): SELECT *, threading, /tmp paths, schema inference, chained withColumn
    - Pass 4 (Low): count anti-patterns, ZORDER awareness, manual partition configs

#### Scala to PySpark Skill

**Purpose:** Complete reference for converting Databricks Scala notebooks to PySpark.

Features:

1. Import translations
    - org.apache.spark.sql.functions._ to from pyspark.sql import functions as F
    - org.apache.spark.sql.types._ to from pyspark.sql.types import *
    - Always use F. prefix convention (never import * from functions)

2. Column reference syntax
    - $"col" to F.col("col")
    - .as("alias") to .alias("alias")
    - === to ==, =!= to !=
    - && to & with parentheses, || to | with parentheses

3. Case class to StructType / dataclass conversion
    - Typed Dataset .as[CaseClass] patterns removed entirely
    - Schema definition converted to StructType with StructField
    - Data containers converted to Python dataclass or namedtuple

4. Pattern matching to if/elif or dictionary
    - Simple value matching to dictionary lookup
    - Nested pattern matching to if/elif chains
    - Pattern matching inside UDFs to Python conditionals

5. Option/Some/None to Python None handling
    - .getOrElse(default) to x if x is not None else default
    - .map(f) to f(x) if x is not None else None
    - .isDefined/.isEmpty to is not None / is None

6. Try/Success/Failure to try/except
    - Try block to try/except with specific exception types
    - Success/Failure matching to result/exception handling

7. UDF conversion
    - Explicit returnType required in PySpark UDFs
    - None handling for every code path (null in Scala to None in Python)
    - @F.udf decorator preferred over F.udf() wrapper
    - SQL-registered UDFs via spark.udf.register with returnType

8. Array/map access sugar
    - column(index) to column[index] or .getItem(index) - never column(index)
    - typedLit(Map(...)) to F.create_map() with F.element_at() for lookups
    - split(...)(n) to F.split(...).getItem(n) or F.split(...)[n]

9. Boolean operator precedence
    - && to & and || to | require parentheses around BOTH operands
    - ! (not) to ~ with parentheses
    - Python & and | have higher precedence than comparison operators

10. Numeric precision differences
    - BigDecimal HALF_UP rounding to Decimal with ROUND_HALF_UP (not Python round())
    - Integer division: Scala / is integer division, Python / is float division (use //)
    - Floating-point comparison tolerance (1e-6) for aggregates

11. Date parsing edge cases
    - SimpleDateFormat lenient mode rolls invalid dates forward; Python strptime raises ValueError
    - Thread safety differences (SimpleDateFormat is not thread-safe)
    - Timezone handling: JVM default TZ vs Python naive datetime

12. Silent data difference patterns
    - Null propagation: .getOrElse(null) must map to None, not "" or 0
    - Regex escaping: Java regex in Spark SQL functions vs Python re module
    - Empty collection behavior: .head throws different exceptions in each language

13. Non-determinism warnings
    - Sort order with nulls may differ between runs
    - dropDuplicates row selection is non-deterministic
    - HashMap iteration order (non-deterministic in Scala, insertion-ordered in Python 3.7+)
    - Floating-point aggregation order can produce tiny differences

14. Comprehensive conversion checklist
    - Column references, type system, UDFs, boolean operators
    - Null handling, numeric precision, date parsing
    - Variable names (Python reserved words), regex, collections

#### Conversion Validator Skill

**Purpose:** Validate that migrated code produces output identical to the original pipeline. 18-check framework organized from fast/cheap to slow/thorough.

Features:

1. Schema comparison
    - Column names must match exactly
    - Column types must match exactly
    - Nullable mismatches flagged as warnings

2. Row count comparison
    - Exact count match required
    - Orphan row detection via full outer join on primary key
    - Zero orphans in either direction

3. Null count comparison
    - Per-column null counts must match exactly
    - First signal of UDF conversion issues
    - Differences indicate changed null handling

4. Aggregate statistics comparison
    - SUM, AVG, MIN, MAX per numeric column
    - Tolerance-based comparison (1e-6 for floating-point)
    - Zero count and negative count must match exactly

5. Distinct value set comparison
    - Per string/categorical column
    - Identical distinct value sets required
    - New or missing values indicate transformation changes

6. Row-by-row data comparison
    - Uses eqNullSafe (null == null is true, not null)
    - Zero mismatches expected across all columns
    - Sample mismatched rows shown for debugging

7. Date/timestamp deep validation
    - Value-by-value date comparison
    - Timezone offset detection (dates off by 1 day, timestamps off by hours)
    - Null vs epoch zero confusion (null becoming 1970-01-01)
    - Date boundary cases (leap year, month-end, year rollover)
    - Date type comparison (DATE vs TIMESTAMP in schema)
    - String date format comparison for bronze-layer columns
    - Identifies all date/timestamp columns including string dates by name pattern

8. Null vs empty string confusion detection
    - Detects null becoming "" or "" becoming null
    - Detects null becoming a default string value
    - Critical for joins (null != null but "" == "")

9. Null vs zero/default numeric confusion detection
    - Detects null becoming 0 or 0 becoming null
    - Affects SUM and AVG calculations (null is excluded, 0 is included)

10. Boolean/flag equivalence check
    - Case differences ("NORMAL" vs "Normal")
    - Whitespace differences
    - Boolean column true/false/null distribution comparison

11. Numeric precision and rounding difference detection
    - Categorizes diffs: floating_point_noise (<1e-10), rounding_difference (<0.01), small (<1.0), large (>=1.0)
    - Detects banker's rounding vs HALF_UP pattern (values differing by ~0.01)
    - Floating-point noise is acceptable; rounding differences need investigation

12. UDF output consistency check
    - Targeted checks per UDF-produced column
    - Categorizes mismatches as null-related vs value-different
    - Shows input values that caused mismatches for debugging

13. Edge case data patterns
    - Negative number count comparison
    - Empty string vs null distribution
    - Very large numbers (potential overflow)
    - Special string values (N/A, null, None, NaN)
    - NaN count comparison
    - Duplicate primary key detection (validates join correctness)

14. Non-determinism detection
    - Identifies columns where differences may be from non-deterministic behavior
    - Checks if mismatched rows correlate with duplicate-like data
    - Floating-point aggregation order analysis (relative diff < 1e-10 is noise)

15. Serverless compute verification
    - Confirms job ran on serverless via run details API
    - Every task must have environment_key, no cluster_id
    - Detects if job accidentally ran on classic compute

16. Config compliance check
    - Scans run logs for CONFIG_NOT_AVAILABLE errors
    - Verifies no unsupported configs were set in notebook code
    - References supported config list from resource 05

17. Environment key verification
    - Confirms environment_key on all tasks in job config
    - Verifies environments block has client "4"
    - Checks for unresolved %env_name% placeholders in dependencies path

18. Performance comparison
    - Task-level duration comparison: classic vs serverless
    - Flags any task exceeding 2x classic runtime as regression
    - Informational, not a blocking failure

#### Conversion Report Skill

**Purpose:** Generate audit trail documenting every code change and its rationale.

Features:

1. Executive summary
    - Total change count
    - Breakdown by category: syntax, API, ANSI compliance, runtime, UDF, behavioral
    - Risk assessment: how many changes are purely syntactic vs potentially behavior-altering

2. Notebook-by-notebook diff
    - Every change with notebook name, cell/line number
    - Category, original code, converted code, reason for change
    - Risk level (None, Low, Medium, High) and behavioral impact (Yes/No/Possible)

3. ANSI compliance audit section
    - Every ANSI-sensitive pattern found in original code
    - How it was addressed (TRY_CAST, TRY_DIVIDE, IS TRUE, etc.)
    - Risk assessment per fix

4. UDF conversion detail
    - Side-by-side Scala vs PySpark code for every UDF
    - Null handling analysis per UDF (every code path traced)
    - Edge case behavior comparison

5. Date/timestamp conversion detail
    - Every date operation analyzed for timezone, format, and precision differences
    - Cross-referenced with validation results from Check 7
    - SimpleDateFormat lenient mode behavior documented

6. Risk summary table
    - All changes ranked by risk (High, Medium, Low, None)
    - High-risk changes include mitigation steps
    - Validation references for each risk item

7. Serverless-specific change section
    - Job JSON transformation details (before/after)
    - Environment variable migration (os.environ.get to dbutils.widgets.get)
    - Spark config removals with rationale per config
    - Unsupported operation removals (REFRESH TABLE, MSCK REPAIR, .persist())

8. Library replacement section
    - Each replaced library with original usage and new approach
    - Before/after code examples
    - Why the change was needed (JAR not supported, etc.)
    - Risk level and testing notes

9. Repo-side change manifest
    - Files changed in Azure DevOps repo (job JSON, PowerShell script)
    - Variable group changes (if any)
    - Deployment verification checklist

### 2.3 Prompts (6 files)

| Prompt | Runs In | Purpose |
|--------|---------|---------|
| `assess_databricks.txt` | Genie Code | Assessment + change manifest for single job or batch |
| `assess_repo.txt` | VS Code/Copilot or manual | Repo-side checklist for Azure DevOps artifacts |
| `migrate_to_serverless.txt` | Genie Code | Apply code changes to staging copies in workspace |
| `validate_migration.txt` | Genie Code | Full validation suite with structured pass/fail report |
| `prompt_scala_upgrade.txt` | Genie Code | Upgrade a Scala pipeline from 13.3to16.4 |
| `prompt_scala_to_pyspark_upgrade.txt` | Genie Code | Convert ScalatoPySpark + upgrade to 16.4 |


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

