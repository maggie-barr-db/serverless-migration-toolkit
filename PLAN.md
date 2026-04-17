# Serverless Migration Toolkit — Plan & Gap Assessment

**Date:** 2026-04-15
**Author:** Maggie Barrett + Claude
**Status:** Draft — pending final inputs before build

## 1. Scope

~5,000 Databricks jobs at Molina Healthcare need to be migrated across four paths. Jobs will be processed in batches of 12–100. A manifest will specify each job's prescribed outcome.

### Migration Paths

| Path | From | To | What Changes |
|------|------|----|-------------|
| **A — DBR Upgrade Only** | Scala on 13.3 | Scala on 16.4 | Spark configs, ANSI-safe code fixes, deprecated API updates |
| **B — Convert + Upgrade** | Scala on 13.3 | PySpark on 16.4 | Language conversion + everything in Path A |
| **C — Upgrade + Serverless** | PySpark/SQL on 13.3 | PySpark/SQL on 16.4 serverless | DBR upgrade + serverless JSON/config/code changes |
| **D — Serverless Only** | PySpark/SQL on 15.4+ | Serverless (same or newer DBR) | Serverless JSON/config/code changes only |

Any path ending in serverless requires PySpark or SQL — Scala cannot run on serverless. Paths B and C are effectively compound (B = conversion then optionally serverless; C = upgrade then serverless).

### Two Execution Tracks

Every migration has changes in two places:

1. **Databricks-side** — notebook code, spark configs, table metadata. Genie Code can assess and assist with these.
2. **Repo-side (Azure DevOps)** — job JSON templates, CI/CD pipeline variables, PowerShell deployment scripts. These must be changed upstream in the repository.

## 2. Target Solution Architecture

This section describes the complete toolkit as if built from scratch — the full set of skills, prompts, and resources needed to support all four migration paths at scale.

### 2.1 Toolkit Manifest

```
serverless-migration-toolkit/
├── PLAN.md
├── skills/
│   ├── assessment/skill.md
│   ├── serverless_migration/skill.md
│   ├── data_compatibility/skill.md
│   ├── dbr_upgrade/skill.md
│   ├── scala_to_pyspark/skill.md
│   ├── conversion_validator/skill.md
│   └── conversion_report/skill.md
├── prompts/
│   ├── assess_databricks.txt
│   ├── assess_repo.txt
│   ├── migrate_to_serverless.txt
│   ├── validate_migration.txt
│   ├── prompt_scala_upgrade.txt
│   └── prompt_scala_to_pyspark_upgrade.txt
└── resources/
    ├── breaking_changes_13_to_16.md
    ├── serverless_blockers.md
    ├── serverless_known_issues.md
    ├── job_json_transformation.md
    ├── cicd_change_guide.md
    ├── supported_spark_configs.md
    └── spark_conf_changes.md
```

### 2.2 Skills — Detailed Feature Specifications

#### Assessment Skill

**Purpose:** The main entry point. Takes a manifest of jobs + prescribed outcomes, runs analysis, produces a per-job migration plan with batch summary.

**Features:**

- F1: Accept a manifest (job_id, prescribed_outcome)
    - Supported outcomes: `upgrade_only`, `convert_to_pyspark`, `upgrade_and_serverless`, `serverless_only`
- F2: Pull job config via Jobs API
    - Extract DBR version, language, cluster spec, init scripts, libraries, spark configs, task definitions
- F3: Validate feasibility
    - Flag impossible combinations:
        - Serverless prescribed for a Scala job without conversion
        - Streaming jobs
        - GPU/MLR workloads
- F4: Route each job to the correct analysis path based on prescribed outcome
    - Path A (upgrade_only): dbr_upgrade checks only
    - Path B (convert_to_pyspark): scala_to_pyspark assessment + dbr_upgrade checks
    - Path C (upgrade_and_serverless): dbr_upgrade checks + serverless_migration checks
    - Path D (serverless_only): serverless_migration checks only
- F5: Scan all notebook code referenced by the job
    - Use regex patterns from breaking_changes and serverless_blockers resources
    - Only scan for issues relevant to that job's migration path
- F6: Run data_compatibility checks on every output table written by the job
- F7: Classify each finding as Databricks-side change vs repo-side change
- F8: Produce structured per-job report
    - Severity-ranked findings with specific notebook/cell locations
    - Recommended ANSI-safe fix per occurrence
    - Table-level issues
    - Proposed action list
- F9: Produce batch summary
    - Counts by severity
    - Most common patterns across all jobs
    - Effort distribution
    - Repo change manifest (which job JSONs need transformation, what CI/CD variables to add)
- F10: Env-based catalog awareness
    - Recognize `USE CATALOG` with 2-part table names as valid Unity Catalog usage
    - Do not flag 2-part namespaces as non-UC

#### Serverless Migration Skill

**Purpose:** Complete reference for everything specific to the classic→serverless compute transition. Used by the assessment skill for checks and by developers as a how-to guide.

**Features:**

- F1: Job JSON transformation specification
    - Step-by-step with before/after templates:
        - Remove `job_clusters` section entirely
        - Replace `job_cluster_key` with `environment_key: "serverless_environment_v1"` on each task
        - Add `environments` block with `client: "4"` and requirements.txt path (`/Volumes/%env%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`)
        - Add `parameters` block (PATH_LANDING, PATH_DATALAKE) with `%env%` substitution
        - Add `"queue": {"enabled": true}`
        - Add `"performance_target": "PERFORMANCE_OPTIMIZED"`
- F2: Environment variable migration
    - `os.environ.get()` → `dbutils.widgets.get()` with code examples for notebooks
    - `sys.argv` / `argparse` for Python scripts
- F3: Unsupported operation catalog
    - Detection patterns and fix for each:
        - `.persist()` / `.cache()` / `CACHE TABLE` / `UNCACHE TABLE` → remove, rely on serverless autoscaling
        - `REFRESH TABLE` → remove, serverless auto-handles
        - `MSCK REPAIR TABLE` → remove
        - `REFRESH MATERIALIZED VIEW` / `CREATE MATERIALIZED VIEW` → must use SQL Warehouse
        - Global temp views → convert to session temp views or tables
        - RDD APIs (`sc.textFile`, `sc.parallelize`, `rdd.map`) → rewrite as DataFrame operations
        - Multithreading/parallelism → anti-pattern on serverless, use workflows/for-each tasks
- F4: Unsupported library catalog
    - Detection and workaround for each:
        - `com.crealytics.spark.excel` → pandas + openpyxl via local_disk0 → Volume → abfss
        - Wheel files → must be cp312 (Python 3.12), built on DBR 16.4
        - JARs in notebooks → not supported, must move to job tasks or rewrite in Python
- F5: Spark config migration map
    - Complete supported/unsupported list with replacement for each
    - References supported_spark_configs.md resource
- F6: ANSI-safe code fix patterns
    - For every ANSI-unsafe pattern, the specific fix:
        - `CAST(x AS INT)` → `TRY_CAST(x AS INT)`
        - `to_timestamp(col)` on invalid data → `try_to_timestamp(col)`
        - `col / divisor` → `TRY_DIVIDE(col, divisor)` or `CASE WHEN divisor = 0 THEN NULL`
        - `boolean_col = 1` → `boolean_col IS TRUE`
        - `.cast("int")` in PySpark → add `when/otherwise` null guard
        - Array index access → bounds check or `try_element_at`
        - Map key access → `try_element_at` or key existence check
- F7: `_metadata` column conflict detection
    - Check for row tracking enabled on tables
    - Cross-reference with code referencing `_metadata`
- F8: Schema inference issues
    - Flag `spark.createDataFrame()` on complex/nested data without explicit schema
    - Recommend explicit `StructType`
- F9: Performance considerations for serverless
    - Remove unnecessary `.count()` actions
    - Use `VACUUM LITE` instead of `VACUUM` for better performance
    - Use Liquid Clustering instead of ZORDER for new tables
    - Run `OPTIMIZE` and `ANALYZE TABLE ... COMPUTE STATISTICS` on external tables
    - Use `.first() is not None` instead of `.count() > 0` for existence checks
- F10: Known issues catalog
    - Indexed by error message pattern
    - Linked to resolution
    - References serverless_known_issues.md resource

#### Data Compatibility Skill

**Purpose:** Check all output tables for a job to identify data-level risks when moving to a new Spark version or serverless compute. Every output table must be checked — these are regulated healthcare pipelines.

**Features:**

- F1: Table type classification
    - `DESCRIBE DETAIL` to determine managed vs external
    - Flag external tables for:
        - No Predictive Optimization available
        - Need explicit OPTIMIZE, VACUUM, ANALYZE
        - May have older parquet files from legacy writers
- F2: Delta protocol version check
    - Extract `minReaderVersion`/`minWriterVersion` from `DESCRIBE DETAIL`
    - Flag tables where new DBR may auto-upgrade protocol on write, potentially breaking compatibility with older readers
- F3: Table properties audit
    - `SHOW TBLPROPERTIES` to check:
        - Row tracking status (creates `_metadata` column that can conflict with code)
        - Retention duration settings
        - Auto-optimization settings
        - Partition columns
- F4: Datetime/timestamp column analysis
    - For all timestamp/date columns:
        - Sample values looking for pre-1582 dates (Proleptic Gregorian calendar edge case)
        - Check for invalid date strings that would fail ANSI parsing (e.g., '00000000', '9999-99-99')
        - Check for dates stored as strings that get cast in code
- F5: BOOLEAN column detection
    - Identify BOOLEAN columns that might be compared to INT literals in notebook code
    - Cross-reference with code scan
- F6: Parquet file metadata check
    - Via table history or file listing, determine writer version
    - Files written by Spark 2.x may need rebase mode for datetime handling
- F7: Partition scheme analysis
    - Flag tables with ZORDER or partitioning schemes that may interact differently with AQE defaults on new DBR
- F8: File layout health
    - Check file count and average file size
    - Flag tables with many small files that would benefit from OPTIMIZE before migration
- F9: Output risk classification
    - Per table, produce HIGH/MEDIUM/LOW rating
    - Include specific findings and recommended pre-migration actions

#### DBR Upgrade Skill (13.3 → 16.4)

**Purpose:** Complete reference for upgrading Databricks Runtime from 13.3 LTS to 16.4 LTS. Applies to both Scala and PySpark code.

**Features:**

- F1: ANSI compliance fix patterns
    - For every ANSI-unsafe pattern, the specific ANSI-safe code change
    - Includes try_cast, try_divide, null guards, IS TRUE
    - Never recommend `spark.sql.ansi.enabled = false` as permanent solution
- F2: Deprecated config detection and removal
    - Configs removed between 13.3 and 16.4
    - Replacement or removal action for each
- F3: Changed default detection
    - Configs where default value changed
    - Key changes: `spark.sql.ansi.enabled` now true in 16.4, `spark.sql.sources.default` now delta
- F4: Deprecated API detection
    - SQL functions, DataFrame methods, and UDF patterns that changed
- F5: New features available in 16.4
    - Liquid Clustering, Predictive I/O, IDENTIFIER() clause, Variant type, default column values
    - For awareness, not required adoption
- F6: Delta Lake changes
    - Protocol version impact
    - Deletion vectors
    - Column mapping defaults
- F7: PySpark-specific changes
    - Arrow-based UDF defaults
    - pandas_udf improvements
- F8: Scala-specific changes
    - Scala version bump
    - Dataset API deprecations
- F9: Structured regex patterns for automated scanning
    - Reference to breaking_changes_13_to_16.md resource file
    - Line-scannable patterns organized by severity
- F10: Scan checklist
    - Searchable pattern table for manual or automated code review

#### Scala to PySpark Skill

**Purpose:** Complete reference for converting Databricks Scala notebooks to PySpark.

**Features (already complete):**

- F1: Import translations (org.apache.spark → pyspark with F. prefix convention)
- F2: Column reference syntax ($"col" → F.col(), .as() → .alias(), === → ==)
- F3: Case class → StructType / dataclass conversion
- F4: Pattern matching → if/elif or dictionary
- F5: Option/Some/None → Python None handling
- F6: Try/Success/Failure → try/except
- F7: UDF conversion with explicit return types and None handling
- F8: Array/map access sugar (Scala apply → Python indexing/getItem/element_at)
- F9: Boolean operator precedence (& and | require parentheses in PySpark)
- F10: Numeric precision differences (banker's rounding, integer division)
- F11: Date parsing edge cases (SimpleDateFormat lenient mode vs strptime)
- F12: Silent data difference patterns (null propagation, regex escaping, empty collections)
- F13: Non-determinism warnings (sort order, dropDuplicates, HashMap iteration, float aggregation)
- F14: Comprehensive conversion checklist

#### Conversion Validator Skill

**Purpose:** Validate that migrated code produces output identical to the original pipeline. 14-check framework organized from fast/cheap to slow/thorough.

**Features (existing + new):**

- F1: Schema comparison — column names, types, nullability
- F2: Row count comparison — exact match + orphan row detection
- F3–F5: Statistical checks — null counts, aggregates, distinct value counts
- F6–F7: Row-level checks — full data comparison, date deep dive
- F8–F12: Semantic checks — null confusion, type coercion, rounding, UDF behavior
- F13–F14: Edge cases and non-determinism
- F15 (NEW): Serverless compute verification
    - Confirm job ran on serverless via run details API
- F16 (NEW): Config compliance check
    - Verify no unsupported spark configs were set during run
- F17 (NEW): Environment key verification
    - Confirm environment_key present on all tasks
- F18 (NEW): Performance comparison
    - Classic vs serverless runtime and DBU cost from system tables

#### Conversion Report Skill

**Purpose:** Generate audit trail documenting every code change and its rationale.

**Features (existing + new):**

- F1: Executive summary with change counts by category
- F2: Notebook-by-notebook diff with category, risk, and behavioral impact per change
- F3: ANSI compliance audit section
- F4: UDF conversion detail section
- F5: Date/timestamp conversion detail section
- F6: Risk summary table ranked by severity
- F7 (NEW): Serverless-specific change section
    - JSON transformation
    - Env var migration
    - Config removals
- F8 (NEW): Library replacement section
    - Documenting spark.excel → pandas+openpyxl and similar
- F9 (NEW): Repo-side change manifest
    - What was changed in the Azure DevOps pipeline

### 2.3 Prompts

| Prompt | Purpose | Execution Context |
|--------|---------|-------------------|
| **assess_databricks.txt** | Run assessment against live workspace — pull job configs, scan notebooks, check tables | Genie Code in Databricks |
| **assess_repo.txt** | Guide repo-level assessment — scan job JSON templates, identify CI/CD variable gaps | Manual or local tooling against Azure DevOps repo |
| **migrate_to_serverless.txt** | Execute the Databricks-side migration for a single job — apply code fixes, update configs | Genie Code in Databricks |
| **validate_migration.txt** | Run full validation suite post-migration — all output tables, all checks | Genie Code in Databricks |
| **prompt_scala_upgrade.txt** | Upgrade a Scala pipeline from 13.3→16.4 (generalized) | Genie Code in Databricks |
| **prompt_scala_to_pyspark_upgrade.txt** | Convert Scala→PySpark + upgrade to 16.4 (generalized) | Genie Code in Databricks |

### 2.4 Resources

| Resource | Purpose | Contents |
|----------|---------|----------|
| **breaking_changes_13_to_16.md** | Automated code scanning | Structured regex patterns for every breaking change between DBR 13.3 and 16.4, organized by severity (Critical/High/Medium) with detection patterns and fix references |
| **serverless_blockers.md** | Eligibility screening | Hard blockers (Scala, R, JARs, GPU, streaming, non-UC) and soft blockers (init scripts, unsupported configs, legacy libraries) with resolution path for each |
| **serverless_known_issues.md** | Proactive issue detection | Catalog of 35+ real-world issues from Molina's migration, indexed by error message pattern, with root cause and proven resolution for each |
| **job_json_transformation.md** | Repo-side reference | Complete before/after job JSON with every change annotated. Includes `%env%` variable substitution pattern and environment block template |
| **cicd_change_guide.md** | DevOps team reference | Step-by-step for Azure DevOps pipeline changes: add `%env%` variable (additive only), update job JSON templates, deploy via existing scripts |
| **supported_spark_configs.md** | Config migration reference | Complete list of supported serverless configs with defaults, unsupported configs with replacement/removal action, and cross-reference to ANSI-safe code fixes |
| **spark_conf_changes.md** | Config migration reference | Configs removed, renamed, or with changed defaults between 13.3 and 16.4, with recommended legacy compatibility flags |

## 3. What Exists Today

### Current Skills (in serverless-migration-toolkit/skills/)

| Skill | Coverage | Quality | Status vs Target |
|-------|----------|---------|-----------------|
| **dbr_upgrade** (13.3→16.4) | ANSI compliance fixes, deprecated configs, new features | Strong — detailed fix patterns with before/after for SQL, Scala, PySpark | Missing F9 (regex resource file) and F10 (scan checklist). No serverless awareness. |
| **scala_to_pyspark** | Full language conversion guide | Excellent — covers every edge case including null propagation, operator precedence, numeric precision, date parsing, silent data differences | Complete. All features (F1–F14) implemented. No changes needed. |
| **conversion_validator** | 14-check validation framework | Strong — organized by cost, covers schema/row/statistical/semantic checks | Has F1–F14. Missing F15–F18 (serverless-specific checks). |
| **conversion_report** | Audit trail for code changes | Good — categories, risk levels, UDF detail sections | Has F1–F6. Missing F7–F9 (serverless change sections). |

### Current Prompts (in serverless-migration-toolkit/prompts/)

| Prompt | Purpose | Status vs Target |
|--------|---------|-----------------|
| **prompt_scala_upgrade.txt** | Upgrade Scala pipeline 13.3→16.4 | Exists but scoped to demo pipeline, not generalized for arbitrary jobs. |
| **prompt_scala_to_pyspark_upgrade.txt** | Convert Scala→PySpark + upgrade to 16.4 | Exists but scoped to demo pipeline, not generalized. |

### External Assets

| Asset | Location | Usefulness |
|-------|----------|-----------|
| **dbr-migration-audit** skill + prompt | ~/Documents/repos/agent_skills/dbr-migration-audit/ | Built for Bright Health EOL (7.3/9.1→14.3). Wrong version range but useful patterns for the assessment skill architecture. |
| **serverless-notebook-audit** skill | ~/Downloads/serverless_migration_notebook_audit_skill.md | Covers ~60% of serverless code checks. Has flaws: false-positive on 2-part namespace, recommends ansi.enabled=false, missing issue log patterns, no job/table analysis, no migration path routing, no structured output. Starting point for the assessment skill. |
| **Serverless SOP** | Google Doc (1xQLRG7V...) | Good coverage of eligibility, JSON changes, env vars, spark configs. Source material for serverless_migration skill and multiple resources. |
| **Issue Log** | ~/Downloads/Serverless Migration_Issue Log (1).xlsx | 35+ real-world issues with resolutions. Critical input for serverless_known_issues.md resource. |

## 4. Gap Assessment

The delta between Section 2 (target) and Section 3 (current state).

### Gap 1: Assessment Skill — Does Not Exist

None of the target features (F1–F10) exist. The external notebook audit skill is a partial starting point (~60% of code-level checks) but needs major rework:

- **Missing capabilities:**
    - Manifest-driven routing (F1, F3, F4)
    - Job-level config analysis (F2)
    - Table/data analysis integration (F6)
    - Databricks-side vs repo-side classification (F7)
    - Structured output format (F8, F9)
- **Bugs to fix:**
    - Env-based catalog false positive (F10) — flags 2-part namespaces as non-UC
    - ANSI recommendation — should be ANSI-safe fixes, not ansi.enabled=false
- **Missing detection patterns:**
    - 15+ patterns from the issue log not covered (com.crealytics.spark.excel, MSCK REPAIR TABLE, _metadata conflicts, schema inference, multithreading, Delta config removals, etc.)

### Gap 2: Serverless Migration Skill — Does Not Exist

None of the target features (F1–F10) exist. Must be built from scratch using:

- Before/after job JSON samples (collected)
- SOP document (collected)
- Issue log patterns and resolutions (collected)
- Spark config supported list from Microsoft docs (referenced in SOP)

### Gap 3: Data Compatibility Skill — Does Not Exist

None of the target features (F1–F9) exist. No existing skill or asset covers table-level analysis. Must be built from scratch. Critical because all output tables must be checked — regulated healthcare pipelines with a mix of managed and external Delta tables.

### Gap 4: Resources — None Exist

All 7 resources need to be created:

| Resource | Source Material | Status |
|----------|----------------|--------|
| breaking_changes_13_to_16.md | dbr_upgrade skill prose + Databricks docs | Need to extract regex patterns from prose |
| serverless_blockers.md | SOP eligibility section + issue log | Have source material |
| serverless_known_issues.md | Issue log (35+ issues) | Have source material |
| job_json_transformation.md | Before/after JSON samples | Have source material |
| cicd_change_guide.md | PowerShell script analysis + pipeline layout | Have partial material, **pending: exact %env% variable name** |
| supported_spark_configs.md | SOP Section 7 + Microsoft docs | Have source material |
| spark_conf_changes.md | Old audit resources + dbr_upgrade skill | Have source material, needs version range update |

### Gap 5: Existing Skills Need Updates

| Skill | What's Missing |
|-------|---------------|
| **dbr_upgrade** | F9 (regex resource file for 13.3→16.4), F10 (scan checklist). ANSI section should emphasize ANSI-safe fixes over the flag. |
| **conversion_validator** | F15–F18 (serverless compute verification, config compliance, environment_key check, performance comparison) |
| **conversion_report** | F7–F9 (serverless JSON changes, library replacements, repo-side change manifest) |

### Gap 6: Prompts Need Updates or Creation

| Prompt | Status |
|--------|--------|
| assess_databricks.txt | Does not exist |
| assess_repo.txt | Does not exist |
| migrate_to_serverless.txt | Does not exist |
| validate_migration.txt | Does not exist |
| prompt_scala_upgrade.txt | Exists but needs generalization |
| prompt_scala_to_pyspark_upgrade.txt | Exists but needs generalization |

## 5. Workflow Design

### Phase 1: Assessment (per batch of 12–100 jobs)

```
                    ┌──────────────────┐
                    │  Job Manifest     │
                    │  (ID + outcome)   │
                    └────────┬─────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
    ┌───────────▼──────────┐  ┌──────────▼───────────┐
    │  Databricks Assessment│  │  Repo Assessment      │
    │  (Genie Code)         │  │  (manual / scripted)  │
    │                       │  │                       │
    │  - Job configs (API)  │  │  - Job JSON templates │
    │  - Notebook code scan │  │  - CI/CD variables    │
    │  - Table metadata     │  │  - PowerShell scripts │
    │  - Data spot checks   │  │  - Identify %env% gap │
    └───────────┬──────────┘  └──────────┬───────────┘
                │                         │
                └────────────┬────────────┘
                             │
                    ┌────────▼─────────┐
                    │  Assessment Report │
                    │  (per-job + batch) │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Review with team  │
                    │  Confirm/adjust    │
                    │  prescribed paths  │
                    └──────────────────┘
```

### Phase 2: Execution (per job)

```
    ┌─────────────────────────────────────────────┐
    │              Repo-Side (Azure DevOps)         │
    │                                               │
    │  1. Update job JSON template                  │
    │     - Remove job_clusters                     │
    │     - Add environments + environment_key      │
    │     - Add parameters                          │
    │     - Add queue + performance_target          │
    │  2. Add %env% to pipeline variables           │
    │     (additive only — keep existing vars)      │
    │  3. Commit + deploy via existing CI/CD        │
    └─────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────┐
    │          Databricks-Side (Genie Code)         │
    │                                               │
    │  1. Apply ANSI-safe code fixes                │
    │     (try_cast, try_divide, IS TRUE, etc.)     │
    │  2. Replace os.environ.get → widgets.get      │
    │  3. Remove unsupported operations             │
    │     (REFRESH TABLE, MSCK REPAIR, .persist())  │
    │  4. Remove/replace unsupported configs        │
    │  5. Replace unsupported libraries             │
    │     (spark.excel → pandas+openpyxl)           │
    │  6. Scala→PySpark conversion (if path B)      │
    └─────────────────────────────────────────────┘
```

### Phase 3: Validation (per job)

```
    ┌─────────────────────────────────────────────┐
    │          Validation (Genie Code)              │
    │                                               │
    │  1. Run converted job on serverless           │
    │  2. Run conversion_validator against all      │
    │     output tables (every table, full checks)  │
    │  3. Compare runtime and DBU cost              │
    │  4. Generate conversion_report                │
    │  5. Flag any tables needing manual review     │
    └─────────────────────────────────────────────┘
```

## 6. Design Principles

1. **ANSI-safe fixes, not blanket flags.** Never recommend `spark.sql.ansi.enabled = false` as a permanent solution. Always generate the specific ANSI-safe code fix (try_cast, try_divide, IS TRUE, null guards). The flag may be used as a temporary escape hatch during validation only.

2. **Every output table gets checked.** These are regulated healthcare pipelines with a mix of managed and external Delta tables. No shortcuts on data validation.

3. **Additive changes to CI/CD.** Do not remove existing pipeline variables or modify deployment script structure. Only add the new `%env%` variable and update job JSON templates.

4. **Managed vs external table awareness.** External tables need explicit OPTIMIZE, VACUUM, ANALYZE. No Predictive Optimization. The assessment must flag these and recommend pre-migration actions.

5. **Two-track execution.** Clearly separate what happens in Databricks (Genie Code can help) from what happens in the repo (manual or scripted). Never conflate the two.

6. **Env-based catalog awareness.** Molina uses `USE CATALOG {{env}}_catalog` with 2-part table names. This is correct Unity Catalog usage — do not flag 2-part namespaces as "non-Unity Catalog."

7. **Batch-oriented design.** Everything must work for 12–100 jobs at a time, not just single jobs.

## 7. Build Order

Suggested sequence — resources first (they're referenced by skills), then skills, then prompts.

| Order | Item | Type | Dependencies |
|-------|------|------|-------------|
| 1 | `resources/serverless_known_issues.md` | Resource | Issue log (have it) |
| 2 | `resources/supported_spark_configs.md` | Resource | SOP (have it) |
| 3 | `resources/job_json_transformation.md` | Resource | Before/after JSON (have it) |
| 4 | `resources/breaking_changes_13_to_16.md` | Resource | dbr_upgrade skill (have it) |
| 5 | `resources/serverless_blockers.md` | Resource | SOP + issue log (have it) |
| 6 | `resources/cicd_change_guide.md` | Resource | Pipeline screenshot + **pending: exact %env% variable name** |
| 7 | `resources/spark_conf_changes.md` | Resource | Old audit resources (have it) |
| 8 | `skills/serverless_migration/skill.md` | Skill | Resources 1–7 |
| 9 | `skills/data_compatibility/skill.md` | Skill | None |
| 10 | `skills/assessment/skill.md` | Skill | All resources + skills 8–9 + existing skills |
| 11 | Update `skills/dbr_upgrade/skill.md` | Update | Resource 4 |
| 12 | Update `skills/conversion_validator/skill.md` | Update | Skill 8 |
| 13 | Update `skills/conversion_report/skill.md` | Update | Skill 8 |
| 14 | `prompts/assess_databricks.txt` | Prompt | Skills 8–10 |
| 15 | `prompts/assess_repo.txt` | Prompt | Resource 6 |
| 16 | `prompts/migrate_to_serverless.txt` | Prompt | All skills |
| 17 | `prompts/validate_migration.txt` | Prompt | Updated validator |

## 8. Open Items

| # | Item | Status | Owner |
|---|------|--------|-------|
| 1 | Exact DevOps pipeline variable name for environment (dev/uat/prod) | Maggie confirming today | Maggie |
| 2 | ~~Confirm `performance_target` vs `performance_optimized`~~ — **Resolved:** use `"performance_target": "PERFORMANCE_OPTIMIZED"` | Done | Maggie |
| 3 | Any additional known issues beyond the 35 in the issue log | If available | Maggie |
| 4 | Sample CI/CD pipeline YAML (if accessible) showing variable definitions | Nice to have | Maggie |
