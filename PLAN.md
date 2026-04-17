# Serverless & DBR Migration Toolkit

**Date:** 2026-04-17
**Author:** Maggie Barrett
**Status:** Built — ready for execution
**Repo:** https://github.com/maggie-barr-db/serverless-migration-toolkit (dev branch)

---

## 1. Scope

~5,000 Databricks jobs at Molina Healthcare need to be migrated across four paths. Jobs will be processed in batches of 12–100. A manifest will specify each job's prescribed outcome.

### Migration Paths

| Path | From | To | What Changes |
|------|------|----|-------------|
| **A — DBR Upgrade Only** | Scala on 13.3 | Scala on 16.4 | Spark configs, ANSI-safe code fixes, deprecated API updates |
| **B — Convert + Upgrade** | Scala on 13.3 | PySpark on Serverless | Language conversion + DBR upgrade + serverless compute changes |
| **C — Upgrade + Serverless** | PySpark/SQL on 13.3 | PySpark/SQL on Serverless | DBR upgrade + serverless JSON/config/code changes |
| **D — SQL to DBSQL** | SQL-only or PySpark-SQL | DBSQL Serverless Current Channel | Notebook format conversion + SQL dialect changes |

Any path ending in serverless requires PySpark or SQL — Scala cannot run on serverless notebooks (no GA serverless option for Scala). Paths B and C are compound migrations.

### Two Execution Tracks

Every migration has changes in two places:

1. **Databricks-side** — notebook code, spark configs, table metadata. Genie Code assesses and assists with these.
2. **Repo-side (Azure DevOps)** — job JSON templates, CI/CD pipeline variables, PowerShell deployment scripts. These must be changed in the repository by a developer (manually or via VS Code + Copilot).

---

## 2. Toolkit Contents

### 2.1 Resources (19 files)

**Scenario Guides:**

| # | File | Purpose |
|---|------|---------|
| 01 | `scala-13.3-to-scala-16.4.md` | Path A — Scala DBR upgrade. ANSI patterns, deprecated configs, Delta changes, ML runtime, scan checklist. |
| 02 | `scala-13.3-to-pyspark-serverless.md` | Path B — Language + DBR + compute. Order of operations, all serverless restrictions. |
| 03 | `pyspark-sql-13.3-to-pyspark-sql-serverless.md` | Path C — PySpark/SQL to serverless. ANSI, Python 3.12 changes, config migration. |
| 04 | `sql-to-dbsql-serverless.md` | Path D — SQL to DBSQL Serverless. Notebook conversion, SQL dialect, parameters. |

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

### 2.2 Skills — Detailed Feature Specifications

#### DBR Upgrade Skill (13.3 → 16.4)

**Purpose:** Complete reference for upgrading Databricks Runtime from 13.3 LTS to 16.4 LTS. Applies to both Scala and PySpark code.

- F1: ANSI compliance fix patterns — TRY_CAST, TRY_DIVIDE, null guards, IS TRUE for every ANSI-unsafe pattern
- F2: Deprecated config detection and removal — configs removed between 13.3 and 16.4 with replacements
- F3: Changed default detection — configs where default values changed (ANSI enabled, sources.default)
- F4: Deprecated API detection — SQL functions, DataFrame methods, UDF patterns that changed
- F5: New features available in 16.4 — Liquid Clustering, Predictive I/O, IDENTIFIER(), default column values
- F6: Delta Lake changes — protocol versions, deletion vectors, column mapping defaults
- F7: PySpark-specific changes — Arrow-based UDF defaults, pandas_udf improvements
- F8: Scala-specific changes — Scala 2.12.18 compatibility, Dataset API deprecations
- F9: Structured regex patterns for automated scanning — references `resources/15-breaking-changes-13-to-16-regex.md`
- F10: Scan checklist — 4-pass severity-organized checklist (Critical → High → Medium → Low)

#### Scala to PySpark Skill

**Purpose:** Complete reference for converting Databricks Scala notebooks to PySpark.

- F1: Import translations — org.apache.spark → pyspark with F. prefix convention
- F2: Column reference syntax — $"col" → F.col(), .as() → .alias(), === → ==
- F3: Case class → StructType / dataclass conversion
- F4: Pattern matching → if/elif or dictionary
- F5: Option/Some/None → Python None handling
- F6: Try/Success/Failure → try/except
- F7: UDF conversion — explicit return types, None handling for every code path
- F8: Array/map access sugar — Scala apply → Python indexing/getItem/element_at
- F9: Boolean operator precedence — & and | require parentheses in PySpark
- F10: Numeric precision differences — banker's rounding, integer division
- F11: Date parsing edge cases — SimpleDateFormat lenient mode vs strptime
- F12: Silent data difference patterns — null propagation, regex escaping, empty collections
- F13: Non-determinism warnings — sort order, dropDuplicates, HashMap iteration, float aggregation
- F14: Comprehensive conversion checklist

#### Conversion Validator Skill

**Purpose:** Validate that migrated code produces output identical to the original pipeline. 18-check framework organized from fast/cheap to slow/thorough.

- F1: Schema comparison — column names, types, nullability
- F2: Row count comparison — exact match + orphan row detection
- F3–F5: Statistical checks — null counts, aggregates, distinct value counts
- F6–F7: Row-level checks — full data comparison, date/timestamp deep dive (7 sub-checks)
- F8–F12: Semantic checks — null↔empty confusion, null↔zero confusion, type coercion, rounding, UDF behavior
- F13–F14: Edge cases and non-determinism detection
- F15: Serverless compute verification — confirm job ran on serverless via run details API
- F16: Config compliance check — verify no unsupported spark configs were set during run
- F17: Environment key verification — confirm environment_key on all tasks, no unresolved placeholders
- F18: Performance comparison — classic vs serverless runtime with regression flagging (>2x threshold)

#### Conversion Report Skill

**Purpose:** Generate audit trail documenting every code change and its rationale.

- F1: Executive summary — change counts by category (syntax, API, ANSI, runtime, UDF, behavioral)
- F2: Notebook-by-notebook diff — every change with location, category, risk, behavioral impact
- F3: ANSI compliance audit section — every ANSI-sensitive pattern and how it was fixed
- F4: UDF conversion detail — side-by-side comparison with null handling analysis
- F5: Date/timestamp conversion detail — timezone, format, precision analysis
- F6: Risk summary table — ranked by severity with mitigation steps
- F7: Serverless-specific change section — job JSON transformation, env var migration, config removals
- F8: Library replacement section — documenting each replaced library with before/after code
- F9: Repo-side change manifest — what was changed in Azure DevOps (job JSON, PowerShell script, variable groups)

### 2.3 Prompts (6 files)

| Prompt | Runs In | Purpose |
|--------|---------|---------|
| `assess_databricks.txt` | Genie Code | Assessment + change manifest for single job or batch |
| `assess_repo.txt` | VS Code/Copilot or manual | Repo-side checklist for Azure DevOps artifacts |
| `migrate_to_serverless.txt` | Genie Code | Apply code changes to staging copies in workspace |
| `validate_migration.txt` | Genie Code | Full validation suite with structured pass/fail report |
| `prompt_scala_upgrade.txt` | Genie Code | Upgrade a Scala pipeline from 13.3→16.4 |
| `prompt_scala_to_pyspark_upgrade.txt` | Genie Code | Convert Scala→PySpark + upgrade to 16.4 |

---

## 3. Execution Workflow

### Phase 1: Assessment (per batch of 12–100 jobs)

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
    │  - Job configs (API)  │  │  - Job JSON templates │
    │  - Notebook code scan │  │  - PowerShell scripts │
    │  - Table metadata     │  │  - Variable groups    │
    │  - Data spot checks   │  │  - %env_name% fix     │
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
3. Replace os.environ.get → dbutils.widgets.get
4. Remove unsupported operations (REFRESH TABLE, MSCK REPAIR, .persist())
5. Remove/replace unsupported configs
6. Replace unsupported libraries (spark.excel → pandas+openpyxl)
7. Scala→PySpark conversion (Path B only)
8. Produce change manifest for developer

**Repo-side (Developer in Azure DevOps):**
1. Apply change manifest to repo source files (preserving %placeholder% tokens)
2. Update job JSON templates (remove job_clusters, add environments block)
3. Add %env_name% replacement to PowerShell deploy script
4. Commit to feature branch

### Phase 3: Validation

```
Gate 1: UAT Staging (Genie Code changes in workspace)
  → Run migrated job from staging copies
  → Validate output against baseline

Gate 2: Repo Commit (Developer applies changes)
  → Feature branch committed with parameterized source files

Gate 3: CI/CD Round-Trip (Confirms repo version works)
  → CI/CD deploys feature branch to UAT
  → Re-run validation (confirms CI/CD version matches staging)

Gate 4: Production Cutover
  → PR merged → CI/CD deploys to PROD
  → First run monitored, post-deployment validation
```

**Key constraint:** Genie Code operates in Databricks only. Workspace notebooks have hardcoded environment values (from CI/CD deployment). The path back to the repo is always through the **change manifest** applied to parameterized source files — never by exporting workspace notebooks.

---

## 4. Design Principles

1. **ANSI-safe fixes, not blanket flags.** Never recommend `spark.sql.ansi.enabled = false`. Always generate the specific ANSI-safe code fix. ANSI mode is mandatory on serverless and cannot be disabled.

2. **Every output table gets checked.** Regulated healthcare pipelines. No shortcuts on data validation.

3. **Additive changes to CI/CD.** Do not remove existing pipeline variables or modify deployment script structure. Only add the new `%env_name%` replacement and update job JSON templates.

4. **Managed vs external table awareness.** External tables need explicit OPTIMIZE, VACUUM, ANALYZE. No Predictive Optimization.

5. **Two-track execution.** Databricks-side (Genie Code) and repo-side (developer/VS Code). Never conflate.

6. **Env-based catalog awareness.** Molina uses `USE CATALOG {{env}}_catalog` with 2-part table names. Do not flag as non-UC. ADLS `abfss://` paths are registered in Unity Catalog — do not flag as non-UC.

7. **Batch-oriented design.** Everything works for 12–100 jobs at a time.

8. **GA features only.** Do not recommend Public Preview features (VACUUM LITE, serverless JAR tasks). Only recommend GA features.

9. **Change manifests, not file exports.** Genie Code produces change manifests (cell/line diffs). Developers apply these to repo source files. Never export workspace notebooks back to the repo (they contain hardcoded environment values).

10. **UAT-first workflow.** All migration work happens in UAT. Changes promote to PROD only after CI/CD round-trip validation passes.

