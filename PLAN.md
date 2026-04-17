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

### 2.2 Skills (4 files, all updated)

| Skill | Features | Status |
|-------|----------|--------|
| `dbr_upgrade/skill.md` | F1-F10: ANSI fixes, deprecated configs, new features, regex patterns, scan checklist | Complete |
| `scala_to_pyspark/skill.md` | F1-F14: Full language conversion with edge cases, null handling, precision | Complete |
| `conversion_validator/skill.md` | F1-F18: 14 original checks + 4 serverless checks (compute verify, config compliance, env key, performance) | Complete |
| `conversion_report/skill.md` | F1-F9: Original sections + serverless changes, library replacements, repo-side manifest | Complete |

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

---

## 5. Resolved Items

| # | Item | Resolution |
|---|------|-----------|
| 1 | DevOps pipeline variable name for environment | `env_name` (exists in variable groups as `uat`, `dev`, `prod`) |
| 2 | performance_target vs performance_optimized | Use `"performance_optimized": true` in job JSON |
| 3 | %env_name% not being replaced in deploy script | Add `$bodyJson.Replace("%env_name%","$env_name")` to PowerShell script |
| 4 | Job JSON template with parameters | PATH_LANDING and PATH_DATALAKE added with %env_name% substitution |
| 5 | SOP incorporation into skills | All SOP recommendations incorporated (resource 19). ANSI=false overridden with ANSI-safe fixes. |
| 6 | Customer feedback on non-UC flagging | Sanjay confirmed ADLS paths are UC-registered. Do not flag. |
| 7 | Non-GA feature scrub | VACUUM LITE and serverless JAR tasks removed. GA features only. |
| 8 | Testing workflow for CI/CD one-way deployment | Change manifest approach — Genie Code prescribes, developer applies to repo. |
