I need to migrate a Databricks job. I will provide the job ID, the migration path, and which changes to apply.

## Input

- **Job ID**: The Databricks job to migrate
- **Path**: One of:
  - **A** - Scala 13.3 to Scala 16.4 (DBR upgrade only, keep Scala)
  - **B** - Scala 13.3 to PySpark on Serverless (language conversion + DBR upgrade + serverless)
  - **C** - PySpark/SQL 13.3 to PySpark/SQL on Serverless (DBR upgrade + serverless)
  - **D** - SQL-only to DBSQL Serverless Current Channel (notebook format conversion)
- **Changes to apply**: One of:
  - "all" - apply all findings from the assessment
  - "critical and high" - apply CRITICAL + HIGH severity only
  - "critical only" - apply only CRITICAL
  - Specific finding IDs: "C1, C2, C3, H1, H2"
  - "all except X, Y" - apply everything except specific items
- **Assessment report** (optional): I may paste or reference a previously generated assessment report. If not provided, run the assessment first using resource 19 (comprehensive-job-assessment.md).

## Step 1: Confirm Inputs

Print back what you understood:
```
Migration Plan:
  Job: <job_name> (<job_id>)
  Path: <path letter> - <path description>
  Changes: <what to apply>
  Assessment: <provided / will run now>
```

Wait for confirmation before proceeding.

## Step 2: Record Baseline

Record current Delta table versions for all output tables:
```sql
DESCRIBE HISTORY <schema>.<table> LIMIT 1;
```

## Step 3: Create Staging Copies

Copy all notebooks to: `/Workspace/Migration/staging/<job_name>/`
Do NOT modify the originals.

## Step 4: Apply Changes (Path-Specific)

Route to the correct migration workflow based on the path:

### If Path A (Scala to Scala 16.4):

Apply only approved changes. Key references:
- Skill: `skills/dbr_upgrade/skill.md`
- Resource: `resources/01-scala-13.3-to-scala-16.4.md`
- ANSI patterns: `resources/07-ansi-compliance-reference.md`

Changes to apply (if approved):
1. ANSI compliance fixes (TRY_CAST, TRY_DIVIDE, IS TRUE, try_to_timestamp, bounds checks)
2. Remove deprecated configs
3. Remove unsupported operations (REFRESH TABLE, MSCK REPAIR TABLE)
4. Remove .cache() / .persist()
5. Keep Scala - do NOT change the language
6. Do NOT set spark.sql.ansi.enabled = false

### If Path B (Scala to PySpark Serverless):

Apply in this order - language conversion first, then DBR fixes, then serverless fixes:

**Phase 1: Language Conversion**
- Skill: `skills/scala_to_pyspark/skill.md`
- Resource: `resources/02-scala-13.3-to-pyspark-serverless.md`
- Convert all Scala syntax to PySpark ($"col" to F.col(), case classes to StructType, UDFs, pattern matching, Option/Try, etc.)
- Follow every rule in the scala_to_pyspark conversion checklist

**Phase 2: ANSI/DBR Fixes**
- Skill: `skills/dbr_upgrade/skill.md`
- Resource: `resources/07-ansi-compliance-reference.md`
- Apply ANSI-safe fixes to the PySpark code

**Phase 3: Serverless Fixes**
- Resource: `resources/03-pyspark-sql-13.3-to-pyspark-sql-serverless.md` (use serverless sections)
- Resource: `resources/05-spark-config-classic-to-serverless.md`
- Remove unsupported operations (.persist, REFRESH TABLE, MSCK REPAIR, global temp views)
- Remove unsupported Spark configs
- Replace os.environ.get() with dbutils.widgets.get()
- Replace com.crealytics.spark.excel with pandas + openpyxl (if applicable)
- Do NOT set spark.sql.ansi.enabled = false

### If Path C (PySpark/SQL to Serverless):

Apply DBR fixes and serverless fixes together:
- Resource: `resources/03-pyspark-sql-13.3-to-pyspark-sql-serverless.md`
- Resource: `resources/07-ansi-compliance-reference.md`
- Resource: `resources/05-spark-config-classic-to-serverless.md`

Changes to apply (if approved):
1. ANSI compliance fixes
2. Remove unsupported operations (.persist, REFRESH TABLE, MSCK REPAIR, materialized views)
3. Remove unsupported Spark configs
4. Replace os.environ.get() with dbutils.widgets.get()
5. Remove %pip install (move to requirements.txt)
6. Remove ThreadPoolExecutor parallelism (recommend workflow for-each tasks)
7. Replace .withColumn() chains with .withColumns() if >20
8. Fix /tmp paths to /local_disk0/tmp
9. Fix spark.createDataFrame() without schema
10. Do NOT set spark.sql.ansi.enabled = false

### If Path D (SQL to DBSQL):

Convert notebook format:
- Resource: `resources/04-sql-to-dbsql-serverless.md`

Changes to apply:
1. Extract SQL from spark.sql() calls into direct SQL cells
2. Convert Python f-string interpolation to SQL parameters
3. Remove display() wrappers
4. Convert dbutils.widgets to SQL parameter syntax
5. Apply ANSI fixes to all SQL
6. Remove CACHE TABLE, REFRESH TABLE
7. Remove SET spark.sql.ansi.enabled = false
8. Convert simple Python UDFs to SQL UDFs where possible
9. Note: %run dependencies need to be restructured as workflow task dependencies

## Step 5: Generate Change Manifest

After applying changes, produce the manifest:

```
CHANGE MANIFEST: <job_name>
Path: <path>
Applied: <count> of <total> findings

Notebook: <name>
  Cell <N>, Line <N>:
    BEFORE: <exact original code>
    AFTER:  <exact new code>
    Reason: <finding ID> - <description>
    Status: APPLIED / SKIPPED (not approved)
```

## Step 6: Run and Validate

Run the migrated pipeline:
- Path A: Run on a DBR 16.4 LTS cluster
- Path B/C: Run on serverless compute
- Path D: Run on a DBSQL Serverless warehouse

Then apply the `conversion_validator` skill (skills/conversion_validator/skill.md):
- Compare output tables against baseline versions (time travel)
- Run all applicable validation checks (1-14, plus 15-18 for serverless paths)
- Flag any data differences

## Step 7: Generate Report

Apply the `conversion_report` skill (skills/conversion_report/skill.md):
1. Executive summary with change counts
2. Notebook-by-notebook diff
3. ANSI compliance audit
4. Risk summary
5. Validation results

## Step 8: Print Summary

```
MIGRATION SUMMARY: <job_name>
Path: <path>
Findings: <applied>/<total> applied, <skipped> skipped
Validation: <pass/fail> (<checks passed>/<total checks>)

Applied Changes:
  CRITICAL: <count>
  HIGH: <count>
  MEDIUM: <count>
  LOW: <count>

Skipped:
  <list of skipped finding IDs and reasons>

Validation Failures (if any):
  <check name>: <failure detail>
  Likely cause: <correlated code change>

Next Steps:
  - Review change manifest
  - Apply changes to repo source files (for serverless paths: update job JSON per resources/16-cicd-change-guide.md)
  - CI/CD deploy to UAT for round-trip validation
```

## Cross-References

Skills:
- DBR upgrade: skills/dbr_upgrade/skill.md
- Scala to PySpark: skills/scala_to_pyspark/skill.md
- Conversion validator: skills/conversion_validator/skill.md
- Conversion report: skills/conversion_report/skill.md

Resources:
- Path A guide: resources/01-scala-13.3-to-scala-16.4.md
- Path B guide: resources/02-scala-13.3-to-pyspark-serverless.md
- Path C guide: resources/03-pyspark-sql-13.3-to-pyspark-sql-serverless.md
- Path D guide: resources/04-sql-to-dbsql-serverless.md
- Spark configs: resources/05-spark-config-classic-to-serverless.md
- ANSI compliance: resources/07-ansi-compliance-reference.md
- Testing framework: resources/08-testing-validation-framework.md
- Known issues: resources/13-serverless-known-issues.md
- CI/CD guide: resources/16-cicd-change-guide.md
- Assessment: resources/19-comprehensive-job-assessment.md
