# Testing and Validation Framework

This guide covers the complete testing workflow for all migration paths, designed for Molina's environment: Azure DevOps CI/CD with one-directional deployment to Databricks workspaces, no dev environment, UAT as the primary testing environment, and promotion to PROD via CI/CD.

---

## 1. Environment Promotion Workflow

Migrations happen in UAT, get committed to the repo, and promote to PROD via CI/CD. **Code changes never happen directly in PROD.**

### The Parameterization Problem

Molina's CI/CD pipeline deploys notebooks and job JSONs to the Databricks workspace via a PowerShell script that replaces `%placeholder%` tokens with environment-specific values (e.g., `%env_name%` → `uat`, `%eim_wsp%` → the actual workspace path). This means:

- **Repo source files** have `%env_name%`, `%eim_wsp%`, etc. — parameterized and portable
- **Deployed workspace files** have `uat_catalog`, the full workspace path, etc. — hardcoded for that environment

**You cannot export notebooks from the workspace back to the repo.** The hardcoded environment values would break in other environments. The path back to the repo must always go through the change manifest.

### Genie Code's Role: Assess and Prescribe

Genie Code operates as an **advisor, not an editor** for the repo source files:

1. **Reads** deployed notebooks in the UAT workspace (hardcoded values are fine for analysis)
2. **Produces a change manifest** — specific cell/line changes with before/after code
3. **Applies changes to staging copies** in the workspace for testing (these are throwaway copies)
4. **Never exports** modified workspace files back to the repo

The **developer** takes the change manifest and applies the edits to the repo source files (which still have `%placeholder%` tokens intact).

### End-to-End Workflow

```
Step 1: CI/CD deploys current repo code to UAT workspace
        (normal deployment — placeholders resolved to UAT values)

Step 2: Genie Code reads deployed notebooks in UAT
        Produces change manifest:
        ┌─────────────────────────────────────────────┐
        │ CHANGE MANIFEST                              │
        │                                              │
        │ Notebook: .../nyher_member_qa                │
        │ Cell 5, Line 12:                             │
        │   BEFORE: CAST(claim_date AS TIMESTAMP)      │
        │   AFTER:  TRY_CAST(claim_date AS TIMESTAMP)  │
        │                                              │
        │ Cell 8, Line 3:                              │
        │   BEFORE: spark.conf.set("spark.sql.ansi...  │
        │   AFTER:  [REMOVE THIS LINE]                 │
        │                                              │
        │ Cell 12, Line 1:                             │
        │   BEFORE: REFRESH TABLE uat_catalog.schema.. │
        │   AFTER:  [REMOVE THIS LINE]                 │
        └─────────────────────────────────────────────┘

Step 3: Genie Code applies changes to STAGING copies in workspace
        /Workspace/Migration/staging/{job_name}/
        (for testing only — these copies are throwaway)

Step 4: Run migrated job from staging folder
        Job writes to: uat_catalog.{schema}.{table}
        Validate output against production baseline

Step 5: Developer applies change manifest to REPO source files
        (repo files still have %eim_wsp%, %env_name% placeholders)
        Commit to feature branch in Azure DevOps

Step 6: CI/CD redeploys from repo to UAT
        (confirms the repo version matches what was tested)

Step 7: Re-run validation
        (confirms CI/CD-deployed version produces same output)

Step 8: PR merged → CI/CD deploys to PROD
        First PROD run monitored closely
```

**Steps 6-7 are critical** — they close the loop and confirm that the repo version (with placeholder substitution) produces the same results as the staging version that was tested directly.

---

## 2. Archiving and Source of Truth

### Git-Based Archiving (Primary for Molina)

The Azure DevOps repo IS the archive. Original code lives on the main/release branch; migration changes live on a feature branch.

```
Azure DevOps Repo:
├── main (or release branch)          ← Original code (the archive)
├── migration/batch_01/job_name_1     ← Feature branch with changes
├── migration/batch_01/job_name_2     ← Feature branch with changes
└── ...
```

The developer creates the feature branch, applies the change manifest, and submits a PR. The original code is preserved in git history.

### Workspace Staging (For Testing Only)

Genie Code creates staging copies in the workspace for testing. These are **temporary and disposable** — they exist only to validate the changes before the developer commits them to the repo.

```
/Workspace/Migration/staging/
├── {job_name}/
│   ├── notebook_1.py             ← Modified copy (hardcoded UAT values — NOT for repo)
│   ├── notebook_2.py
│   └── change_manifest.md        ← What changed and why (THIS goes to the developer)
```

**Never commit workspace staging files to the repo.** They contain hardcoded environment values.

---

## 3. Cross-Catalog Validation (UAT vs PROD)

The primary validation compares migrated job output in UAT against the production baseline.

### Approach A: Cross-Catalog Comparison (Recommended)

```
Original job (classic compute) writes to:  prod_catalog.{schema}.{table}
Migrated job (serverless) writes to:       uat_catalog.{schema}.{table}

Compare tables across catalogs.
```

```python
# Cross-catalog comparison
original_df = spark.table("prod_catalog.claims.claims_silver")
migrated_df = spark.table("uat_catalog.claims.claims_silver")

# Run all validation checks (see Section 5)
```

**Pros:** Production data is completely untouched. UAT is isolated.
**Cons:** Input data may differ between environments. Must ensure same source data or account for differences.

**Handling input data differences:** If UAT and PROD have different source data volumes, focus validation on:
- Schema match (exact)
- Null count ratios (proportional, not absolute)
- Distinct value sets (should match if same reference data)
- Sample row-level comparison on overlapping data

### Approach B: Time Travel within UAT (For Iterative Testing)

```
1. Run original job (classic compute) in UAT → record baseline version
2. Run migrated job (serverless) in UAT → overwrites same tables
3. Compare current version vs baseline via Delta time travel
```

```python
# Record baseline BEFORE migrated run
baseline_version = spark.sql(
    "DESCRIBE HISTORY uat_catalog.claims.claims_silver LIMIT 1"
).select("version").collect()[0][0]

# After migrated run:
original_df = spark.read.format("delta") \
    .option("versionAsOf", baseline_version) \
    .table("uat_catalog.claims.claims_silver")
migrated_df = spark.table("uat_catalog.claims.claims_silver")
```

**Pros:** Same input data guaranteed. Apples-to-apples comparison.
**Cons:** Original UAT data overwritten. Must validate within retention period.

### Approach C: Post-Deployment PROD Validation (After Cutover)

After CI/CD deploys to PROD, validate the first run:

```
1. Record pre-migration PROD table versions (time travel baseline)
2. CI/CD deploys migrated job to PROD
3. First PROD run executes
4. Compare current PROD output vs pre-migration baseline via time travel
```

**This is the final gate.** If validation fails in PROD, roll back by redeploying the previous job JSON from the repo's main branch.

### Recommended Sequence

| Phase | Environment | Approach | Purpose |
|-------|-------------|----------|---------|
| Development | UAT | B (time travel within UAT) | Iterative testing while Genie Code makes changes |
| Pre-commit validation | UAT vs PROD | A (cross-catalog) | Confirm migrated output matches production |
| Post-CI/CD validation | UAT | B (time travel) | Confirm repo version matches staging version |
| Production cutover | PROD | C (time travel) | Final gate after CI/CD deploys to PROD |

---

## 4. Recording Baseline Delta Table Versions

Before running the migrated job, record the current version of every output table.

```python
# Run this BEFORE the migrated job executes
# Can run against either uat_catalog or prod_catalog depending on the phase

catalog = dbutils.widgets.get("catalog")  # "uat_catalog" or "prod_catalog"

output_tables = [
    f"{catalog}.claims.claims_bronze",
    f"{catalog}.claims.claims_silver",
    f"{catalog}.claims.claims_gold_summary"
]

baseline_versions = {}
for table_name in output_tables:
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        version = history.select("version").collect()[0][0]
        baseline_versions[table_name] = version
        print(f"{table_name}: baseline version = {version}")
    except Exception as e:
        print(f"WARNING: Could not get history for {table_name}: {e}")
```

---

## 4. Validation Checks (Complete Suite)

These checks must ALL pass before a migrated job is approved. Reference the `conversion_validator` skill for detailed code.

### Structural Checks (Seconds to Run)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 1 | Schema comparison | Column names, types, nullability all match | Code change altered output schema |
| 2 | Row count | Exact match + zero orphan rows | Filter logic changed or data loss |

### Statistical Checks (Seconds to Minutes)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 3 | Null count per column | Identical | UDF null handling changed |
| 4 | Aggregate stats (sum, avg, min, max) | Within tolerance (1e-6 for floats) | Calculation logic changed |
| 5 | Distinct value sets | Identical | Value transformation changed |

### Row-Level Checks (Minutes)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 6 | Row-by-row comparison (eqNullSafe) | Zero mismatches | Any data difference |
| 7 | Date/timestamp deep validation | All sub-checks pass | Date parsing, timezone, or format change |

### Semantic Checks (Minutes)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 8 | Null vs empty string confusion | Zero conversions | UDF returns "" instead of None |
| 9 | Null vs zero/default confusion | Zero conversions | UDF returns 0 instead of None |
| 10 | Boolean/flag equivalence | Zero case/whitespace diffs | Case handling changed |
| 11 | Numeric precision/rounding | No rounding_difference+ categories | BigDecimal vs Python round |
| 12 | UDF output consistency | Zero mismatches per UDF column | UDF logic changed |

### Edge Case Checks (Minutes)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 13 | Edge case patterns | All counts match | ANSI mode handling edge cases |
| 14 | Non-determinism detection | Flagged as informational | Sort order or dedup non-determinism |

### Serverless-Specific Checks (New)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 15 | Serverless compute verification | Job ran on serverless | Job JSON misconfigured |
| 16 | Config compliance | No unsupported configs were set | Missed config in migration |
| 17 | Environment key verification | environment_key on all tasks | Job JSON template issue |
| 18 | Performance comparison | Runtime within 2x of classic | Performance regression |

### DBSQL-Specific Checks (Path D)

| # | Check | Pass Criteria | Failure Indicates |
|---|-------|--------------|-------------------|
| 19 | Warehouse execution verification | Query ran on SQL warehouse | Task misconfigured |
| 20 | Parameter passing | All parameters resolved | Widget migration issue |
| 21 | SQL syntax compatibility | No runtime SQL errors | Dialect difference |

---

## 5. Check 15: Serverless Compute Verification

```python
# Verify the job ran on serverless compute
import requests

# Get job run details via API
run_id = dbutils.widgets.get("run_id")  # Pass from workflow
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

response = requests.get(
    f"https://{host}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id}
)
run_details = response.json()

# Check each task for serverless execution
for task in run_details.get("tasks", []):
    cluster_instance = task.get("cluster_instance", {})
    if "spark_context_id" in cluster_instance:
        # Check if it was serverless
        env_key = task.get("environment_key")
        if env_key:
            print(f"PASS: Task '{task['task_key']}' ran with environment_key='{env_key}'")
        else:
            print(f"FAIL: Task '{task['task_key']}' missing environment_key — may have run on classic compute")
```

---

## 6. Check 18: Performance Comparison

```python
# Compare classic vs serverless execution metrics
# Requires system tables access

classic_run_id = dbutils.widgets.get("classic_run_id")
serverless_run_id = dbutils.widgets.get("serverless_run_id")

# Get run durations
classic_metrics = spark.sql(f"""
    SELECT
        task_key,
        duration_ms,
        result_state
    FROM system.lakeflow.job_task_run_timeline
    WHERE run_id = {classic_run_id}
""")

serverless_metrics = spark.sql(f"""
    SELECT
        task_key,
        duration_ms,
        result_state
    FROM system.lakeflow.job_task_run_timeline
    WHERE run_id = {serverless_run_id}
""")

# Compare
comparison = classic_metrics.alias("c").join(
    serverless_metrics.alias("s"),
    on="task_key",
    how="full_outer"
).select(
    "task_key",
    F.col("c.duration_ms").alias("classic_ms"),
    F.col("s.duration_ms").alias("serverless_ms"),
    (F.col("s.duration_ms") / F.col("c.duration_ms")).alias("ratio")
)

comparison.show(truncate=False)

# Flag regressions > 2x
regressions = comparison.filter(F.col("ratio") > 2.0)
if regressions.count() > 0:
    print("WARNING: Performance regressions detected (serverless > 2x classic):")
    regressions.show(truncate=False)
else:
    print("PASS: No significant performance regressions")
```

---

## 7. Test Coverage Requirements

If the git repository does not have tests for the notebooks/code being migrated, add proper test coverage.

### What to Test

| Code Element | Test Type | Framework |
|-------------|-----------|-----------|
| Python classes and methods | Unit tests | `pytest` |
| UDFs (Python) | Unit tests | `pytest` with mock SparkSession |
| Scala classes and methods | Unit tests | `ScalaTest` or `JUnit` |
| Notebook execution | Integration tests | `nutter` or Databricks workflow tests |
| Output data quality | Data validation tests | Custom SQL checks or Great Expectations |

### Python Unit Test Template

```python
# tests/test_udfs.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

class TestCategorizeDiagnosisUDF:
    """Tests for the categorize_diagnosis UDF."""

    def test_valid_endocrine_code(self, spark):
        from notebooks.silver_transforms import categorize_diagnosis
        assert categorize_diagnosis("E11.9") == "Endocrine/Metabolic"

    def test_null_input(self, spark):
        from notebooks.silver_transforms import categorize_diagnosis
        assert categorize_diagnosis(None) is None  # Must return None, not "Unknown"

    def test_empty_string(self, spark):
        from notebooks.silver_transforms import categorize_diagnosis
        assert categorize_diagnosis("") is None  # or "Unknown" — match original behavior

    def test_whitespace_only(self, spark):
        from notebooks.silver_transforms import categorize_diagnosis
        assert categorize_diagnosis("   ") is None

    def test_unknown_prefix(self, spark):
        from notebooks.silver_transforms import categorize_diagnosis
        assert categorize_diagnosis("Z99.99") == "Other"

    def test_spark_integration(self, spark):
        """Test UDF works on a Spark DataFrame."""
        from notebooks.silver_transforms import categorize_diagnosis_udf
        df = spark.createDataFrame([("E11.9",), (None,), ("",)], ["code"])
        result = df.withColumn("category", categorize_diagnosis_udf(F.col("code")))
        rows = result.collect()
        assert rows[0]["category"] == "Endocrine/Metabolic"
        assert rows[1]["category"] is None
        assert rows[2]["category"] is None
```

### Data Validation Test Template

```python
# tests/test_data_quality.py

class TestClaimsSilverDataQuality:
    """Data quality checks for claims_silver table after migration."""

    def test_no_null_claim_ids(self, spark):
        df = spark.table("test_catalog.claims.claims_silver")
        null_count = df.filter(F.col("claim_id").isNull()).count()
        assert null_count == 0, f"Found {null_count} null claim_ids"

    def test_valid_date_ranges(self, spark):
        df = spark.table("test_catalog.claims.claims_silver")
        invalid = df.filter(
            (F.col("claim_date") < "2000-01-01") |
            (F.col("claim_date") > F.current_date())
        ).count()
        assert invalid == 0, f"Found {invalid} claims with dates outside valid range"

    def test_amount_not_negative(self, spark):
        df = spark.table("test_catalog.claims.claims_silver")
        negative = df.filter(F.col("paid_amount") < 0).count()
        # Note: Some claims legitimately have negative amounts (reversals)
        # Adjust assertion based on business rules

    def test_schema_matches_expected(self, spark):
        df = spark.table("test_catalog.claims.claims_silver")
        expected_columns = {
            "claim_id": "string",
            "member_id": "string",
            "claim_date": "date",
            "paid_amount": "decimal(18,2)",
            # ... all expected columns
        }
        for col_name, col_type in expected_columns.items():
            assert col_name in df.columns, f"Missing column: {col_name}"
```

### Notebook Integration Test Template (Nutter)

```python
# tests/test_notebook_integration.py
from runtime.nutterfixture import NutterFixture

class TestClaimsPipeline(NutterFixture):

    def before_all(self):
        # Setup: ensure test catalog and schema exist
        spark.sql("CREATE CATALOG IF NOT EXISTS test_catalog")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_catalog.claims_test")

    def assertion_bronze_table_created(self):
        """Verify bronze table was created with expected schema."""
        df = spark.table("test_catalog.claims_test.claims_bronze")
        assert df.count() > 0
        assert "claim_id" in df.columns

    def assertion_silver_table_populated(self):
        """Verify silver table has transformed data."""
        df = spark.table("test_catalog.claims_test.claims_silver")
        assert df.count() > 0
        # Verify transforms applied
        assert "diagnosis_category" in df.columns

    def after_all(self):
        # Cleanup: drop test tables
        spark.sql("DROP SCHEMA IF EXISTS test_catalog.claims_test CASCADE")
```

---

## 8. Validation Report Template

After running all checks, produce a structured report:

```
╔══════════════════════════════════════════════════════════════╗
║              MIGRATION VALIDATION REPORT                     ║
║  Job: daily_claims_pipeline (ID: 123456789)                  ║
║  Path: C (PySpark/SQL 13.3 → Serverless)                     ║
║  Date: 2026-04-16                                            ║
╚══════════════════════════════════════════════════════════════╝

TABLES VALIDATED: 3/3
  ✓ prod_catalog.claims.claims_bronze
  ✓ prod_catalog.claims.claims_silver
  ✓ prod_catalog.claims.claims_gold_summary

CHECK RESULTS:
  Structural:     2/2  PASS
  Statistical:    3/3  PASS
  Row-Level:      2/2  PASS
  Semantic:       5/5  PASS
  Edge Cases:     2/2  PASS
  Serverless:     4/4  PASS

OVERALL: PASS — All 18 checks passed for all 3 tables

PERFORMANCE:
  Task 01_bronze:   Classic 4m 12s → Serverless 3m 45s (0.89x) ✓
  Task 02_silver:   Classic 8m 30s → Serverless 7m 15s (0.85x) ✓
  Task 03_gold:     Classic 2m 05s → Serverless 1m 50s (0.88x) ✓

RECOMMENDATION: Approved for production cutover
═══════════════════════════════════════════════════════════════
```

---

## 9. Failure Remediation Workflow

When validation fails:

```
Validation fails
├── Which check failed?
│   ├── Check 1 (Schema) → Code changed output schema
│   │   └── Review migrated notebook for .select() / .drop() / .withColumn() changes
│   ├── Check 2 (Row count) → Filter logic changed
│   │   └── Review .filter() / .where() conditions for ANSI mode impact
│   ├── Checks 3,8,9 (Null patterns) → UDF null handling
│   │   └── Fix UDFs: return None, not "", not 0, not "Unknown"
│   ├── Check 4 (Aggregates) → Calculation logic changed
│   │   └── Check division fixes and rounding changes
│   ├── Check 7 (Dates) → Date parsing changed
│   │   └── Check try_to_date/try_to_timestamp conversions
│   ├── Check 11 (Precision) → Rounding changed
│   │   └── Check BigDecimal → Decimal conversion in UDFs
│   ├── Check 15 (Serverless verify) → Wrong compute
│   │   └── Check job JSON for environment_key
│   └── Check 18 (Performance) → Regression
│       └── Check for .collect() on large data, missing OPTIMIZE
├── Fix the issue in the migrated copy
├── Re-run the migrated job
└── Re-run validation
    └── Repeat until all checks pass
```

---

## 10. Sign-Off Checklist

### Gate 1: UAT Staging Validation (Genie Code changes in workspace)

- [ ] Genie Code assessment completed (resource 19)
- [ ] Change manifest produced with all findings
- [ ] Staging copies created in `/Workspace/Migration/staging/`
- [ ] Migrated job ran successfully on serverless in UAT
- [ ] Validation checks passed: UAT migrated output vs baseline

### Gate 2: Repo Commit (Developer applies changes)

- [ ] Developer applied change manifest to repo source files
- [ ] Repo source files still have `%placeholder%` tokens (not hardcoded values)
- [ ] Job JSON updated (serverless environments block, parameters, etc.)
- [ ] PowerShell script has `%env_name%` replacement
- [ ] Feature branch committed to Azure DevOps

### Gate 3: CI/CD Round-Trip Validation (Confirms repo version works)

- [ ] CI/CD deployed feature branch to UAT
- [ ] Job ran successfully on serverless from CI/CD-deployed version
- [ ] Validation checks passed: CI/CD version output matches staging version output
- [ ] Job ID preserved (not recreated)
- [ ] Performance within acceptable range (< 2x classic)

### Gate 4: Production Cutover

- [ ] PR reviewed and approved
- [ ] PR merged to main/release branch
- [ ] CI/CD deployed to PROD
- [ ] First PROD run monitored
- [ ] Post-PROD validation passed (time travel comparison against pre-migration baseline)
- [ ] Conversion report generated and archived
- [ ] Staging workspace copies cleaned up
