# Testing and Validation Framework

This guide covers the complete testing workflow for all migration paths: archiving, parallel testing, SIT comparison, test coverage creation, and the validation checks that must pass before a migrated job goes to production.

---

## 1. Archiving Original Notebooks

Before Genie Code modifies any notebook, the original must be archived for side-by-side comparison and rollback.

### Archiving Process

```
For each job being migrated:
1. Identify all notebooks referenced by the job (tasks + %run dependencies)
2. Create an archive folder: /Workspace/Archive/migration_2026/{job_name}/original/
3. Copy each notebook to the archive folder (preserving folder structure)
4. Record the notebook paths and last-modified timestamps
5. Create the working copy: /Workspace/Archive/migration_2026/{job_name}/migrated/
6. All modifications happen ONLY in the migrated copy
```

### Archive Naming Convention

```
/Workspace/Archive/migration_2026/
├── {job_name}/
│   ├── original/              ← Untouched copies of the original notebooks
│   │   ├── notebook_1.py
│   │   ├── notebook_2.py
│   │   └── utils/
│   │       └── helpers.py
│   ├── migrated/              ← Working copies with all changes applied
│   │   ├── notebook_1.py
│   │   ├── notebook_2.py
│   │   └── utils/
│   │       └── helpers.py
│   ├── validation/            ← Validation notebooks and reports
│   │   ├── validate_outputs.py
│   │   └── conversion_report.py
│   └── manifest.json          ← Metadata about the migration
```

### manifest.json Template

```json
{
  "job_id": "123456789",
  "job_name": "daily_claims_pipeline",
  "migration_path": "C",
  "source_dbr": "13.3 LTS",
  "target_compute": "serverless_env_v4",
  "notebooks": [
    {
      "original_path": "/Repos/production/claims/01_bronze",
      "archive_path": "/Archive/migration_2026/daily_claims_pipeline/original/01_bronze",
      "migrated_path": "/Archive/migration_2026/daily_claims_pipeline/migrated/01_bronze",
      "language": "python",
      "last_modified": "2026-03-15T10:30:00Z"
    }
  ],
  "output_tables": [
    "prod_catalog.claims.claims_bronze",
    "prod_catalog.claims.claims_silver",
    "prod_catalog.claims.claims_gold_summary"
  ],
  "baseline_versions": {
    "prod_catalog.claims.claims_bronze": 42,
    "prod_catalog.claims.claims_silver": 38,
    "prod_catalog.claims.claims_gold_summary": 35
  },
  "migration_started": "2026-04-16T09:00:00Z",
  "migration_completed": null,
  "validation_status": "pending"
}
```

---

## 2. Recording Baseline Delta Table Versions

Before running the migrated job, record the current version of every output table.

```python
# Run this BEFORE the migrated job executes
from delta.tables import DeltaTable

output_tables = [
    "prod_catalog.claims.claims_bronze",
    "prod_catalog.claims.claims_silver",
    "prod_catalog.claims.claims_gold_summary"
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

# Save these version numbers — you need them for time-travel comparison later
```

---

## 3. Parallel SIT Testing Strategy

The original job and new job run in parallel to ensure no schema changes, table name changes, or data mismatches.

### Approach A: Side-by-Side Schemas (Recommended for Development)

```
Original job writes to:  prod_catalog.claims.*
Migrated job writes to:  test_catalog.claims_migration.*

Compare tables across schemas.
```

**Pros:** Original data is completely untouched. Safe to re-run.
**Cons:** Requires duplicate storage. Must ensure same input data.

### Approach B: Time Travel (Recommended for Final Validation)

```
1. Record baseline versions (original job's output)
2. Run migrated job (overwrites same tables)
3. Compare current version against baseline version via Delta time travel
```

**Pros:** No duplicate storage. Validates in the real environment.
**Cons:** Original data is overwritten. Must complete validation within retention period (30 days default).

### Approach C: Snapshot Comparison (Recommended for Production Cutover)

```
1. Original job runs on schedule → writes to prod tables
2. Migrated job runs immediately after → writes to staging tables
3. Compare prod tables against staging tables
4. After N successful comparisons, swap migrated job to prod
```

**Pros:** Real input data. Production-like conditions.
**Cons:** Doubled compute cost during validation period.

### Choosing an Approach

| Scenario | Recommended Approach |
|----------|---------------------|
| First migration of a job | A (side-by-side) — safest |
| Re-running after fixing issues | A or B |
| Final validation before cutover | B (time travel) |
| Ongoing production validation | C (snapshot) |

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

Before marking a job migration as complete:

- [ ] Original notebooks archived at known path
- [ ] Baseline Delta versions recorded
- [ ] All ANSI fixes applied and documented
- [ ] All serverless restrictions addressed
- [ ] Spark configs migrated or removed
- [ ] Environment variables migrated to widgets
- [ ] Dependencies in requirements.txt (if serverless)
- [ ] Job JSON transformed (if serverless)
- [ ] Migrated job ran successfully on target compute
- [ ] ALL validation checks passed for ALL output tables
- [ ] Performance within acceptable range (< 2x classic)
- [ ] Conversion report generated
- [ ] Test coverage added (if missing in git repo)
- [ ] Parallel SIT run completed with matching results
- [ ] Developer reviewed and approved the conversion report
