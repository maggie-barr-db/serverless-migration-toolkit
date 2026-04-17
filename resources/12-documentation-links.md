# Documentation Links Reference

Consolidated reference of all Databricks documentation relevant to the migration paths. Use these links for edge cases and detailed specifications beyond what the migration guides cover.

---

## DBR Release Notes

| Version | Link | Key Changes |
|---------|------|-------------|
| DBR 16.4 LTS | https://docs.databricks.com/en/release-notes/runtime/16.4lts.html | ANSI on by default, Spark 3.5.x, Scala 2.12.18 |
| DBR 15.4 LTS | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html | Intermediate LTS between 13.3 and 16.4 |
| DBR 14.3 LTS | https://docs.databricks.com/en/release-notes/runtime/14.3lts.html | Intermediate LTS, significant Delta changes |
| DBR 13.3 LTS | https://docs.databricks.com/en/release-notes/runtime/13.3lts.html | Source runtime for all migration paths |
| ML Runtime 16.4 | https://docs.databricks.com/en/release-notes/runtime/16.4lts-ml.html | ML-specific library versions |
| ML Runtime 13.3 | https://docs.databricks.com/en/release-notes/runtime/13.3lts-ml.html | Source ML runtime |

## Serverless Compute

| Topic | Link |
|-------|------|
| Serverless compute overview | https://docs.databricks.com/en/compute/serverless.html |
| Serverless compute limitations | https://docs.databricks.com/en/compute/serverless.html#limitations |
| Serverless environment versions | https://docs.databricks.com/en/compute/serverless.html#environment-versions |
| Serverless dependencies (requirements.txt) | https://docs.databricks.com/en/compute/serverless.html#install-python-libraries |
| Supported Spark configs on serverless | https://docs.databricks.com/en/compute/serverless.html#supported-spark-configuration-properties |
| Serverless jobs configuration | https://docs.databricks.com/en/jobs/serverless.html |

## DBSQL Serverless

| Topic | Link |
|-------|------|
| SQL warehouses overview | https://docs.databricks.com/en/compute/sql-warehouse/index.html |
| Serverless SQL warehouses | https://docs.databricks.com/en/compute/sql-warehouse/serverless.html |
| SQL warehouse configuration | https://docs.databricks.com/en/compute/sql-warehouse/warehouse-settings.html |
| SQL reference | https://docs.databricks.com/en/sql/language-manual/index.html |
| SQL functions reference | https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html |
| SQL task in workflows | https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html#add-a-sql-task |
| SQL parameters | https://docs.databricks.com/en/sql/user/queries/query-parameters.html |
| Photon engine | https://docs.databricks.com/en/compute/photon.html |

## ANSI Mode

| Topic | Link |
|-------|------|
| ANSI compliance overview | https://docs.databricks.com/en/sql/language-manual/sql-ref-ansi-compliance.html |
| TRY_CAST function | https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html |
| TRY_DIVIDE function | https://docs.databricks.com/en/sql/language-manual/functions/try_divide.html |
| TRY_ELEMENT_AT function | https://docs.databricks.com/en/sql/language-manual/functions/try_element_at.html |
| try_to_timestamp function | https://docs.databricks.com/en/sql/language-manual/functions/try_to_timestamp.html |
| try_to_date function | https://docs.databricks.com/en/sql/language-manual/functions/try_to_date.html |
| try_to_number function | https://docs.databricks.com/en/sql/language-manual/functions/try_to_number.html |

## Delta Lake

| Topic | Link |
|-------|------|
| Delta Lake documentation | https://docs.databricks.com/en/delta/index.html |
| Delta table properties | https://docs.databricks.com/en/delta/table-properties.html |
| Liquid Clustering | https://docs.databricks.com/en/delta/clustering.html |
| Deletion vectors | https://docs.databricks.com/en/delta/deletion-vectors.html |
| Column mapping | https://docs.databricks.com/en/delta/column-mapping.html |
| Row tracking | https://docs.databricks.com/en/delta/row-tracking.html |
| OPTIMIZE command | https://docs.databricks.com/en/sql/language-manual/delta-optimize.html |
| VACUUM command | https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html |
| Table protocol versions | https://docs.databricks.com/en/delta/table-protocol-versioning.html |
| Predictive Optimization | https://docs.databricks.com/en/optimizations/predictive-optimization.html |

## Apache Spark

| Topic | Link |
|-------|------|
| Spark 3.5 migration guide | https://spark.apache.org/docs/3.5.0/migration-guide.html |
| Spark SQL reference | https://spark.apache.org/docs/3.5.0/sql-ref.html |
| PySpark API reference | https://spark.apache.org/docs/3.5.0/api/python/index.html |
| Spark configuration | https://spark.apache.org/docs/3.5.0/configuration.html |

## Databricks Jobs and Workflows

| Topic | Link |
|-------|------|
| Jobs API 2.1 | https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html |
| Job parameters | https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html |
| Serverless jobs | https://docs.databricks.com/en/jobs/serverless.html |
| Multi-task workflows | https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html |
| Job JSON format | https://docs.databricks.com/api/workspace/jobs/create |

## Unity Catalog

| Topic | Link |
|-------|------|
| Unity Catalog overview | https://docs.databricks.com/en/data-governance/unity-catalog/index.html |
| External locations | https://docs.databricks.com/en/connect/unity-catalog/external-locations.html |
| Volumes | https://docs.databricks.com/en/connect/unity-catalog/volumes.html |
| System tables | https://docs.databricks.com/en/administration-guide/system-tables/index.html |

## Python

| Topic | Link |
|-------|------|
| Python 3.12 what's new | https://docs.python.org/3.12/whatsnew/3.12.html |
| Python 3.11 what's new | https://docs.python.org/3.11/whatsnew/3.11.html |
| Python 3.12 removed modules | https://docs.python.org/3.12/whatsnew/3.12.html#removed |

## ML and Data Science

| Topic | Link |
|-------|------|
| MLlib guide | https://docs.databricks.com/en/machine-learning/train-model/mllib.html |
| Feature Engineering | https://docs.databricks.com/en/machine-learning/feature-store/index.html |
| MLflow on Databricks | https://docs.databricks.com/en/mlflow/index.html |
| Model Serving | https://docs.databricks.com/en/machine-learning/model-serving/index.html |
| pandas API on Spark | https://docs.databricks.com/en/pandas/pandas-on-spark.html |

## Testing and Validation

| Topic | Link |
|-------|------|
| Delta time travel | https://docs.databricks.com/en/delta/history.html |
| DESCRIBE HISTORY | https://docs.databricks.com/en/sql/language-manual/delta-describe-history.html |
| Nutter testing framework | https://github.com/microsoft/nutter |
| pytest on Databricks | https://docs.databricks.com/en/dev-tools/testing.html |

---

## How to Use These Links in Genie Code

When Genie Code encounters an edge case not covered by the migration guides:

1. **Identify the category** (ANSI, Delta, Serverless, etc.)
2. **Reference the specific documentation link** from this file
3. **Provide the link to the developer** with a note about what to check
4. **Do NOT guess** — if the behavior is ambiguous, recommend the developer verify against the documentation

### Example Edge Case Handling

```
EDGE CASE: Notebook uses VARIANT type columns
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This is a DBR 16.4+ feature not available in 13.3.
If the code creates VARIANT columns, it only works on 16.4+.
If the code reads existing VARIANT columns, verify serverless support.

Reference: https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html

Recommendation: Verify VARIANT type GA status before using in production.
Do not adopt non-GA features for Molina workloads.
```
