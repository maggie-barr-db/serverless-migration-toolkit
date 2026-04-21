# DBR Runtime Upgrade: 13.3 LTS to 16.4 LTS

This skill covers the runtime upgrade from DBR 13.3 LTS (Spark 3.4.1, Scala 2.12.15) to DBR 16.4 LTS (Spark 3.5.x, Scala 2.12.18). It applies to Scala, PySpark, and SQL code.

ANSI compliance fix patterns are in the **ansi_fixes** skill. This skill covers everything else.

---

## 1. Overview

DBR 16.4 LTS upgrades Spark from 3.4.1 to 3.5.x and Scala from 2.12.15 to 2.12.18. Key areas of change:

- **ANSI mode ON by default** -- the single most impactful change (fix patterns in ansi_fixes skill)
- **Spark config defaults changed** -- sources.default, AQE behavior, datetime rebase modes
- **Delta Lake protocol advances** -- deletion vectors default on new tables, Liquid Clustering GA, row tracking available
- **SQL parser stricter** -- trailing commas, reserved words, GROUP BY ordinals
- **ML library version bumps** -- pandas 2.x, scikit-learn 1.3, XGBoost 2.0, PyTorch 2.1
- **New features available** -- Predictive I/O, IDENTIFIER(), Variant type, default column values

What stays the same: all DataFrame API operations, Spark SQL syntax, dbutils, Delta DML, window functions, UDF registration, notebook %run, spark.read/spark.write.

---

## 2. Config Changes

### 2.1 Configs with Changed Defaults

| Config | 13.3 Default | 16.4 Default | Action |
|--------|-------------|-------------|--------|
| `spark.sql.ansi.enabled` | `false` | `true` | **CRITICAL** -- see ansi_fixes skill |
| `spark.sql.sources.default` | `parquet` | `delta` | Affects `spark.read`/`spark.write` without explicit format |
| `spark.sql.parquet.datetimeRebaseModeInRead` | `LEGACY` | `CORRECTED` | Affects pre-1582 dates (rare) |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | `LEGACY` | `CORRECTED` | Affects pre-1582 dates (rare) |
| `spark.sql.parquet.int96RebaseModeInRead` | `LEGACY` | `CORRECTED` | Affects INT96 timestamps |
| `spark.sql.parquet.int96RebaseModeInWrite` | `LEGACY` | `CORRECTED` | Affects INT96 timestamps |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | `true` | `true` | More aggressive in 3.5 -- may change partition counts |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `30MB` | Updated in 3.5 | May change join strategies |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` (some contexts) | Write performance change |
| `spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled` | `false` | `true` | Cleaner PySpark UDF tracebacks |

### 2.2 Deprecated / Removed Configs

| Config | Status | Replacement |
|--------|--------|-------------|
| `spark.sql.legacy.createHiveTableByDefault` | Removed | Tables always Delta by default |
| `spark.sql.legacy.allowNonEmptyLocationInCTAS` | Removed | CTAS always requires empty location |
| `spark.sql.legacy.sizeOfNull` | Removed | `size(null)` returns `null` (was `-1`) -- update null checks |
| `spark.sql.legacy.timeParserPolicy` | Removed | Always CORRECTED policy |
| `spark.sql.legacy.replaceDatabricksSparkAvro.enabled` | Removed | Always uses built-in Avro |
| `spark.sql.legacy.fromDayTimeString.enabled` | Removed | Strict interval parsing |
| `spark.sql.legacy.setCommandRejectsSparkCoreConfs` | Removed | Spark core confs always accepted |
| `spark.sql.legacy.allowCastNumericToTimestamp` | Removed | No replacement |
| `spark.sql.legacy.exponentLiteralAsDecimalEnabled` | Removed | No replacement |
| `spark.sql.legacy.bucketedTableScan.enabled` | Removed | No replacement |
| `spark.sql.hive.convertMetastoreOrc` | Deprecated | Use Delta or native ORC reader |
| `spark.sql.hive.convertMetastoreParquet` | Deprecated | Use Delta or native Parquet reader |

Detect all legacy configs:
```regex
spark\.sql\.legacy\.\w+|spark\.sql\.hive\.convert
```

### 2.3 Intermediate Version Config Changes (14.3, 15.4)

| Config | Changed In | Old | New |
|--------|-----------|-----|-----|
| `spark.sql.ansi.enabled` | DBR 14.3+ | `false` | `true` |
| `spark.databricks.delta.properties.defaults.deletionVectors.enabled` | DBR 14.3+ | `false` | `true` (new tables) |
| `spark.sql.adaptive.forceOptimizedHashAgg` | Spark 3.5 | Not present | `true` |

---

## 3. Delta Lake Changes

### 3.1 Protocol Versions

| Feature | DBR 13.3 | DBR 16.4 | Protocol Required |
|---------|----------|----------|-------------------|
| minWriterVersion default | 2 | 7 (new tables with features) | N/A |
| Deletion Vectors | Available (opt-in) | Default for new tables | Reader v3, Writer v7 |
| Column Mapping | Available | Available | Reader v2, Writer v5 |
| Row Tracking | Not available | Available (opt-in) | Writer v7 |
| Liquid Clustering | Not available | GA | Reader v3, Writer v7 |

**Protocol upgrades are ONE-WAY.** Once upgraded, older runtimes cannot read/write the table.

### 3.2 Deletion Vectors

New tables on 16.4 have deletion vectors enabled by default. Existing tables are NOT auto-upgraded.

Impact: tables created on 16.4 require Reader v3. If any 13.3 cluster reads them, it will FAIL.

During migration window, disable per-table if backward compatibility needed:
```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
```

After all jobs are on 16.4, re-enable deletion vectors.

### 3.3 Row Tracking

New opt-in feature. Not enabled by default. Enables row-level change tracking for CDC patterns.
```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');
```

### 3.4 Column Mapping

Available in both versions. Default mode may differ for new tables. Existing tables unaffected.

### 3.5 Liquid Clustering (GA in 16.4)

Replaces ZORDER with automatic, incremental clustering.
```sql
-- Migrate existing table (requires protocol upgrade to Writer v7)
ALTER TABLE catalog.schema.my_table
CLUSTER BY (col_a, col_b);

-- Then optimize once to reorganize existing data
OPTIMIZE catalog.schema.my_table;
-- No ZORDER BY needed -- clustering columns stored in table metadata
```

**Do NOT migrate to Liquid Clustering during the DBR upgrade.** Plan as a separate post-migration optimization. It changes physical layout and requires Writer v7.

### 3.6 Auto-Upgrade Risks

| Operation | Protocol Upgrade? | Risk |
|-----------|-------------------|------|
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')` | YES - Writer v7, Reader v3 | Breaks 13.3 access |
| `ALTER TABLE ... CLUSTER BY (...)` | YES - Writer v7, Reader v3 | Breaks 13.3 access |
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')` | YES - Writer v7 | Breaks 13.3 writes |
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` | YES - Reader v2, Writer v5 | May break 13.3 access |
| Regular INSERT/UPDATE/DELETE on existing tables | NO | Safe |
| Creating new tables | Possible (deletion vectors default) | May break 13.3 readers |

---

## 4. Scala-Specific Changes

### 4.1 Scala 2.12.15 to 2.12.18

Minor version bump -- full backward compatibility. Changes are bug fixes only.

| Area | 2.12.15 | 2.12.18 | Impact |
|------|---------|---------|--------|
| Pattern matching exhaustiveness | Less strict warnings | Improved warnings | Compilation warnings may appear |
| Implicit resolution | Minor edge cases | Bug fixes | Extremely unlikely to affect user code |
| Collections | No changes | Bug fixes | No user-visible impact |
| Reflection (`scala.reflect`) | Minor bugs | Fixes | Only affects code using reflection |

No code changes required for the Scala version bump alone.

### 4.2 Dataset API Deprecations

```scala
// Encoder derivation warnings may appear -- fix with explicit encoder:
import org.apache.spark.sql.Encoders
implicit val enc = Encoders.product[MyCaseClass]
val ds = df.as[MyCaseClass]
```

| Method | Status | Replacement |
|--------|--------|-------------|
| `SQLContext` | Deprecated | `SparkSession` |
| `HiveContext` | Deprecated | `SparkSession` |
| `Dataset.explain(true)` | Deprecated overload | `Dataset.explain("extended")` |
| `Dataset.queryExecution.toRdd` | Internal API, may break | `Dataset.rdd` |
| `DataFrameReader.json(RDD)` | Deprecated | `spark.read.json(Dataset[String])` |
| `unionAll()` | Deprecated | `union()` |

### 4.3 SQL Parser Strictness (Spark 3.5)

- Trailing commas in SELECT lists now produce errors in some contexts
- Unquoted reserved words used as identifiers may fail
- `GROUP BY` with ordinal references has stricter validation
- Ambiguous column references in joins are stricter
- Cross join detection is stricter (accidental cross joins may fail)

### 4.4 Type Inference Changes

Decimal precision may differ for complex expressions in 3.5. Explicitly cast result to desired precision:
```scala
df.selectExpr("CAST(SUM(CAST(amount AS DECIMAL(10,2))) AS DECIMAL(18,2))")
```

---

## 5. PySpark-Specific Changes

### 5.1 Arrow UDF Defaults

| Config | 13.3 | 16.4 |
|--------|------|------|
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | `true` |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | `true` | `true` |
| `spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled` | `false` | `true` |

Simplified traceback: PySpark UDF errors show cleaner tracebacks. Error-handling code that parsed traceback strings may need updates.

### 5.2 pandas_udf Improvements

Same API, better performance in 16.4:
- Arrow serialization is faster
- Type checking is stricter under ANSI mode
- Null handling may differ if input types do not match schema

Add explicit null handling for ANSI safety:
```python
@pandas_udf("double")
def normalize_amount(s: pd.Series) -> pd.Series:
    if s.empty:
        return s
    max_val = s.max()
    if max_val == 0 or pd.isna(max_val):
        return pd.Series([None] * len(s))
    return s / max_val
```

### 5.3 Package Changes

| Package | 13.3 | 16.4 | Action |
|---------|------|------|--------|
| `pandas` | 1.5.x | 2.0.x+ | Check for deprecated pandas APIs |
| `numpy` | 1.23.x | 1.24.x+ | Minor changes, mostly compatible |
| `koalas` | Deprecated | Removed | Use `pyspark.pandas` |
| `pyarrow` | Pre-installed | Pre-installed (newer) | Remove explicit version pins |

Detect koalas usage:
```regex
import\s+databricks\.koalas|import\s+koalas
```
Replace with `import pyspark.pandas as ps`.

---

## 6. ML Runtime Changes

### 6.1 Library Version Bumps

| Library | ML 13.3 | ML 16.4 | Key Breaking Changes |
|---------|---------|---------|---------------------|
| scikit-learn | 1.1.x | 1.3.x+ | `normalize` param removed from LinearRegression; KMeans `n_init` default -> `'auto'` |
| XGBoost | 1.7.x | 2.0.x+ | Default tree_method -> `'hist'`; `gpu_id`/`tree_method="gpu_hist"` deprecated -> use `device="cuda"` |
| pandas | 1.5.x | 2.0.x+ | Copy-on-write, deprecation warnings; check `.append()` removal |
| numpy | 1.23.x | 1.24.x+ | Minor deprecations; mostly compatible |
| PyTorch | 1.13.x | 2.1.x+ | `torch.compile()` stable; distributed training API changes |
| TensorFlow | 2.12.x | 2.15.x+ | Keras 3.x changes; `tf.compat.v1.*` further removals; SavedModel format forward-only |
| MLflow | Pre-installed | Pre-installed (newer) | Remove explicit install -- use runtime version |
| LightGBM | 3.3.x | 4.1.x+ | API largely stable |
| Hugging Face | 4.26.x | 4.36.x+ | Pipeline API new defaults; tokenizer changes; model loading safety checks |
| Hyperopt | 0.2.7 | 0.2.7 | No change |

### 6.2 MLlib API Changes (Spark 3.4 to 3.5)

- `spark.mllib` (RDD-based) still deprecated -- use `spark.ml` (DataFrame-based)
- StringIndexer/VectorAssembler: explicitly set `handleInvalid` for ANSI safety
- Pipeline save/load: models saved on 13.3 load on 16.4; models saved on 16.4 may NOT load on 13.3
- CrossValidator memory management improved -- no code change needed

### 6.3 Feature Store

Old `FeatureStoreClient` is deprecated. Migrate to:
```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
```

---

## 7. New Features Available in 16.4

| Feature | Description | How to Use |
|---------|-------------|------------|
| **Predictive I/O** | Prefetches data for faster Delta scans | Enabled by default -- no code change |
| **Liquid Clustering** | Replaces ZORDER with automatic incremental clustering | `ALTER TABLE ... CLUSTER BY (cols)` then `OPTIMIZE` |
| **IDENTIFIER()** | Dynamic SQL without injection risk | `SELECT * FROM IDENTIFIER(:table_name)` |
| **Variant type** | Native semi-structured data (verify GA for your version) | `PARSE_JSON('...')`, access via `col:field::type` |
| **Default column values** | Table-level column defaults | `ALTER TABLE ADD COLUMN x INT DEFAULT 0` |
| **Python UDF improvements** | 3-5x faster Python UDFs | Automatic -- no code change |
| **Async progress tracking** | Reduces streaming commit latency | `spark.conf.set("spark.sql.streaming.asyncProgressTrackingEnabled", "true")` |
| **TRY_ADD / TRY_SUBTRACT** | Overflow-safe arithmetic (Spark 3.5+) | `TRY_ADD(a, b)` returns null on overflow |

IDENTIFIER() example:
```sql
-- Safe dynamic SQL (replaces string interpolation)
SELECT * FROM IDENTIFIER('catalog.schema.my_table')
SELECT IDENTIFIER('member_id') FROM my_table
```

Default column values example:
```sql
CREATE TABLE catalog.schema.claims (
  claim_id BIGINT,
  status STRING DEFAULT 'PENDING',
  created_at TIMESTAMP DEFAULT current_timestamp()
);
```

---

## 8. Scan Checklist

When upgrading a notebook from 13.3 to 16.4, run all checks in this order.

### Pass 1: CRITICAL -- Will cause runtime failures

- [ ] Scan for ANSI-breaking patterns -- **see ansi_fixes skill for all ANSI patterns**
- [ ] Scan for `spark.sql.legacy.*` configs -- remove or replace (see Section 2.2)
- [ ] Scan for `REFRESH TABLE` / `MSCK REPAIR TABLE` -- remove (not needed with Delta)
- [ ] Scan for `spark.sql.ansi.enabled = false` -- remove, fix code instead
- [ ] Verify no explicit `spark.sql.sources.default` = `parquet` if Delta is intended

### Pass 2: HIGH -- Likely to cause failures

- [ ] Scan for `size(col) == -1` null checks -- replace with `col IS NULL` (`sizeOfNull` removed)
- [ ] Scan for datetime rebase mode configs -- verify pre-1582 dates are not present
- [ ] Scan for `CREATE TABLE` / `.saveAsTable` -- check if 13.3 readers need access (deletion vectors)
- [ ] Scan for `ALTER TABLE ... SET TBLPROPERTIES` with Delta features -- protocol upgrade risk
- [ ] Scan for `com.crealytics.spark.excel` -- replace with pandas + openpyxl
- [ ] Scan for `dbutils.library.install` -- move to requirements.txt
- [ ] Scan for Jackson version pins -- remove explicit dependency, use Spark bundled version

### Pass 3: MEDIUM -- May cause issues depending on data

- [ ] Scan for `SQLContext` / `HiveContext` -- replace with `SparkSession`
- [ ] Scan for `.unionAll()` -- replace with `.union()`
- [ ] Scan for `koalas` imports -- replace with `pyspark.pandas`
- [ ] Scan for `.explain(true)` -- replace with `.explain("extended")`
- [ ] Scan for `FeatureStoreClient` -- migrate to `FeatureEngineeringClient`
- [ ] Review `.repartition(N)` / `.coalesce(N)` -- AQE may change partition counts
- [ ] Review `SELECT *` usage with row filters -- use explicit column lists

### Pass 4: LOW -- Performance and informational

- [ ] Note ZORDER usage -- recommend awareness of Liquid Clustering for future optimization
- [ ] Scan for `io.delta:delta-core` in Maven coordinates -- remove (built into runtime)
- [ ] Scan for `com.databricks:spark-avro` -- remove (built into Spark)
- [ ] Scan for `%pip install pyspark` / `%pip install delta-spark` -- remove (use runtime version)
- [ ] Scan for `tree_method="gpu_hist"` / `gpu_id` -- replace with `device="cuda"` (XGBoost 2.0)
- [ ] Scan for `.queryExecution` usage -- internal API, use public Dataset methods

---

## 9. Troubleshooting

### ArithmeticException on Divide

```
SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero.
Use `try_divide` to tolerate divisor being 0 and return NULL instead.
```
**Cause:** ANSI mode ON, denominator is zero.
**Fix:** See ansi_fixes skill for TRY_DIVIDE / NULLIF patterns. Do NOT set `spark.sql.ansi.enabled = false`.

### NumberFormatException on Cast

```
SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'ABC' cannot be cast to "INT".
Use `try_cast` to tolerate malformed input and return NULL instead.
```
**Cause:** ANSI mode ON, invalid string-to-numeric conversion.
**Fix:** See ansi_fixes skill for TRY_CAST patterns. Investigate the bad data:
```sql
SELECT DISTINCT value FROM table
WHERE TRY_CAST(value AS INT) IS NULL AND value IS NOT NULL
```

### AQE Partition Count Changes

**Symptom:** Output files have different counts. Data is logically correct but physical layout differs.
**Fix:** If exact file count matters, repartition before write:
```scala
df.repartition(targetCount).write.format("delta").save(path)
```
Or run `OPTIMIZE` after write. Usually the new counts are better -- only investigate if downstream depends on exact file count.

### UDF Null Handling Changes

**Symptom:** UDFs throw NullPointerException that did not occur on 13.3.
**Cause:** ANSI mode makes type coercion stricter, changing what types reach UDFs.
**Fix:** Always handle nulls explicitly:
```scala
val safeUdf = udf((input: String) => {
  Option(input).map(_.trim.toUpperCase).orNull
})
```
```python
@udf("string")
def safe_udf(value):
    if value is None:
        return None
    return value.strip().upper()
```

### Config Not Found / Removed Config Errors

```
AnalysisException: The SQL config 'spark.sql.legacy.sizeOfNull' was removed in version X.X.X
```
**Cause:** Code sets a config that was removed in 16.4.
**Fix:** Remove the config and update code to work with new default behavior:
```scala
// Remove: spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
// Change: size(array_col) == -1  -->  array_col IS NULL
df.filter(col("array_col").isNull)
```

### Delta Protocol Errors

```
InvalidProtocolVersionException: Delta table requires reader version 3
but the current reader version is 1.
```
**Cause:** Table protocol was upgraded (e.g., deletion vectors enabled) but the cluster does not support it.
**Investigation:**
```sql
DESCRIBE DETAIL catalog.schema.table_name
SHOW TBLPROPERTIES catalog.schema.table_name
```
**Fix:**
- If error on 13.3 cluster: the table was upgraded by a 16.4 job. Cannot downgrade. Move all jobs to 16.4.
- Prevention: record all table protocols before migration:
```sql
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE data_source_format = 'DELTA'
```

### SparkDateTimeException on Date Parsing

```
SparkDateTimeException: [CAST_INVALID_INPUT] The value '02/30/2024' cannot be cast to "DATE".
```
**Cause:** ANSI mode rejects invalid dates that previously returned null.
**Fix:** See ansi_fixes skill for `try_to_date` / `try_to_timestamp` patterns.

### SparkArrayIndexOutOfBoundsException

```
SparkArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index 5 is out of bounds.
The array has 3 elements. Use `try_element_at` to tolerate invalid index.
```
**Cause:** ANSI mode throws on out-of-bounds array access.
**Fix:** See ansi_fixes skill for TRY_ELEMENT_AT patterns.
