# DBR Runtime Upgrade (13.3 → 16.4)

This skill teaches you how to upgrade Databricks notebooks and job configurations from DBR 13.3 LTS (Spark 3.4.1) to DBR 16.4 LTS (Spark 3.5.x). It applies to both Scala and PySpark code.

## What Changes Between 13.3 and 16.4

### Spark Configuration Defaults Changed

These configs have new default values in 16.4. If the code relies on the old behavior, add explicit overrides.

| Config | DBR 13.3 Default | DBR 16.4 Default | Action |
|--------|-----------------|-----------------|--------|
| `spark.sql.ansi.enabled` | `false` | `true` | Do NOT set to false — fix the code instead (see ANSI Compliance Fixes below) |
| `spark.sql.adaptive.enabled` | `true` | `true` | No change, but AQE behavior is more aggressive — may change partition counts |
| `spark.sql.sources.default` | `parquet` | `delta` | Only affects `spark.read`/`spark.write` without explicit format |
| `spark.sql.timestampType` | `TIMESTAMP_NTZ` in 13.3+ | `TIMESTAMP_NTZ` | No change, but verify timestamp handling if upgrading from pre-13.3 |

### ANSI Mode — The Most Impactful Change

DBR 16.4 enables ANSI mode by default. This changes error behavior:

| Operation | ANSI OFF (13.3 default) | ANSI ON (16.4 default) |
|-----------|------------------------|----------------------|
| Integer overflow | Silent wraparound | Throws ArithmeticException |
| Divide by zero | Returns null | Throws ArithmeticException |
| Cast "abc" to int | Returns null | Throws NumberFormatException |
| Array out of bounds | Returns null | Throws ArrayIndexOutOfBoundsException |
| Map key not found | Returns null | Throws NoSuchElementException |

**Do NOT set `spark.sql.ansi.enabled = false`.** Instead, fix the code to be ANSI-compliant. The ANSI behaviors are safer — especially for healthcare data where silent null returns can mask data quality issues. See the ANSI Compliance Fixes section below.

### ANSI Compliance Fixes

When upgrading to 16.4, scan all notebooks for the patterns below and apply the corresponding fix. This ensures the code works correctly with ANSI mode enabled (the 16.4 default) rather than preserving legacy behavior with a flag.

#### Type Casting

**Pattern:** `CAST(expr AS type)` or `.cast(type)` on values that may not be valid for the target type.

**Risk:** ANSI mode throws `NumberFormatException` instead of returning null for invalid casts.

**Fix — SQL:**
```sql
-- Before (fails on invalid values in ANSI mode):
SELECT CAST(string_col AS INT) FROM table

-- After (returns null for invalid values, same as legacy behavior):
SELECT TRY_CAST(string_col AS INT) FROM table
```

**Fix — Scala:**
```scala
// Before:
df.withColumn("amount_int", $"amount_str".cast(IntegerType))

// After — add a null guard or use a when/otherwise:
df.withColumn("amount_int",
  when($"amount_str".rlike("^-?\\d+$"), $"amount_str".cast(IntegerType))
    .otherwise(lit(null).cast(IntegerType))
)
```

**Fix — PySpark:**
```python
# Before:
df = df.withColumn("amount_int", F.col("amount_str").cast("int"))

# After:
df = df.withColumn("amount_int",
    F.when(F.col("amount_str").rlike(r"^-?\d+$"), F.col("amount_str").cast("int"))
     .otherwise(F.lit(None).cast("int"))
)
```

#### Division by Zero

**Pattern:** Any division operation where the denominator could be zero.

**Risk:** ANSI mode throws `ArithmeticException` instead of returning null.

**Fix — SQL:**
```sql
-- Before:
SELECT total / count FROM table

-- After (option 1 — return null when dividing by zero):
SELECT TRY_DIVIDE(total, count) FROM table

-- After (option 2 — explicit guard):
SELECT CASE WHEN count = 0 THEN NULL ELSE total / count END FROM table
```

**Fix — Scala:**
```scala
// Before:
df.withColumn("rate", $"total" / $"count")

// After:
df.withColumn("rate",
  when($"count" =!= 0, $"total" / $"count")
    .otherwise(lit(null).cast(DoubleType))
)
```

**Fix — PySpark:**
```python
# Before:
df = df.withColumn("rate", F.col("total") / F.col("count"))

# After:
df = df.withColumn("rate",
    F.when(F.col("count") != 0, F.col("total") / F.col("count"))
     .otherwise(F.lit(None).cast("double"))
)
```

#### Integer Overflow

**Pattern:** Arithmetic on integer columns that could exceed `Int.MaxValue` or `Long.MaxValue`.

**Risk:** ANSI mode throws `ArithmeticException` instead of silently wrapping around.

**Fix:** Widen the type before the operation:
```sql
-- Before:
SELECT col_a * col_b FROM table  -- both are INT, product could overflow

-- After:
SELECT CAST(col_a AS BIGINT) * col_b FROM table
```

**Fix — Scala/PySpark:**
```scala
// Cast to long before multiplication
df.withColumn("product", $"col_a".cast(LongType) * $"col_b")
```

#### Array Index Out of Bounds

**Pattern:** Accessing array elements by index where the index may be out of range.

**Risk:** ANSI mode throws `ArrayIndexOutOfBoundsException` instead of returning null.

**Fix — SQL:**
```sql
-- Before:
SELECT array_col[5] FROM table

-- After:
SELECT TRY_ELEMENT_AT(array_col, 6) FROM table  -- note: element_at is 1-indexed
```

**Fix — Scala:**
```scala
// Before:
df.withColumn("val", $"array_col".getItem(5))

// After:
df.withColumn("val",
  when(size($"array_col") > 5, $"array_col".getItem(5))
    .otherwise(lit(null))
)
```

#### Map Key Not Found

**Pattern:** Accessing map values by key where the key may not exist.

**Risk:** ANSI mode throws `NoSuchElementException` instead of returning null.

**Fix — SQL:**
```sql
-- Before:
SELECT map_col['missing_key'] FROM table

-- After:
SELECT TRY_ELEMENT_AT(map_col, 'missing_key') FROM table
```

**Fix — Scala:**
```scala
// Before:
df.withColumn("val", $"map_col".getItem("key"))

// After — use element_at which returns null for missing keys:
df.withColumn("val", element_at($"map_col", lit("key")))
// Note: element_at also throws in ANSI mode. Use try_element_at if available,
// or guard with map_contains_key:
df.withColumn("val",
  when(map_contains_key($"map_col", lit("key")), $"map_col".getItem("key"))
    .otherwise(lit(null))
)
```

#### Scan Checklist

When upgrading a notebook, search for these patterns:

| Search For | Language | Potential Issue |
|-----------|----------|----------------|
| `.cast(` | Scala/PySpark | Type casting — use when/otherwise guard or TRY_CAST |
| `CAST(` | SQL | Type casting — replace with TRY_CAST |
| ` / ` (division) | All | Divide by zero — use TRY_DIVIDE or null guard |
| `.getItem(` | Scala/PySpark | Array/map access — add bounds/key check |
| `[` in SQL column expressions | SQL | Array/map access — use TRY_ELEMENT_AT |
| `* `, ` + `, ` - ` on int columns | All | Potential overflow — widen type if large values possible |

### Deprecated Configurations (Remove)

These configs were deprecated or removed between 13.3 and 16.4. Remove them from notebook code and cluster/job configs:

| Config | Status | Replacement |
|--------|--------|-------------|
| `spark.databricks.delta.retentionDurationCheck.enabled` | Deprecated | Use Delta table properties directly |
| `spark.sql.legacy.allowNonEmptyLocationInCTAS` | Removed | No replacement — CTAS always requires empty location |
| `spark.sql.legacy.setCommandRejectsSparkCoreConfs` | Removed | Spark core confs always accepted |

### New Features Available in 16.4

These aren't breaking changes but are worth adopting:

| Feature | Benefit | How to Use |
|---------|---------|------------|
| Predictive I/O | Faster reads for Delta | Enabled by default |
| Liquid Clustering | Replaces ZORDER | `ALTER TABLE ... CLUSTER BY (cols)` |
| `IDENTIFIER()` clause | Dynamic SQL without injection | `SELECT * FROM IDENTIFIER(:table_name)` |
| Python UDF improvements | 3-5x faster Python UDFs | Automatic — no code change needed |
| Variant type | Semi-structured data | `PARSE_JSON()`, `VARIANT` column type |
| Default column values | Table-level defaults | `ALTER TABLE ADD COLUMN x DEFAULT 0` |

### Delta Lake Changes

| Change | 13.3 | 16.4 | Action |
|--------|------|------|--------|
| Default protocol version | Higher in 16.4 | May auto-upgrade | Check `DESCRIBE DETAIL` after migration |
| Deletion vectors | Available | Default for new tables | Performance benefit, no code change |
| Row tracking | Not available | Available | Opt-in feature |
| Column mapping | Must specify mode | Default `name` mode for new tables | No action for existing tables |

### PySpark-Specific Changes

| Change | Impact | Action |
|--------|--------|--------|
| `pandas_udf` performance | Improved in 16.4 | No code change — just faster |
| `mapInPandas`/`applyInPandas` | More stable | No code change |
| Arrow-based UDFs | Default in 16.4 | May change UDF behavior slightly for edge cases with null handling |
| `spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled` | Default true | Better UDF error messages |

### Scala-Specific Changes

| Change | Impact | Action |
|--------|--------|--------|
| Scala version | 2.12.15 in 13.3 | 2.12.x in 16.4 (minor bump) | Generally compatible |
| Dataset API | Minor deprecations | Check for compiler warnings |
| `spark-monitoring` library | Not compatible | See separate migration plan if applicable |

## Upgrade Process

### Step 1: Audit Spark Configs

Search all notebooks and cluster configs for:
- Any `spark.conf.set` calls
- Any `spark.sql.` properties in cluster Spark Config
- Init scripts that set Spark properties

Flag any configs that changed defaults (see table above).

### Step 2: Add Compatibility Configs

At the top of the pipeline config notebook (or in the cluster Spark Config), add:

```
spark.sql.ansi.enabled false
```

This is the minimum required to preserve 13.3 behavior on 16.4. Add others only if the code uses the specific features affected.

### Step 3: Check for Deprecated APIs

Search notebooks for:
- Deprecated Spark SQL functions
- Removed config keys
- Legacy mode flags that no longer exist

### Step 4: Update Job Configurations

For Databricks jobs:
- Change the DBR version to 16.4 LTS
- Review cluster Spark Config for deprecated settings
- Update init scripts if they reference version-specific JARs or paths

### Step 5: Test

Run the pipeline on 16.4 and compare output to 13.3 baseline using the `conversion_validator` skill.

## Common Issues After Upgrade

| Symptom | Cause | Fix |
|---------|-------|-----|
| ArithmeticException on divide | ANSI mode enabled | Set `spark.sql.ansi.enabled=false` or fix divide-by-zero in code |
| NumberFormatException on cast | ANSI mode enabled | Set ANSI=false or use `try_cast()` |
| Different partition counts | AQE improvements | Usually better — only investigate if output differs |
| UDF returns different nulls | Arrow-based UDF changes | Check null handling in UDFs explicitly |
| "Config X not found" | Removed config | Remove the config from code |
| Delta protocol error | Auto-upgraded protocol | Downgrade protocol or accept new version |

## What Stays the Same

- All DataFrame API operations
- All Spark SQL syntax
- `dbutils` API
- Delta MERGE, UPDATE, DELETE syntax
- Window functions
- UDF registration API
- Notebook `%run` and `dbutils.notebook.run`
- All `spark.read` / `spark.write` operations
