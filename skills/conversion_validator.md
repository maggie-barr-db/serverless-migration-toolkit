# Conversion Validator

This skill provides a complete 18-check validation framework for verifying that converted code (Scala to PySpark, DBR 13.3 to 16.4, classic to serverless, or any combination) produces output identical to the original pipeline. All validation code is inline and self-contained.

## Validation Approach

Run all checks in order for every table pair. Earlier checks are fast and narrow; later checks are thorough and expensive. If an earlier check fails, later checks provide the detail needed to diagnose the root cause.

**Check categories:**
- Checks 1-2: Structure (schema, row count) -- seconds to run
- Checks 3-5: Statistical (nulls, aggregates, distinct values) -- seconds to minutes
- Checks 6-7: Row-level (full data comparison, date deep dive) -- minutes
- Checks 8-12: Semantic equivalence (null confusion, type coercion, rounding, UDF behavior) -- minutes
- Checks 13-14: Edge cases and non-determinism -- minutes
- Checks 15-18: Serverless-specific (compute verification, config compliance, env key, performance) -- seconds to minutes

---

## Comparison Modes

### Mode 1: Cross-Catalog (Separate Schemas)

The original and converted pipelines write to different schemas. Compare across schemas.

**When to use:** During development and testing. Safest approach -- original data is untouched.

```python
original_df = spark.table("catalog.original_schema.table_name")
converted_df = spark.table("catalog.converted_schema.table_name")
```

### Mode 2: Time Travel (Same Table, Different Versions)

The converted pipeline overwrites the same tables. Compare the current version against a previous version using Delta time travel.

**When to use:** Production cutover validation.

```python
# Get baseline version BEFORE running converted pipeline
from delta.tables import DeltaTable
baseline_version = (
    DeltaTable.forName(spark, "catalog.schema.table_name")
    .history(1)
    .select("version")
    .collect()[0][0]
)

# After running converted pipeline:
original_df = (
    spark.read.format("delta")
    .option("versionAsOf", baseline_version)
    .table("catalog.schema.table_name")
)
converted_df = spark.table("catalog.schema.table_name")
```

Or via SQL:
```sql
DESCRIBE HISTORY catalog.schema.table_name LIMIT 1;
SELECT * FROM catalog.schema.table_name VERSION AS OF 3;
```

**Important:** Delta time travel requires retention period has not expired (default 30 days).

### Choosing a Mode

The validator notebook should accept a `mode` parameter:
- `mode = "cross_catalog"` -- provide `original_schema` and `converted_schema`
- `mode = "time_travel"` -- provide `schema` and `baseline_version`

All checks below work identically regardless of mode.

## Prerequisites

1. The **original pipeline** has been run and produced output tables
2. The **converted pipeline** has been run -- either to a separate schema or to the same tables
3. Both pipelines used the **same input data**
4. For time travel mode: baseline version number recorded before running converted pipeline

---

## Setup Code

Before running checks, build the joined DataFrame that most checks use.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType, DecimalType,
    StringType, DateType, TimestampType, BooleanType
)
import re

# --- Configuration (customize per table) ---
pk_col = "primary_key_column"  # Primary key column name

# Metadata columns to exclude from comparison (will always differ between runs)
exclude_cols = {
    "_ingestion_timestamp", "_transform_timestamp",
    "_source_file", "_row_hash"
}

# --- Column classification ---
compare_columns = [
    c for c in original_df.columns
    if c not in exclude_cols and c != pk_col
]

numeric_cols = [
    f.name for f in original_df.schema.fields
    if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))
    and f.name not in exclude_cols
]

string_cols = [
    f.name for f in original_df.schema.fields
    if isinstance(f.dataType, StringType) and f.name not in exclude_cols
]

date_cols = [
    f.name for f in original_df.schema.fields
    if isinstance(f.dataType, DateType) and f.name not in exclude_cols
]

timestamp_cols = [
    f.name for f in original_df.schema.fields
    if isinstance(f.dataType, TimestampType) and f.name not in exclude_cols
]

boolean_cols = [
    f.name for f in original_df.schema.fields
    if isinstance(f.dataType, BooleanType) and f.name not in exclude_cols
]

# String columns that likely contain dates (by name pattern)
string_date_cols = [
    c for c in string_cols
    if re.search(r'date|_dt$|_dob|_dod|_dos|timestamp|_time$', c, re.IGNORECASE)
]

# --- Build joined DataFrame (used by most checks) ---
joined = original_df.alias("o").join(
    converted_df.alias("c"),
    on=pk_col,
    how="full_outer"
)
```

---

## Structural Checks

### Check 1: Schema Comparison

Compare column names, types, and nullability.

```python
original_fields = {
    f.name: (str(f.dataType), f.nullable)
    for f in original_df.schema.fields if f.name not in exclude_cols
}
converted_fields = {
    f.name: (str(f.dataType), f.nullable)
    for f in converted_df.schema.fields if f.name not in exclude_cols
}

# Missing columns
only_original = set(original_fields.keys()) - set(converted_fields.keys())
only_converted = set(converted_fields.keys()) - set(original_fields.keys())

# Type mismatches
type_mismatches = {}
for col_name in set(original_fields.keys()) & set(converted_fields.keys()):
    if original_fields[col_name][0] != converted_fields[col_name][0]:
        type_mismatches[col_name] = (
            original_fields[col_name][0],
            converted_fields[col_name][0]
        )

# Nullable mismatches
nullable_mismatches = {}
for col_name in set(original_fields.keys()) & set(converted_fields.keys()):
    if original_fields[col_name][1] != converted_fields[col_name][1]:
        nullable_mismatches[col_name] = (
            original_fields[col_name][1],
            converted_fields[col_name][1]
        )
```

**Pass criteria:** No missing columns, no type mismatches. Nullable differences are informational warnings.

### Check 2: Row Count Comparison

```python
original_count = original_df.count()
converted_count = converted_df.count()

# Check for rows in one but not the other via the full outer join
only_in_original = joined.filter(F.col(f"c.{pk_col}").isNull()).count()
only_in_converted = joined.filter(F.col(f"o.{pk_col}").isNull()).count()
```

**Pass criteria:** Exact count match AND zero orphan rows in either direction.

---

## Statistical Checks

### Check 3: Null Count Comparison

Compare null counts per column. This is the first signal of UDF conversion issues.

```python
null_diffs = {}
for col_name in compare_columns:
    o_nulls = original_df.filter(F.col(col_name).isNull()).count()
    c_nulls = converted_df.filter(F.col(col_name).isNull()).count()
    if o_nulls != c_nulls:
        null_diffs[col_name] = (o_nulls, c_nulls, c_nulls - o_nulls)
```

**Pass criteria:** Identical null counts per column. Any difference indicates changed null handling.

### Check 4: Aggregate Comparison

Compare summary statistics for numeric columns.

```python
agg_diffs = {}
for col_name in numeric_cols:
    o_stats = original_df.agg(
        F.sum(col_name).alias("sum"),
        F.avg(col_name).alias("avg"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.stddev(col_name).alias("stddev"),
        F.count(F.when(F.col(col_name) == 0, True)).alias("zero_count"),
        F.count(F.when(F.col(col_name) < 0, True)).alias("negative_count")
    ).collect()[0]

    c_stats = converted_df.agg(
        F.sum(col_name).alias("sum"),
        F.avg(col_name).alias("avg"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max"),
        F.stddev(col_name).alias("stddev"),
        F.count(F.when(F.col(col_name) == 0, True)).alias("zero_count"),
        F.count(F.when(F.col(col_name) < 0, True)).alias("negative_count")
    ).collect()[0]

    diffs = {}
    # Exact match checks
    for stat in ["sum", "min", "max", "zero_count", "negative_count"]:
        o_val = o_stats[stat]
        c_val = c_stats[stat]
        if o_val != c_val:
            # Allow floating-point tolerance for sum/min/max
            if stat in ("sum", "min", "max") and o_val is not None and c_val is not None:
                if abs(float(o_val) - float(c_val)) > 1e-6:
                    diffs[stat] = (o_val, c_val)
            else:
                diffs[stat] = (o_val, c_val)

    # Tolerance checks
    for stat in ["avg", "stddev"]:
        o_val = o_stats[stat]
        c_val = c_stats[stat]
        if o_val is not None and c_val is not None:
            if abs(float(o_val) - float(c_val)) > 1e-6:
                diffs[stat] = (o_val, c_val)

    if diffs:
        agg_diffs[col_name] = diffs
```

**Pass criteria:** All stats within tolerance. Zero count and negative count must match exactly -- they often reveal ANSI mode differences.

### Check 5: Distinct Value Comparison

For string and categorical columns, compare distinct value sets.

```python
distinct_diffs = {}
for col_name in string_cols:
    o_distinct = set(
        row[0] for row in original_df.select(col_name).distinct().collect()
    )
    c_distinct = set(
        row[0] for row in converted_df.select(col_name).distinct().collect()
    )

    only_in_orig = o_distinct - c_distinct
    only_in_conv = c_distinct - o_distinct

    if only_in_orig or only_in_conv:
        distinct_diffs[col_name] = {
            "only_original": only_in_orig,
            "only_converted": only_in_conv
        }
```

**Pass criteria:** Identical distinct value sets per column.

---

## Row-Level Checks

### Check 6: Row-by-Row Data Comparison

Join on primary key and compare every column value.

```python
mismatch_summary = {}
for col_name in compare_columns:
    mismatches = joined.filter(
        ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
    )
    cnt = mismatches.count()
    if cnt > 0:
        mismatch_summary[col_name] = cnt
        # Show sample mismatches
        mismatches.select(
            pk_col,
            F.col(f"o.{col_name}").alias("original"),
            F.col(f"c.{col_name}").alias("converted")
        ).show(10, truncate=False)
```

**Key:** Use `eqNullSafe` -- regular `==` treats null == null as null (not true).

**Pass criteria:** Zero mismatches across all columns.

### Check 7: Date and Timestamp Deep Validation

#### 7a: Identify All Date/Timestamp Columns

```python
# Already classified in Setup section above
print(f"Date columns: {date_cols}")
print(f"Timestamp columns: {timestamp_cols}")
print(f"String date columns: {string_date_cols}")
```

#### 7b: Value-by-Value Date Comparison

```python
for col_name in date_cols:
    mismatches = joined.filter(
        ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
    )
    if mismatches.count() > 0:
        mismatches.select(
            pk_col,
            F.col(f"o.{col_name}").alias("original"),
            F.col(f"c.{col_name}").alias("converted")
        ).show(20, truncate=False)
```

#### 7c: Timezone Offset Detection

```python
for col_name in date_cols:
    shifted = joined.filter(
        F.col(f"o.{col_name}").isNotNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.abs(F.datediff(F.col(f"o.{col_name}"), F.col(f"c.{col_name}"))) == 1)
    )
    if shifted.count() > 0:
        print(f"WARNING: {col_name} has {shifted.count()} rows off by exactly 1 day -- likely timezone issue")

for col_name in timestamp_cols:
    hour_diff = joined.withColumn(
        "hour_diff",
        F.abs(
            (F.unix_timestamp(F.col(f"o.{col_name}"))
             - F.unix_timestamp(F.col(f"c.{col_name}")))
            / 3600
        )
    ).filter(F.col("hour_diff").between(0.5, 24))
    if hour_diff.count() > 0:
        print(f"WARNING: {col_name} has {hour_diff.count()} rows with hour-level offset")
        hour_diff.groupBy(
            F.round("hour_diff", 1).alias("hours_off")
        ).count().orderBy("hours_off").show()
```

#### 7d: Null vs Epoch Zero Confusion

```python
epoch_date = F.lit("1970-01-01").cast("date")
epoch_ts = F.lit("1970-01-01 00:00:00").cast("timestamp")

for col_name in date_cols:
    null_to_epoch = joined.filter(
        F.col(f"o.{col_name}").isNull()
        & (F.col(f"c.{col_name}") == epoch_date)
    ).count()
    epoch_to_null = joined.filter(
        (F.col(f"o.{col_name}") == epoch_date)
        & F.col(f"c.{col_name}").isNull()
    ).count()
    if null_to_epoch > 0:
        print(f"WARNING: {col_name} -- {null_to_epoch} rows where null became 1970-01-01")
    if epoch_to_null > 0:
        print(f"WARNING: {col_name} -- {epoch_to_null} rows where 1970-01-01 became null")
```

#### 7e: Date Boundary Cases

```python
for col_name in date_cols:
    for subset_name, subset_filter in [
        (
            "leap_year_feb29",
            (F.month(F.col(f"o.{col_name}")) == 2)
            & (F.dayofmonth(F.col(f"o.{col_name}")) == 29)
        ),
        (
            "month_end_28_31",
            F.dayofmonth(F.col(f"o.{col_name}")) >= 28
        ),
        (
            "year_boundary",
            (F.dayofyear(F.col(f"o.{col_name}")) <= 1)
            | (F.dayofyear(F.col(f"o.{col_name}")) >= 365)
        ),
    ]:
        subset = joined.filter(subset_filter)
        mismatches = subset.filter(
            ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
        )
        if mismatches.count() > 0:
            print(f"WARNING: {col_name} has {mismatches.count()} mismatches in {subset_name} dates")
```

#### 7f: Date Type Comparison

```python
for col_name in date_cols + timestamp_cols:
    o_type = str(original_df.schema[col_name].dataType)
    c_type = str(converted_df.schema[col_name].dataType)
    if o_type != c_type:
        print(f"TYPE MISMATCH: {col_name} is {o_type} in original but {c_type} in converted")
```

#### 7g: String Date Format Comparison (Bronze Layer)

```python
for col_name in string_date_cols:
    o_sample = [
        r[0] for r in original_df.filter(
            F.col(col_name).isNotNull()
        ).select(col_name).limit(10).collect()
    ]
    c_sample = [
        r[0] for r in converted_df.filter(
            F.col(col_name).isNotNull()
        ).select(col_name).limit(10).collect()
    ]
    print(f"{col_name} original samples: {o_sample}")
    print(f"{col_name} converted samples: {c_sample}")
```

**Pass criteria for Check 7:** All sub-checks pass with zero differences.

---

## Semantic Equivalence Checks

These checks catch silent data differences where the values are "close" but semantically wrong. They are specifically designed for the failure modes that occur during Scala to PySpark and DBR upgrade conversions.

### Check 8: Null vs Empty String Confusion

The most common silent bug in UDF conversion. Scala `null` and Python `None` both map to SQL null, but if a Python UDF returns `""` instead of `None`, the data looks similar but behaves differently in joins, filters, and aggregations.

```python
for col_name in string_cols:
    # Original has null, converted has empty string
    null_to_empty = joined.filter(
        F.col(f"o.{col_name}").isNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.trim(F.col(f"c.{col_name}")) == "")
    ).count()

    # Original has empty string, converted has null
    empty_to_null = joined.filter(
        F.col(f"o.{col_name}").isNotNull()
        & (F.trim(F.col(f"o.{col_name}")) == "")
        & F.col(f"c.{col_name}").isNull()
    ).count()

    # Original has null, converted has some default string value
    null_to_default = joined.filter(
        F.col(f"o.{col_name}").isNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.trim(F.col(f"c.{col_name}")) != "")
    )
    null_to_default_count = null_to_default.count()

    if null_to_empty > 0:
        print(f"SEMANTIC: {col_name} -- {null_to_empty} rows where null became empty string")
    if empty_to_null > 0:
        print(f"SEMANTIC: {col_name} -- {empty_to_null} rows where empty string became null")
    if null_to_default_count > 0:
        defaults = null_to_default.groupBy(
            F.col(f"c.{col_name}").alias("default_value")
        ).count().orderBy(F.desc("count"))
        print(f"SEMANTIC: {col_name} -- {null_to_default_count} rows where null became a default value:")
        defaults.show(10, truncate=False)
```

**Why this matters:**
- `WHERE col IS NULL` will not match empty strings -- changes filter counts
- `JOIN ON a.col = b.col` -- null != null (no match), but "" == "" (matches) -- changes join cardinality
- `COUNT(col)` counts non-null values -- empty strings are counted, nulls are not

**Pass criteria:** Zero null-to-empty-string conversions, zero null-to-default-value conversions.

### Check 9: Null vs Zero/Default Numeric Confusion

Same concept as Check 8 but for numeric columns. A UDF returning `0` or `0.0` instead of `None` changes aggregates.

```python
for col_name in numeric_cols:
    # Null became zero
    null_to_zero = joined.filter(
        F.col(f"o.{col_name}").isNull()
        & (F.col(f"c.{col_name}") == 0)
    ).count()

    # Zero became null
    zero_to_null = joined.filter(
        (F.col(f"o.{col_name}") == 0)
        & F.col(f"c.{col_name}").isNull()
    ).count()

    # Null became some other default
    null_to_other = joined.filter(
        F.col(f"o.{col_name}").isNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.col(f"c.{col_name}") != 0)
    ).count()

    if null_to_zero > 0:
        print(f"SEMANTIC: {col_name} -- {null_to_zero} rows where null became 0")
    if zero_to_null > 0:
        print(f"SEMANTIC: {col_name} -- {zero_to_null} rows where 0 became null")
    if null_to_other > 0:
        print(f"SEMANTIC: {col_name} -- {null_to_other} rows where null became a non-zero default")
```

**Why this matters:** `SUM(col)` ignores nulls but includes zeros. `AVG(col)` ignores nulls but divides by count including zeros. A column with 100 values and 10 nulls has AVG = sum/90. If nulls become zeros, AVG = sum/100 -- different result.

**Pass criteria:** Zero null-to-zero conversions, zero null-to-default conversions.

### Check 10: Boolean/Flag Column Equivalence

Check columns that represent flags, categories, or status values for semantic equivalence even when the string values differ.

```python
for col_name in string_cols:
    # Check for case differences (e.g., "NORMAL" vs "Normal" vs "normal")
    case_diff = joined.filter(
        F.col(f"o.{col_name}").isNotNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.col(f"o.{col_name}") != F.col(f"c.{col_name}"))
        & (F.upper(F.col(f"o.{col_name}")) == F.upper(F.col(f"c.{col_name}")))
    ).count()

    # Check for whitespace differences
    whitespace_diff = joined.filter(
        F.col(f"o.{col_name}").isNotNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.col(f"o.{col_name}") != F.col(f"c.{col_name}"))
        & (F.trim(F.col(f"o.{col_name}")) == F.trim(F.col(f"c.{col_name}")))
    ).count()

    if case_diff > 0:
        print(f"SEMANTIC: {col_name} -- {case_diff} rows differ only in case")
    if whitespace_diff > 0:
        print(f"SEMANTIC: {col_name} -- {whitespace_diff} rows differ only in whitespace")

# Also check actual boolean columns
for col_name in boolean_cols:
    mismatches = joined.filter(
        ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
    ).count()
    if mismatches > 0:
        print(f"BOOLEAN DIFF: {col_name} -- {mismatches} mismatches")
        joined.groupBy(
            F.col(f"o.{col_name}").alias("original"),
            F.col(f"c.{col_name}").alias("converted")
        ).count().show()
```

**Pass criteria:** Zero case differences, zero whitespace differences, zero boolean mismatches.

### Check 11: Numeric Precision and Rounding

Detect rounding differences caused by Scala BigDecimal HALF_UP vs Python banker's rounding, or float vs double precision.

```python
for col_name in numeric_cols:
    # Find rows that differ
    diffs = joined.filter(
        F.col(f"o.{col_name}").isNotNull()
        & F.col(f"c.{col_name}").isNotNull()
        & (F.col(f"o.{col_name}") != F.col(f"c.{col_name}"))
    )

    if diffs.count() > 0:
        # Categorize the magnitude of differences
        diff_analysis = diffs.withColumn(
            "abs_diff",
            F.abs(F.col(f"o.{col_name}") - F.col(f"c.{col_name}"))
        ).withColumn(
            "diff_category",
            F.when(F.col("abs_diff") < 1e-10, "floating_point_noise")
             .when(F.col("abs_diff") < 0.01, "rounding_difference")
             .when(F.col("abs_diff") < 1.0, "small_difference")
             .otherwise("large_difference")
        )

        print(f"PRECISION: {col_name} -- difference distribution:")
        diff_analysis.groupBy("diff_category").count().orderBy("diff_category").show()

        # Show samples of each category
        for cat in ["rounding_difference", "small_difference", "large_difference"]:
            samples = diff_analysis.filter(F.col("diff_category") == cat)
            if samples.count() > 0:
                print(f"\n  {cat} samples:")
                samples.select(
                    pk_col,
                    F.col(f"o.{col_name}").alias("original"),
                    F.col(f"c.{col_name}").alias("converted"),
                    "abs_diff"
                ).show(5, truncate=False)

        # Check for rounding pattern: values that differ by exactly ~0.01
        # (common with HALF_UP vs banker's rounding)
        rounding_pattern = diffs.filter(
            F.abs(
                F.col(f"o.{col_name}") - F.col(f"c.{col_name}")
            ).between(0.005, 0.015)
        ).count()
        if rounding_pattern > 0:
            print(
                f"  LIKELY ROUNDING ISSUE: {rounding_pattern} rows differ by ~0.01"
                " -- check BigDecimal/round() conversion"
            )
```

**Pass criteria:**
- `floating_point_noise` (< 1e-10): acceptable, ignore
- `rounding_difference` (< 0.01): investigate -- likely BigDecimal conversion issue
- `small_difference` (< 1.0): failure -- logic difference
- `large_difference` (>= 1.0): failure -- wrong calculation

### Check 12: UDF Output Consistency

For columns known to be produced by UDFs, run targeted checks. Customize the `udf_columns` dict per pipeline.

```python
# Define UDF-produced columns (customize per pipeline)
# Format: {"output_column_name": "UDF function name"}
udf_columns = {
    # "column_name": "udf_name",
}

for col_name, udf_name in udf_columns.items():
    if col_name not in compare_columns:
        continue

    print(f"\n=== UDF Check: {udf_name} -> {col_name} ===")

    # Count mismatches
    mismatches = joined.filter(
        ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
    )
    cnt = mismatches.count()

    if cnt == 0:
        print(f"  PASS: {cnt} mismatches")
        continue

    print(f"  FAIL: {cnt} mismatches")

    # Categorize mismatch types
    null_related = mismatches.filter(
        F.col(f"o.{col_name}").isNull() | F.col(f"c.{col_name}").isNull()
    ).count()

    value_different = mismatches.filter(
        F.col(f"o.{col_name}").isNotNull() & F.col(f"c.{col_name}").isNotNull()
    ).count()

    print(f"  Null-related mismatches: {null_related}")
    print(f"  Value-different mismatches: {value_different}")

    # Show the input values that caused mismatches (helps debug the UDF)
    mismatches.select(
        pk_col,
        F.col(f"o.{col_name}").alias("original_output"),
        F.col(f"c.{col_name}").alias("converted_output")
    ).show(10, truncate=False)
```

**Pass criteria:** Zero mismatches for all UDF-produced columns.

---

## Edge Case and Non-Determinism Checks

### Check 13: Edge Case Data Patterns

Check specific data patterns that commonly cause conversion failures.

```python
# 13a: Negative numbers in numeric columns
for col_name in numeric_cols:
    o_neg = original_df.filter(F.col(col_name) < 0).count()
    c_neg = converted_df.filter(F.col(col_name) < 0).count()
    if o_neg != c_neg:
        print(f"EDGE CASE: {col_name} -- negative count changed: {o_neg} -> {c_neg}")

# 13b: Empty strings vs nulls in all string columns
for col_name in string_cols:
    o_empty = original_df.filter(
        (F.col(col_name).isNotNull()) & (F.trim(F.col(col_name)) == "")
    ).count()
    c_empty = converted_df.filter(
        (F.col(col_name).isNotNull()) & (F.trim(F.col(col_name)) == "")
    ).count()
    o_null = original_df.filter(F.col(col_name).isNull()).count()
    c_null = converted_df.filter(F.col(col_name).isNull()).count()
    if o_empty != c_empty or o_null != c_null:
        print(f"EDGE CASE: {col_name} -- empty={o_empty}->{c_empty}, null={o_null}->{c_null}")

# 13c: Very large numbers (potential overflow)
for col_name in numeric_cols:
    o_large = original_df.filter(F.abs(F.col(col_name)) > 2147483647).count()
    c_large = converted_df.filter(F.abs(F.col(col_name)) > 2147483647).count()
    if o_large != c_large:
        print(
            f"EDGE CASE: {col_name} -- large value count changed: "
            f"{o_large} -> {c_large} (possible overflow)"
        )

# 13d: Special string values
for col_name in string_cols:
    for special_val, label in [
        ("N/A", "N/A"),
        ("null", "literal 'null'"),
        ("None", "literal 'None'"),
        ("", "empty string"),
        ("NaN", "NaN string"),
    ]:
        o_count = original_df.filter(F.col(col_name) == special_val).count()
        c_count = converted_df.filter(F.col(col_name) == special_val).count()
        if o_count != c_count:
            print(f"EDGE CASE: {col_name} -- '{label}' count: {o_count} -> {c_count}")

# 13e: NaN in numeric columns (NaN != NaN in normal comparison)
for col_name in numeric_cols:
    o_nan = original_df.filter(F.isnan(F.col(col_name))).count()
    c_nan = converted_df.filter(F.isnan(F.col(col_name))).count()
    if o_nan != c_nan:
        print(f"EDGE CASE: {col_name} -- NaN count: {o_nan} -> {c_nan}")

# 13f: Duplicate primary keys (should be 0, but verifies join correctness)
o_dupes = original_df.groupBy(pk_col).count().filter(F.col("count") > 1).count()
c_dupes = converted_df.groupBy(pk_col).count().filter(F.col("count") > 1).count()
if o_dupes > 0 or c_dupes > 0:
    print(
        f"WARNING: Duplicate PKs -- original={o_dupes}, converted={c_dupes}. "
        "Row-level comparisons may be unreliable."
    )
```

**Pass criteria:** All edge case counts match between original and converted.

### Check 14: Non-Determinism Detection

Identify columns where differences may be due to non-deterministic behavior rather than conversion bugs.

```python
# 14a: Check if mismatched rows correlate with non-deterministic operations
# (dropDuplicates, first(), head, collect without orderBy)
for col_name, cnt in mismatch_summary.items():
    if cnt > 0 and cnt < 10:  # Small number of mismatches suggests non-determinism
        mismatched_pks = joined.filter(
            ~F.col(f"o.{col_name}").eqNullSafe(F.col(f"c.{col_name}"))
        ).select(pk_col).collect()
        pk_values = [r[0] for r in mismatched_pks]

        print(f"NON-DETERMINISM CHECK: {col_name} has {cnt} mismatches")
        print(f"  Mismatched PKs: {pk_values[:10]}")

# 14b: For aggregate/gold tables, check if differences could be from
# floating-point aggregation order
for col_name in numeric_cols:
    if col_name in mismatch_summary and mismatch_summary[col_name] > 0:
        diffs = joined.filter(
            F.col(f"o.{col_name}").isNotNull()
            & F.col(f"c.{col_name}").isNotNull()
            & (F.col(f"o.{col_name}") != F.col(f"c.{col_name}"))
        ).withColumn(
            "rel_diff",
            F.when(
                F.col(f"o.{col_name}") != 0,
                F.abs(F.col(f"o.{col_name}") - F.col(f"c.{col_name}"))
                / F.abs(F.col(f"o.{col_name}"))
            ).otherwise(F.abs(F.col(f"c.{col_name}")))
        )

        max_rel_diff = diffs.agg(F.max("rel_diff")).collect()[0][0]
        if max_rel_diff and max_rel_diff < 1e-10:
            print(
                f"NON-DETERMINISM: {col_name} -- max relative diff is {max_rel_diff:.2e}, "
                "likely floating-point aggregation order"
            )
```

**Pass criteria:** Non-deterministic differences are flagged as informational, not failures. True failures have large or consistent differences.

---

## Serverless-Specific Checks (15-18)

These checks apply only when the migration target is serverless compute. Run them in addition to Checks 1-14.

### Check 15: Serverless Compute Verification

Confirm the migrated job actually ran on serverless compute, not classic.

```python
import requests

run_id = dbutils.widgets.get("run_id")
host = spark.conf.get("spark.databricks.workspaceUrl")
token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().apiToken().get()
)

response = requests.get(
    f"https://{host}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id}
)
run_details = response.json()

for task in run_details.get("tasks", []):
    task_key = task.get("task_key", "unknown")
    env_key = task.get("environment_key")
    if env_key:
        print(f"PASS: Task '{task_key}' ran with environment_key='{env_key}' (serverless)")
    elif task.get("cluster_instance", {}).get("cluster_id"):
        print(f"FAIL: Task '{task_key}' ran on classic cluster, not serverless")
    else:
        print(f"WARN: Task '{task_key}' -- unable to determine compute type")
```

**Pass criteria:** Every task has an `environment_key` and no `cluster_id`.

### Check 16: Config Compliance Check

Verify no unsupported Spark configs were set during the run. Unsupported configs on serverless either silently fail or throw `CONFIG_NOT_AVAILABLE`.

```python
unsupported_prefixes = [
    "spark.executor.",
    "spark.driver.extra",
    "spark.dynamicAllocation.",
    "spark.shuffle.service.",
    "spark.hadoop.",
    "spark.serializer",
    "spark.sql.warehouse.dir",
    "spark.databricks.cluster.",
    "spark.databricks.passthrough.",
    "fs.azure.",
    "fs.s3a.",
    "spark.databricks.delta.retentionDurationCheck.enabled",
    "spark.databricks.delta.schema.autoMerge.enabled",
    "spark.sql.broadcastTimeout",
    "spark.sql.caseSensitive",
    "spark.sql.streaming.stateStore.stateSchemaCheck",
]

# If checking post-run, verify the job completed without CONFIG_NOT_AVAILABLE errors
print("Check run logs for [CONFIG_NOT_AVAILABLE] errors")
print(f"Unsupported config prefixes to scan for: {len(unsupported_prefixes)}")
for prefix in unsupported_prefixes:
    print(f"  - {prefix}")
```

**Pass criteria:** No `CONFIG_NOT_AVAILABLE` errors in the run logs. No unsupported configs set in notebook code.

### Check 17: Environment Key Verification

Confirm every task in the job JSON has an `environment_key` and the environments block is properly configured.

```python
response = requests.get(
    f"https://{host}/api/2.1/jobs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"job_id": dbutils.widgets.get("job_id")}
)
job_config = response.json()

# Check tasks
tasks_without_env_key = []
for task in job_config.get("settings", {}).get("tasks", []):
    if "environment_key" not in task:
        tasks_without_env_key.append(task.get("task_key", "unknown"))

if tasks_without_env_key:
    print(f"FAIL: Tasks missing environment_key: {tasks_without_env_key}")
else:
    print(
        f"PASS: All {len(job_config['settings']['tasks'])} tasks have environment_key"
    )

# Check environments block
environments = job_config.get("settings", {}).get("environments", [])
if not environments:
    print("FAIL: No environments block in job config")
else:
    for env in environments:
        client = env.get("spec", {}).get("client", "unknown")
        deps = env.get("spec", {}).get("dependencies", [])
        print(
            f"PASS: Environment '{env.get('environment_key')}' -- "
            f"client={client}, dependencies={len(deps)}"
        )
        for dep in deps:
            if "%env_name%" in dep or "%env%" in dep:
                print(f"  FAIL: Unresolved placeholder in dependency: {dep}")
            else:
                print(f"  OK: {dep}")
```

**Pass criteria:** All tasks have `environment_key`. Environments block exists with client "4". No unresolved `%env_name%` placeholders.

### Check 18: Performance Comparison

Compare classic compute vs serverless execution metrics to detect regressions.

```python
classic_run_id = dbutils.widgets.get("classic_run_id")
serverless_run_id = dbutils.widgets.get("serverless_run_id")

def get_task_durations(rid):
    resp = requests.get(
        f"https://{host}/api/2.1/jobs/runs/get",
        headers={"Authorization": f"Bearer {token}"},
        params={"run_id": rid}
    )
    run = resp.json()
    durations = {}
    for task in run.get("tasks", []):
        task_key = task.get("task_key")
        start = task.get("start_time", 0)
        end = task.get("end_time", 0)
        if start and end:
            durations[task_key] = (end - start) / 1000 / 60  # minutes
    return durations

classic = get_task_durations(classic_run_id)
serverless = get_task_durations(serverless_run_id)

print("PERFORMANCE COMPARISON")
print(
    f"{'Task':<40} {'Classic (min)':<15} {'Serverless (min)':<18} "
    f"{'Ratio':<8} {'Status'}"
)
print("=" * 95)

regressions = 0
for task_key in sorted(set(list(classic.keys()) + list(serverless.keys()))):
    c_min = classic.get(task_key, 0)
    s_min = serverless.get(task_key, 0)
    ratio = s_min / c_min if c_min > 0 else float('inf')
    status = "PASS" if ratio <= 2.0 else "REGRESSION"
    if status == "REGRESSION":
        regressions += 1
    print(f"{task_key:<40} {c_min:<15.1f} {s_min:<18.1f} {ratio:<8.2f} {status}")

print(f"\nRegressions (>2x): {regressions}")
if regressions == 0:
    print("PASS: No significant performance regressions")
else:
    print(
        f"WARN: {regressions} task(s) with >2x runtime increase -- "
        "investigate before production cutover"
    )
```

**Pass criteria:** No task exceeds 2x the classic compute runtime. Minor increases (< 2x) are informational.

---

## Report Output Template

The validator should produce a comprehensive summary in this format:

```
========================================================================
              CONVERSION VALIDATION REPORT
  Table: catalog.schema.table_name
  Mode: Cross-Catalog (original_schema vs converted_schema)
========================================================================

STRUCTURAL CHECKS
  1. Schema Comparison:           PASS (N columns matched, 0 type mismatches)
  2. Row Count:                   PASS (N = N, 0 orphans)

STATISTICAL CHECKS
  3. Null Counts:                 PASS/FAIL
     - column_name: X -> Y (diff: Z)
  4. Aggregate Stats:             PASS/FAIL
     - column_name stat: X -> Y (diff: Z)
  5. Distinct Values:             PASS (N string columns matched)

ROW-LEVEL CHECKS
  6. Row-by-Row Comparison:       PASS/FAIL (N mismatches across M columns)
     - column_name: N mismatches
  7. Date/Timestamp Deep:         PASS/FAIL
     - 7a: N date cols, M timestamp cols identified
     - 7b: N value mismatches
     - 7c: N timezone shifts
     - 7d: N null/epoch confusion
     - 7e: N boundary mismatches
     - 7f: N type mismatches
     - 7g: N string date formats checked

SEMANTIC EQUIVALENCE CHECKS
  8. Null vs Empty String:        PASS/FAIL
     - column_name: N rows where null became "value"
  9. Null vs Zero/Default:        PASS/FAIL (N null-to-zero conversions)
 10. Boolean/Flag Equivalence:    PASS/FAIL (N case/whitespace diffs)
 11. Numeric Precision:           PASS/FAIL
     - column_name: N rows with rounding_difference (~0.01)
     - Likely cause: BigDecimal HALF_UP vs Python round()
 12. UDF Output Consistency:      PASS/FAIL
     - udf_name: N mismatches (pattern description)

EDGE CASE CHECKS
 13. Edge Case Patterns:          PASS/FAIL
     - Negative counts: matched/differed
     - Empty/null distribution: matched/differed
     - Large values: matched/differed
     - Special strings: matched/differed
     - NaN counts: matched/differed
 14. Non-Determinism:             INFO
     - Summary of any non-deterministic patterns detected

SERVERLESS CHECKS (if applicable)
 15. Compute Verification:        PASS/FAIL
 16. Config Compliance:           PASS/FAIL
 17. Environment Key:             PASS/FAIL
 18. Performance Comparison:      PASS/WARN

========================================================================
OVERALL: PASS/FAIL -- N checks failed

ROOT CAUSE SUMMARY:
  1. Description of root cause and fix
  2. Description of root cause and fix
========================================================================
```

---

## Columns to Exclude from Comparison

Always exclude these metadata columns -- they will differ between runs:
- `_ingestion_timestamp`
- `_transform_timestamp`
- `_source_file`
- `_row_hash`

---

## Failure Diagnosis Patterns

Use these patterns to correlate failures across checks and identify the root cause.

### Scala to PySpark Conversion Failures

| Checks That Fail | Pattern | Root Cause | Fix |
|---|---|---|---|
| 3 (nulls) + 8 (null/empty) | Null count decreased, empty string count increased | UDF returns `""` instead of `None` | Fix UDF: `return None` not `return ""` |
| 3 (nulls) + 8 (null/default) | Null count decreased, new default values appear | UDF uses `x if x else "default"` instead of `is not None` | Fix UDF: use `is not None` check |
| 4 (aggregates) + 9 (null/zero) | Sum changed, null count decreased | UDF returns `0` instead of `None` | Fix UDF: `return None` not `return 0` |
| 4 (aggregates) + 11 (precision) | Sum slightly different, individual rows off by ~0.01 | BigDecimal HALF_UP vs Python round() | Use `Decimal` with `ROUND_HALF_UP` |
| 5 (distinct) + 10 (case) | New distinct values that are case-variants | UDF or transform changed case handling | Check `.upper()` / `.lower()` calls |
| 6 (row-by-row) on many columns | Widespread mismatches | Boolean operator precedence bug | Check all `&`/`|` have parenthesized operands |
| 6 on single column, all rows | Every row in one column differs | Column renamed or swapped | Check `.alias()` conversion |
| 2 (row count) differs | Missing or extra rows | Filter condition changed due to null comparison | Check `.filter()` with null-sensitive conditions |
| 13 (edge: NaN) | NaN counts differ | Python `float('nan')` vs Spark NaN handling | Check UDF math that could produce NaN |

### DBR Upgrade Failures (13.3 to 16.4)

| Checks That Fail | Pattern | Root Cause | Fix |
|---|---|---|---|
| 3 (nulls) increase | More nulls in converted | ANSI TRY_CAST returns null where old CAST returned a value | Verify TRY_CAST is correct behavior |
| 4 (aggregates) change | Sum/avg changed | ANSI TRY_DIVIDE returns null where old divide returned null or division logic changed | Check all division fixes |
| 6 widespread | Many columns affected | Deprecated config removal changed behavior | Re-add config or fix code |
| 13 (negative counts) | Negative numbers disappeared | ANSI arithmetic exception caught differently | Check error handling around negative values |

### Combined Conversion + Upgrade

| Checks That Fail | How to Isolate | Method |
|---|---|---|
| Any check | Is it the language conversion or the runtime? | Compare PySpark on new DBR output against Scala on new DBR output. If they match, the runtime is the cause. If they differ, the language conversion is the cause. |
| Any check | Need cleaner test | Run PySpark on old DBR cluster, compare against Scala old DBR baseline. Passes = language conversion is clean. Fails = language conversion bug. |

---

## Validation Notebook Structure

Create a validation notebook with this structure:

1. **Config cell:** Define mode, schemas/versions, table pairs, primary keys, UDF column mappings
2. **Setup cell:** Load DataFrames, build joined DataFrame, classify columns (use Setup Code above)
3. **Structural checks:** Checks 1-2
4. **Statistical checks:** Checks 3-5
5. **Row-level checks:** Checks 6-7
6. **Semantic equivalence checks:** Checks 8-12
7. **Edge case checks:** Checks 13-14
8. **Serverless checks (if applicable):** Checks 15-18
9. **Summary:** Print the full report with pass/fail per check using the template above
10. **Root cause analysis:** For failures, correlate across checks using the diagnosis patterns to identify the UDF or transform that caused the issue
11. **Detail output:** For failures, show sample mismatched rows with input context

The notebook should be parameterized so it can be reused across different conversion runs.
