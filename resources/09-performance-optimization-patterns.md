# Performance Optimization Patterns

This guide covers performance optimization opportunities during migration. The primary goal is migration fidelity — the original intent of the code must be preserved. Performance optimization is secondary but should be recommended where it doesn't alter behavior.

**Design principle:** All code changes should ensure that the original intent of the code is intact while biasing towards increased performance.

---

## 1. Single-Threaded Operations → Distributed

### Problem

Notebooks often contain Python loops that process data row-by-row using `.collect()` or `.toPandas()`. This pulls all data to the driver, processes it single-threaded, and throws away Spark's distributed execution.

### Detection

```regex
# Collect-then-loop patterns
\.collect\(\).*for\s+
for\s+\w+\s+in\s+.*\.collect\(\)
\.toPandas\(\).*\.apply\(
\.toPandas\(\).*\.iterrows\(
\.toPandas\(\).*for\s+

# Python-side aggregation
for\s+row\s+in\s+.*:.*\+=
```

### Fix Pattern: Loop with Collect → DataFrame Operations

```python
# BEFORE (single-threaded):
rows = df.select("member_id", "claim_amount").collect()
totals = {}
for row in rows:
    mid = row["member_id"]
    amt = row["claim_amount"]
    totals[mid] = totals.get(mid, 0) + (amt or 0)
result = spark.createDataFrame([(k, v) for k, v in totals.items()], ["member_id", "total"])

# AFTER (distributed):
result = df.groupBy("member_id").agg(
    F.sum(F.coalesce(F.col("claim_amount"), F.lit(0))).alias("total")
)
```

### Fix Pattern: toPandas Apply → pandas_udf

```python
# BEFORE (single-threaded):
pdf = df.toPandas()
pdf["risk_score"] = pdf.apply(lambda row: complex_scoring(row["age"], row["dx_count"]), axis=1)
result = spark.createDataFrame(pdf)

# AFTER (distributed with pandas_udf):
import pandas as pd

@F.pandas_udf(DoubleType())
def complex_scoring_udf(age: pd.Series, dx_count: pd.Series) -> pd.Series:
    # Vectorized version of complex_scoring
    base = age * 0.1 + dx_count * 5.0
    return base.clip(upper=100.0)

result = df.withColumn("risk_score", complex_scoring_udf(F.col("age"), F.col("dx_count")))
```

### Fix Pattern: Row-by-Row API Calls → mapInArrow Batches

```python
# BEFORE (single-threaded API calls):
results = []
for row in df.select("address").collect():
    geocoded = geocode_api(row["address"])  # HTTP call per row
    results.append(geocoded)

# AFTER (batched with mapInArrow):
import pyarrow as pa

def geocode_batch(batch_iter):
    for batch in batch_iter:
        pdf = batch.to_pandas()
        # Batch API call (many addresses at once)
        pdf["lat"], pdf["lon"] = zip(*pdf["address"].apply(geocode_api))
        yield pa.RecordBatch.from_pandas(pdf)

result_schema = StructType([
    StructField("address", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
])
result = df.select("address").mapInArrow(geocode_batch, result_schema)
```

### Recommendation Template

When Genie Code finds a single-threaded pattern, generate this recommendation:

```
PERFORMANCE RECOMMENDATION: Distribute single-threaded operation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Location: notebook_02_silver.py, cell 15
Pattern: .collect() followed by for loop
Estimated rows: ~2M (based on table stats)
Current approach: Collects all rows to driver, processes in Python loop
Recommended: Rewrite as Spark groupBy/agg operation

Risk: LOW — aggregation logic is equivalent
Performance impact: HIGH — eliminates driver bottleneck, enables parallel execution
Action required: Developer review and approval
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 2. Unnecessary Actions (Remove .count() Anti-Patterns)

### Problem

`.count()` triggers a full table scan. It's often used for logging or existence checks where cheaper alternatives exist.

### Detection

```regex
# Count for logging only
print.*\.count\(\)
log.*\.count\(\)
display.*\.count\(\)

# Count for existence check
\.count\(\)\s*>\s*0
\.count\(\)\s*==\s*0
\.count\(\)\s*!=\s*0
if\s+.*\.count\(\)

# Count followed by no use of the result
\w+\s*=\s*\w+\.count\(\)$
```

### Fix Patterns

```python
# BEFORE (full table scan for existence check):
if df.count() > 0:
    process(df)

# AFTER (stops at first row — O(1) instead of O(n)):
if df.first() is not None:
    process(df)
# OR:
if not df.isEmpty():  # Available in Spark 3.3+
    process(df)
# OR (if you need to check after filter):
if df.filter(condition).limit(1).count() > 0:
    process(df)

# BEFORE (count for logging — full scan):
print(f"Processing {df.count()} rows")
df = df.withColumn("processed", F.lit(True))

# AFTER (remove the count, or use an approximation):
# Option 1: Just remove — the count is informational only
df = df.withColumn("processed", F.lit(True))

# Option 2: If count is needed for metrics, do it once and cache:
row_count = df.count()  # Only if this value is used downstream
print(f"Processing {row_count} rows")

# BEFORE (double action — count then collect):
n = df.count()
if n > 0:
    rows = df.collect()  # Second scan

# AFTER (single action):
rows = df.collect()
if len(rows) > 0:
    # process rows
```

---

## 3. Cache/Persist Removal on Serverless

### Problem

`.persist()`, `.cache()`, and `CACHE TABLE` are either unsupported or counterproductive on serverless compute. Serverless manages its own caching and auto-scaling.

### Detection

```regex
# PySpark persist/cache
\.persist\(\)
\.cache\(\)
\.unpersist\(\)

# SQL cache
(?i)\bCACHE\s+(?:LAZY\s+)?TABLE\b
(?i)\bUNCACHE\s+TABLE\b
```

### Fix

```python
# BEFORE:
df = df.cache()
count = df.count()  # Trigger cache
df = df.withColumn("processed", F.lit(True))
result = df.groupBy("category").agg(F.sum("amount"))
df.unpersist()

# AFTER (serverless):
# Remove cache/unpersist — serverless auto-manages memory
df = df.withColumn("processed", F.lit(True))
result = df.groupBy("category").agg(F.sum("amount"))

# If the DataFrame was cached because it's reused multiple times,
# serverless handles this through its own execution engine.
# If performance degrades, consider materializing to a temp table instead:
df.write.mode("overwrite").saveAsTable("temp_catalog.schema.temp_table")
```

```sql
-- BEFORE:
CACHE TABLE claims_staging;
-- ... multiple queries against claims_staging ...
UNCACHE TABLE claims_staging;

-- AFTER:
-- Remove CACHE/UNCACHE. If the table is queried multiple times,
-- serverless will handle caching automatically.
-- If needed, use a temporary table:
CREATE OR REPLACE TABLE temp_catalog.schema.claims_staging AS SELECT ...;
```

---

## 4. VACUUM and Table Maintenance

### VACUUM on Serverless

```sql
-- VACUUM works the same on serverless as on classic compute:
VACUUM catalog.schema.table_name RETAIN 168 HOURS;

-- No code change needed for VACUUM operations.
-- For external tables, schedule regular VACUUM + OPTIMIZE since
-- Predictive Optimization is not available.
```

> **Note:** VACUUM LITE is available as a Public Preview feature that uses the transaction log for faster execution. Since Molina requires GA features only, use standard VACUUM. Monitor Databricks release notes for GA availability.

### Liquid Clustering vs ZORDER

```sql
-- BEFORE (classic compute — ZORDER):
OPTIMIZE catalog.schema.claims ZORDER BY (member_id, claim_date);

-- RECOMMENDATION (16.4+ / serverless — Liquid Clustering):
-- For NEW tables, use Liquid Clustering instead of ZORDER:
ALTER TABLE catalog.schema.claims CLUSTER BY (member_id, claim_date);

-- Then just run OPTIMIZE without ZORDER:
OPTIMIZE catalog.schema.claims;

-- For EXISTING tables with ZORDER:
-- Migration to Liquid Clustering is optional.
-- ZORDER continues to work. Recommend Liquid Clustering for new tables only.
-- Do NOT change existing ZORDER in the first migration pass — minimize changes.
```

### ANALYZE TABLE for External Tables

```sql
-- External tables don't get Predictive Optimization.
-- Run ANALYZE TABLE to help the query optimizer:
ANALYZE TABLE catalog.schema.external_table COMPUTE STATISTICS;
ANALYZE TABLE catalog.schema.external_table COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## 5. Remove Manual Partition and Shuffle Tuning

### Problem

Classic compute notebooks often set `spark.sql.shuffle.partitions` and use `.repartition()` / `.coalesce()` based on cluster size. Serverless auto-tunes these.

### Detection

```regex
# Manual shuffle partition setting
spark\.conf\.set.*shuffle\.partitions
(?i)SET\s+spark\.sql\.shuffle\.partitions

# Manual repartition
\.repartition\(\d+\)
\.coalesce\(\d+\)

# Manual partition control before write
\.repartition\(.*?\)\.write
\.coalesce\(.*?\)\.write
```

### Fix

```python
# BEFORE (classic — manual tuning for cluster size):
spark.conf.set("spark.sql.shuffle.partitions", "200")
df = df.repartition(200)
df.write.mode("overwrite").saveAsTable("output")

# AFTER (serverless — let auto-tuning handle it):
# Remove shuffle.partitions setting
# Remove repartition() unless it's for partitionBy() semantics
df.write.mode("overwrite").saveAsTable("output")

# EXCEPTION: .repartition() BY COLUMN for write partitioning is still valid:
df.repartition("date_col").write.partitionBy("date_col").mode("overwrite").saveAsTable("output")
# But even this may not be needed if using Liquid Clustering.
```

---

## 6. Arrow-Based UDF Optimization

### When to Recommend

If a standard Python UDF processes > 100K rows and does vectorizable operations, recommend converting to a `pandas_udf`.

### Performance Comparison

| UDF Type | Serialization | Processing | Typical Speedup |
|----------|--------------|------------|-----------------|
| Standard Python UDF | Row-by-row pickle | Single row Python | Baseline |
| pandas_udf (Series) | Arrow batches | Vectorized pandas | 3-10x |
| mapInArrow | Arrow batches | Custom batch processing | 5-20x |
| Native Spark function | No serialization | JVM execution | 50-100x |

### Conversion Pattern

```python
# Standard UDF (slow):
@F.udf(returnType=StringType())
def clean_phone(phone):
    if phone is None:
        return None
    digits = ''.join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return None

# pandas_udf (faster — vectorized string operations):
@F.pandas_udf(StringType())
def clean_phone(phone: pd.Series) -> pd.Series:
    digits = phone.str.replace(r'\D', '', regex=True)
    result = pd.Series([None] * len(phone), dtype=object)
    mask_10 = digits.str.len() == 10
    mask_11 = (digits.str.len() == 11) & (digits.str[0] == '1')
    result[mask_10] = '(' + digits[mask_10].str[:3] + ') ' + digits[mask_10].str[3:6] + '-' + digits[mask_10].str[6:]
    result[mask_11] = '(' + digits[mask_11].str[1:4] + ') ' + digits[mask_11].str[4:7] + '-' + digits[mask_11].str[7:]
    return result

# Best: Native Spark (fastest — no Python at all):
df = df.withColumn("clean_phone",
    F.when(
        F.length(F.regexp_replace(F.col("phone"), r"\D", "")) == 10,
        F.concat(
            F.lit("("), F.substring(F.regexp_replace(F.col("phone"), r"\D", ""), 1, 3),
            F.lit(") "), F.substring(F.regexp_replace(F.col("phone"), r"\D", ""), 4, 3),
            F.lit("-"), F.substring(F.regexp_replace(F.col("phone"), r"\D", ""), 7, 4)
        )
    )
)
```

---

## 7. REFRESH TABLE and MSCK REPAIR Removal

### Detection

```regex
(?i)\bREFRESH\s+TABLE\b
(?i)\bMSCK\s+REPAIR\s+TABLE\b
```

### Fix

```sql
-- BEFORE (classic compute):
REFRESH TABLE catalog.schema.my_table;
MSCK REPAIR TABLE catalog.schema.my_table;

-- AFTER (serverless):
-- Remove both. Serverless auto-handles table metadata refresh.
-- For external tables that need partition discovery:
-- Use ALTER TABLE ... ADD PARTITION instead of MSCK REPAIR.
```

---

## 8. Materialized View Migration

### Detection

```regex
(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b
(?i)\bREFRESH\s+MATERIALIZED\s+VIEW\b
```

### Fix

Materialized views CANNOT be created or refreshed on serverless general compute. They must use a SQL Warehouse.

```sql
-- BEFORE (classic compute notebook):
CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.mv_summary AS
SELECT member_id, COUNT(*) as claim_count
FROM catalog.schema.claims
GROUP BY member_id;

-- AFTER:
-- Option 1: Move to a SQL Warehouse task in the workflow
-- Option 2: Replace with a regular table that gets rebuilt:
CREATE OR REPLACE TABLE catalog.schema.summary AS
SELECT member_id, COUNT(*) as claim_count
FROM catalog.schema.claims
GROUP BY member_id;
```

---

## 9. Recommendation Priority Matrix

When generating performance recommendations, prioritize by impact and safety:

| Priority | Pattern | Impact | Risk |
|----------|---------|--------|------|
| 1 | Remove .collect() loops → Spark ops | Very High | Low (if logic equivalent) |
| 2 | Remove .count() for existence checks | High | None |
| 3 | Remove .cache()/.persist() on serverless | Medium | None |
| 4 | Remove manual shuffle/partition tuning | Medium | None |
| 5 | Convert standard UDF → native Spark | Very High | Medium (verify equivalence) |
| 6 | Convert standard UDF → pandas_udf | High | Low |
| 7 | Use Liquid Clustering instead of ZORDER | Medium | Low (new tables only) |
| 8 | Add ANALYZE TABLE for external tables | Medium | None |

**Rule:** Priority 1-4 are safe to apply during migration. Priority 5-8 should be recommended to the developer but not applied automatically unless confirmed.
