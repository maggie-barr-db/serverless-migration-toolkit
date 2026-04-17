# Path A: Scala on DBR 13.3 LTS to Scala on DBR 16.4 LTS Migration Guide

> **Scope:** Runtime upgrade only. No language change, no serverless migration.
> **Source:** Databricks Runtime 13.3 LTS (Spark 3.4.1, Scala 2.12.15)
> **Target:** Databricks Runtime 16.4 LTS (Spark 3.5.x, Scala 2.12.18)
> **Audience:** Genie Code (automated migration agent) and Molina Healthcare engineers
> **Healthcare Data Warning:** Silent data changes are unacceptable. Every transformation must be validated for data equivalence.

---

## Table of Contents

1. [Spark Version Changes](#1-spark-version-changes)
2. [ANSI Mode Migration](#2-ansi-mode-migration)
3. [Deprecated and Removed Spark Configurations](#3-deprecated-and-removed-spark-configurations)
4. [Deprecated APIs](#4-deprecated-apis)
5. [Delta Lake Changes](#5-delta-lake-changes)
6. [PySpark-Specific Changes](#6-pyspark-specific-changes)
7. [Scala-Specific Changes](#7-scala-specific-changes)
8. [ML Runtime Migration](#8-ml-runtime-migration)
9. [Package and Dependency Changes](#9-package-and-dependency-changes)
10. [Spark Config Scan Checklist](#10-spark-config-scan-checklist)
11. [New Features Available in 16.4](#11-new-features-available-in-164)
12. [Common Issues After Upgrade](#12-common-issues-after-upgrade)
13. [Step-by-Step Migration Process](#13-step-by-step-migration-process)
14. [Documentation Links](#14-documentation-links)

---

## 1. Spark Version Changes

### 1.1 Spark 3.4.1 to 3.5.x Behavioral Changes

#### 1.1.1 Adaptive Query Execution (AQE) Changes

AQE has additional optimizations enabled by default in Spark 3.5.x. This can change partition counts, join strategies, and sort behaviors.

**Detect:**
```regex
\.repartition\(\d+\)|\bcoalesce\(\d+\)|\bspark\.sql\.shuffle\.partitions\b
```

**Impact:** Queries may produce different partition counts. Output data is logically equivalent but physical partitioning differs. If downstream consumers depend on exact partition counts (e.g., file counts for a downstream system), this matters.

**Changed defaults:**
| Config | 13.3 Default | 16.4 Default | Notes |
|--------|-------------|-------------|-------|
| `spark.sql.adaptive.enabled` | `true` | `true` | No change |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | No change |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | No change |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | `true` | `true` | More aggressive in 3.5 |

**Fix:** If exact partition count is required:
```scala
// Before: relied on shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

// After: explicitly repartition at the end if file count matters
df.repartition(200).write.format("delta").save(path)
```

#### 1.1.2 SQL Parser Strictness

Spark 3.5 is stricter about SQL syntax. Some previously-tolerated syntax errors now fail.

**Detect:**
```regex
spark\.sql\(|%sql
```

**Known issues:**
- Trailing commas in SELECT lists now produce errors in some contexts
- Unquoted reserved words used as identifiers may fail
- `GROUP BY` with ordinal references has stricter validation

**Fix:**
```sql
-- Before (13.3 tolerated trailing comma):
SELECT col1, col2, FROM my_table

-- After (16.4 requires clean syntax):
SELECT col1, col2 FROM my_table
```

#### 1.1.3 Timestamp and Date Handling

Spark 3.5 tightens timestamp handling for edge cases.

**Detect:**
```regex
TimestampType|DateType|to_timestamp|to_date|from_unixtime|unix_timestamp|date_format|current_timestamp
```

**Changed behaviors:**
- `from_unixtime` with negative values: behavior may differ on edge-case timestamps before epoch
- Daylight saving time edge cases: more consistent handling but different from 3.4 for ambiguous times
- `to_timestamp` with invalid formats: throws `SparkDateTimeException` under ANSI mode (see Section 2)

#### 1.1.4 Null Handling in Aggregations

**Detect:**
```regex
\.agg\(|\.groupBy\(|GROUP BY|collect_list|collect_set|array_agg
```

**Changed behavior:**
- `collect_list` and `collect_set` null handling is now consistent under ANSI mode
- `DISTINCT` in aggregations has more consistent null behavior

#### 1.1.5 Join Behavior Changes

**Detect:**
```regex
\.join\(|JOIN\s|CROSS\s+JOIN|NATURAL\s+JOIN
```

**Changed behaviors:**
- Ambiguous column references in joins are now stricter
- `USING` clause duplicate elimination is more consistent
- Cross join detection is stricter (accidental cross joins may fail instead of silently executing)

### 1.2 Scala 2.12.15 to 2.12.18 Minor Changes

The Scala minor version bump is largely backward compatible. Changes are mostly bug fixes.

**Key differences:**

| Area | 2.12.15 | 2.12.18 | Impact |
|------|---------|---------|--------|
| Pattern matching exhaustiveness | Less strict warnings | Improved warnings | Compilation warnings may appear |
| Implicit resolution | Minor edge cases | Bug fixes | Extremely unlikely to affect user code |
| Collections | No changes | Bug fixes | No user-visible impact |
| Reflection | Minor bugs | Fixes | Only affects code using `scala.reflect` |

**Detect (reflection usage):**
```regex
scala\.reflect|classOf\[|TypeTag|ClassTag|ru\.typeOf
```

**Fix:** No code changes required in most cases. New compiler warnings should be reviewed but are informational.

---

## 2. ANSI Mode Migration

> **CRITICAL:** ANSI mode is ON by default in DBR 16.4. This is the single most impactful change.
>
> **NEVER** set `spark.sql.ansi.enabled = false` as a permanent fix. This masks data quality issues that are unacceptable in healthcare data. Always use ANSI-safe code patterns.

### 2.1 Overview

| Behavior | ANSI OFF (13.3 default) | ANSI ON (16.4 default) |
|----------|------------------------|----------------------|
| Invalid CAST | Returns `null` | Throws `SparkNumberFormatException` |
| Division by zero | Returns `null` | Throws `SparkArithmeticException` |
| Integer overflow | Wraps around silently | Throws `SparkArithmeticException` |
| Array out of bounds | Returns `null` | Throws `SparkArrayIndexOutOfBoundsException` |
| Map key not found | Returns `null` | Throws `SparkNoSuchElementException` |
| Invalid date/timestamp parse | Returns `null` | Throws `SparkDateTimeException` |

### 2.2 Type Casting (CAST to TRY_CAST)

**Detect (SQL):**
```regex
\bCAST\s*\(
```

**Detect (Scala):**
```regex
\.cast\(|\.as\[|Encoders\.|\.selectExpr\(.*[Cc][Aa][Ss][Tt]
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Returns null for invalid data
SELECT CAST(member_id AS INT) FROM claims
-- If member_id = 'ABC123', returns null
```

```scala
// Scala: Returns null for invalid data
df.withColumn("member_id_int", col("member_id").cast(IntegerType))
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Use TRY_CAST for safe conversion
SELECT TRY_CAST(member_id AS INT) FROM claims
-- If member_id = 'ABC123', returns null (same behavior as before)
```

```scala
// Scala: Use try_cast function
import org.apache.spark.sql.functions.{expr, try_cast => tryCast}

// Option 1: Using expr
df.withColumn("member_id_int", expr("TRY_CAST(member_id AS INT)"))

// Option 2: Using selectExpr
df.selectExpr("TRY_CAST(member_id AS INT) AS member_id_int")

// Option 3: If you KNOW the data is clean, keep cast() -- it will validate at runtime
// This is actually SAFER for healthcare data since it catches bad data
df.withColumn("member_id_int", col("member_id").cast(IntegerType))
// Throws exception if data is invalid -- consider this a FEATURE for data quality
```

**Healthcare guidance:** For columns where data MUST be valid (claim amounts, member IDs, procedure codes), consider keeping `CAST` and handling the exception upstream. For columns where nulls are acceptable on invalid data (optional fields, display-only fields), use `TRY_CAST`.

**Comprehensive CAST patterns to check:**

```regex
# SQL patterns
\bCAST\s*\(\s*\w+\s+AS\s+(INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\s*\)

# Scala patterns
\.cast\(\s*(IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*\)
\.cast\(\s*"(int|integer|bigint|smallint|tinyint|float|double|decimal|numeric|date|timestamp|boolean)"\s*\)
```

### 2.3 Division by Zero (TRY_DIVIDE or Null Guards)

**Detect (SQL):**
```regex
\b\w+\s*/\s*\w+|\bDIVIDE\s*\(|\/\s*\w+
```

**Detect (Scala):**
```regex
\.divide\(|col\(.*\)\s*/\s*col\(|\.\/\s*|expr\(.*\/
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Returns null when denominator is 0
SELECT paid_amount / billed_amount AS payment_ratio FROM claims
```

```scala
// Scala: Returns null when denominator is 0
df.withColumn("payment_ratio", col("paid_amount") / col("billed_amount"))
```

**After (16.4 - ANSI ON):**
```sql
-- SQL Option 1: TRY_DIVIDE (returns null on divide by zero)
SELECT TRY_DIVIDE(paid_amount, billed_amount) AS payment_ratio FROM claims

-- SQL Option 2: Null guard with CASE
SELECT
  CASE WHEN billed_amount = 0 OR billed_amount IS NULL THEN NULL
       ELSE paid_amount / billed_amount
  END AS payment_ratio
FROM claims

-- SQL Option 3: NULLIF pattern (compact)
SELECT paid_amount / NULLIF(billed_amount, 0) AS payment_ratio FROM claims
```

```scala
// Scala Option 1: TRY_DIVIDE via expr
df.withColumn("payment_ratio", expr("TRY_DIVIDE(paid_amount, billed_amount)"))

// Scala Option 2: when/otherwise null guard
import org.apache.spark.sql.functions.{when, lit}
df.withColumn("payment_ratio",
  when(col("billed_amount") === 0 || col("billed_amount").isNull, lit(null))
    .otherwise(col("paid_amount") / col("billed_amount"))
)

// Scala Option 3: NULLIF pattern via expr
df.withColumn("payment_ratio", col("paid_amount") / expr("NULLIF(billed_amount, 0)"))
```

### 2.4 Integer Overflow (Type Widening)

**Detect:**
```regex
IntegerType|ShortType|ByteType|\.cast\(\s*"int"\s*\)|CAST\s*\(\s*\w+\s+AS\s+INT\b|\+\s*\d{5,}|\*\s*\d{3,}
```

**Before (13.3 - ANSI OFF):**
```scala
// Silent wraparound: 2147483647 + 1 = -2147483648
val df = spark.sql("SELECT CAST(2147483647 AS INT) + 1 AS overflow_test")
// Returns -2147483648 (WRONG - silent data corruption)
```

**After (16.4 - ANSI ON):**
```scala
// Throws SparkArithmeticException: integer overflow

// Fix Option 1: Widen type to BIGINT
val df = spark.sql("SELECT CAST(2147483647 AS BIGINT) + 1 AS overflow_test")
// Returns 2147483648 (CORRECT)

// Fix Option 2: In DataFrame API, cast before arithmetic
df.withColumn("result",
  col("large_int").cast(LongType) + lit(1L)
)

// Fix Option 3: Use TRY_ADD (Spark 3.5+)
val df = spark.sql("SELECT TRY_ADD(CAST(2147483647 AS INT), 1) AS overflow_test")
// Returns null (safe, but may not be desired)
```

**Healthcare context:** Claim IDs, sequence numbers, and aggregate counts can exceed INT range. Always use `BIGINT`/`LongType` for:
- Claim/encounter counts across large populations
- Dollar amounts multiplied by 100 (for cents representation)
- Row counts from large tables
- Any accumulator or running total

### 2.5 Array Out of Bounds (TRY_ELEMENT_AT or Bounds Check)

**Detect (SQL):**
```regex
\w+\[\d+\]|element_at\s*\(|ELEMENT_AT\s*\(
```

**Detect (Scala):**
```regex
\.getItem\(|element_at\(|\.apply\(\d+\)|\(\d+\)
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Returns null for out-of-bounds access
SELECT diagnosis_codes[5] FROM encounters
-- If array has only 3 elements, returns null
```

```scala
// Scala: Returns null for out-of-bounds
df.withColumn("diag_6", col("diagnosis_codes").getItem(5))
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Use TRY_ELEMENT_AT (1-indexed in SQL)
SELECT TRY_ELEMENT_AT(diagnosis_codes, 6) FROM encounters
-- Returns null for out-of-bounds

-- SQL: Use size check
SELECT
  CASE WHEN SIZE(diagnosis_codes) >= 6 THEN diagnosis_codes[5]
       ELSE NULL
  END AS diag_6
FROM encounters
```

```scala
// Scala: Use TRY_ELEMENT_AT via expr (1-indexed)
df.withColumn("diag_6", expr("TRY_ELEMENT_AT(diagnosis_codes, 6)"))

// Scala: Bounds check
import org.apache.spark.sql.functions.{size, when}
df.withColumn("diag_6",
  when(size(col("diagnosis_codes")) >= 6, col("diagnosis_codes").getItem(5))
    .otherwise(lit(null))
)
```

**Note:** `element_at` uses 1-based indexing in SQL but `getItem` uses 0-based indexing in DataFrame API. `TRY_ELEMENT_AT` is 1-based in both SQL and expr contexts.

### 2.6 Map Key Not Found (TRY_ELEMENT_AT or map_contains_key)

**Detect (SQL):**
```regex
\w+\[['"][\w]+['"]\]|element_at\s*\(|map_keys\s*\(
```

**Detect (Scala):**
```regex
\.getItem\(|\.getField\(|element_at\(
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Returns null for missing key
SELECT provider_attributes['npi'] FROM providers
-- If key 'npi' doesn't exist, returns null
```

```scala
// Scala: Returns null for missing key
df.withColumn("npi", col("provider_attributes").getItem("npi"))
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Use TRY_ELEMENT_AT
SELECT TRY_ELEMENT_AT(provider_attributes, 'npi') FROM providers

-- SQL: Use map_contains_key guard
SELECT
  CASE WHEN map_contains_key(provider_attributes, 'npi')
       THEN provider_attributes['npi']
       ELSE NULL
  END AS npi
FROM providers
```

```scala
// Scala: TRY_ELEMENT_AT via expr
df.withColumn("npi", expr("TRY_ELEMENT_AT(provider_attributes, 'npi')"))

// Scala: Null-safe access with map_contains_key (available in Spark 3.5+)
// Note: For DataFrames, the getItem method on MapType still throws under ANSI mode
// Safest approach is expr-based TRY_ELEMENT_AT
```

### 2.7 Boolean Comparisons

**Detect:**
```regex
boolean_col\s*=\s*[01]|=\s*true\b|=\s*false\b|\bWHERE\s+\w+\s*=\s*1\b|\bWHERE\s+\w+\s*=\s*0\b
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Implicit boolean-to-integer comparison worked
SELECT * FROM members WHERE is_active = 1
SELECT * FROM members WHERE is_active = 0
```

```scala
// Scala: Comparing boolean to integer worked
df.filter(col("is_active") === 1)
df.filter(col("is_active") === 0)
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Use proper boolean syntax
SELECT * FROM members WHERE is_active IS TRUE
SELECT * FROM members WHERE is_active IS FALSE
SELECT * FROM members WHERE is_active IS NOT TRUE  -- includes null and false

-- Or explicit boolean literals
SELECT * FROM members WHERE is_active = TRUE
SELECT * FROM members WHERE is_active = FALSE
```

```scala
// Scala: Use proper boolean comparisons
df.filter(col("is_active") === true)
df.filter(col("is_active") === false)
df.filter(col("is_active").isNotNull && col("is_active") === true)
```

### 2.8 String to Numeric Implicit Conversions

**Detect:**
```regex
\bWHERE\s+\w+\s*[><=]+\s*'?\d+'?|string_col\s*[+\-*/]\s*\d|col\(.*\)\s*[+\-*/]\s*lit\(
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Implicit string-to-number comparison
SELECT * FROM claims WHERE claim_amount > '1000'
-- String '1000' silently converted to number
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Explicit conversion required
SELECT * FROM claims WHERE TRY_CAST(claim_amount AS DOUBLE) > 1000

-- Or if column IS numeric but compared to string literal:
SELECT * FROM claims WHERE claim_amount > 1000
-- (Remove quotes around numeric literals)
```

```scala
// Scala: Ensure types match
// Before:
df.filter(col("claim_amount") > "1000")

// After: Use proper numeric literal
df.filter(col("claim_amount") > 1000)

// Or if claim_amount is StringType:
df.filter(expr("TRY_CAST(claim_amount AS DOUBLE)") > 1000)
```

### 2.9 Date/Timestamp Parsing with Invalid Values

**Detect (SQL):**
```regex
\bto_timestamp\s*\(|to_date\s*\(|date_format\s*\(|from_unixtime\s*\(|unix_timestamp\s*\(
```

**Detect (Scala):**
```regex
to_timestamp\(|to_date\(|date_format\(|from_unixtime\(|unix_timestamp\(
```

**Before (13.3 - ANSI OFF):**
```sql
-- SQL: Returns null for invalid date strings
SELECT to_timestamp('not-a-date', 'yyyy-MM-dd') AS parsed_date
-- Returns null

SELECT to_date('02/30/2024', 'MM/dd/yyyy') AS parsed_date
-- Returns null (Feb 30 doesn't exist)
```

```scala
// Scala: Returns null for invalid parse
df.withColumn("service_date", to_timestamp(col("date_str"), "yyyy-MM-dd"))
```

**After (16.4 - ANSI ON):**
```sql
-- SQL: Use try_to_timestamp / try_to_date (available in Spark 3.5+)
SELECT try_to_timestamp('not-a-date', 'yyyy-MM-dd') AS parsed_date
-- Returns null (safe)

SELECT try_to_timestamp('02/30/2024', 'MM/dd/yyyy') AS parsed_date
-- Returns null (safe)

-- For to_date equivalent:
SELECT try_to_date('not-a-date', 'yyyy-MM-dd') AS parsed_date
-- Returns null (safe)

-- ALTERNATIVE: Use make_date/make_timestamp for known components
SELECT make_date(2024, 2, 28) AS valid_date
```

```scala
// Scala: Use try_to_timestamp via expr
df.withColumn("service_date", expr("try_to_timestamp(date_str, 'yyyy-MM-dd')"))

// Or for to_date:
df.withColumn("service_date", expr("try_to_date(date_str, 'yyyy-MM-dd')"))
```

**Healthcare critical dates to protect:**
- Date of birth (some legacy records have '00/00/0000' or '99/99/9999')
- Date of service
- Date of death
- Enrollment effective/termination dates
- Authorization dates

### 2.10 ANSI Mode Regex Detection Patterns (Complete Set)

Use these patterns to scan all notebooks before migration:

```python
# Complete ANSI detection patterns for automated scanning
ANSI_PATTERNS = {
    "CAST_SQL": {
        "pattern": r"\bCAST\s*\(",
        "severity": "CRITICAL",
        "description": "CAST will throw on invalid data under ANSI mode",
        "fix": "Replace with TRY_CAST where null-on-failure is desired"
    },
    "CAST_SCALA": {
        "pattern": r"\.cast\(",
        "severity": "CRITICAL",
        "description": "DataFrame cast() throws on invalid data under ANSI mode",
        "fix": "Use expr(\"TRY_CAST(...)\") or validate data upstream"
    },
    "DIVISION_SQL": {
        "pattern": r"(?<!\w)/(?!\*)\s*(?!\s*\*)",
        "severity": "HIGH",
        "description": "Division by zero throws under ANSI mode",
        "fix": "Use TRY_DIVIDE() or NULLIF() on denominator"
    },
    "DIVISION_SCALA": {
        "pattern": r"col\([^)]+\)\s*/\s*col\(|\.divide\(",
        "severity": "HIGH",
        "description": "DataFrame division throws on zero denominator",
        "fix": "Use when/otherwise guard or expr(\"TRY_DIVIDE(...)\")"
    },
    "ARRAY_ACCESS_SQL": {
        "pattern": r"\w+\s*\[\s*\d+\s*\]",
        "severity": "HIGH",
        "description": "Array index access throws on out-of-bounds under ANSI mode",
        "fix": "Use TRY_ELEMENT_AT() (1-indexed)"
    },
    "ARRAY_ACCESS_SCALA": {
        "pattern": r"\.getItem\(\s*\d+\s*\)",
        "severity": "HIGH",
        "description": "getItem() throws on out-of-bounds under ANSI mode",
        "fix": "Use expr(\"TRY_ELEMENT_AT(col, idx)\") with 1-based index"
    },
    "ELEMENT_AT": {
        "pattern": r"\belement_at\s*\(",
        "severity": "HIGH",
        "description": "element_at throws on missing key/index under ANSI mode",
        "fix": "Replace with TRY_ELEMENT_AT"
    },
    "MAP_ACCESS": {
        "pattern": r"\w+\s*\[\s*['\"]",
        "severity": "HIGH",
        "description": "Map bracket access throws on missing key under ANSI mode",
        "fix": "Use TRY_ELEMENT_AT or map_contains_key guard"
    },
    "BOOLEAN_INT_COMPARE": {
        "pattern": r"(?:WHERE|WHEN|AND|OR)\s+\w+\s*=\s*[01]\b",
        "severity": "MEDIUM",
        "description": "Boolean-to-integer comparison may fail under ANSI mode",
        "fix": "Use IS TRUE / IS FALSE or = TRUE / = FALSE"
    },
    "TO_TIMESTAMP": {
        "pattern": r"\bto_timestamp\s*\(",
        "severity": "HIGH",
        "description": "to_timestamp throws on invalid input under ANSI mode",
        "fix": "Replace with try_to_timestamp"
    },
    "TO_DATE": {
        "pattern": r"\bto_date\s*\(",
        "severity": "HIGH",
        "description": "to_date throws on invalid input under ANSI mode",
        "fix": "Replace with try_to_date"
    },
    "TO_NUMBER": {
        "pattern": r"\bto_number\s*\(",
        "severity": "HIGH",
        "description": "to_number throws on invalid input under ANSI mode",
        "fix": "Replace with try_to_number"
    },
    "STRING_NUMERIC_COMPARE": {
        "pattern": r"(?:WHERE|WHEN|AND|OR)\s+\w+\s*[><=!]+\s*'[0-9]+'",
        "severity": "MEDIUM",
        "description": "String-to-numeric comparison may fail under ANSI mode",
        "fix": "Remove quotes from numeric literals or use explicit TRY_CAST"
    },
    "INTEGER_OVERFLOW_RISK": {
        "pattern": r"(?:IntegerType|ShortType|ByteType|CAST\s*\(\s*\w+\s+AS\s+(?:INT|SMALLINT|TINYINT))",
        "severity": "MEDIUM",
        "description": "Integer arithmetic may overflow and throw under ANSI mode",
        "fix": "Widen to BIGINT/LongType for accumulations and large values"
    },
    "SUM_OVERFLOW": {
        "pattern": r"\bSUM\s*\(\s*(?:CAST\s*\([^)]+AS\s+INT(?:EGER)?\s*\)|(?!\w*(?:BIGINT|LONG)))",
        "severity": "MEDIUM",
        "description": "SUM of INT column can overflow under ANSI mode",
        "fix": "Cast to BIGINT before SUM: SUM(CAST(col AS BIGINT))"
    }
}
```

---

## 3. Deprecated and Removed Spark Configurations

### 3.1 Configs Removed Between 13.3 and 16.4

| Config | Status in 16.4 | Replacement | Impact |
|--------|----------------|-------------|--------|
| `spark.sql.legacy.createHiveTableByDefault` | Removed | Tables are always Delta by default on Databricks | HIGH - if code relied on Hive SerDe tables |
| `spark.sql.legacy.allowNonEmptyLocationInCTAS` | Removed | Always errors on non-empty location | MEDIUM |
| `spark.sql.legacy.sizeOfNull` | Removed | `size(null)` returns `null` (was `-1`) | HIGH for null array checks |
| `spark.sql.legacy.replaceDatabricksSparkAvro.enabled` | Removed | Always uses built-in Avro | LOW |
| `spark.sql.legacy.timeParserPolicy` | Removed | Always CORRECTED policy | HIGH for date parsing |
| `spark.sql.legacy.fromDayTimeString.enabled` | Removed | Strict interval parsing | LOW |
| `spark.sql.legacy.notReserveProperties` | Removed | Table properties are always reserved | LOW |
| `spark.sql.hive.convertMetastoreOrc` | Deprecated | Use Delta or native ORC reader | LOW |
| `spark.sql.hive.convertMetastoreParquet` | Deprecated | Use Delta or native Parquet reader | LOW |

**Detect (all legacy configs):**
```regex
spark\.sql\.legacy\.\w+|spark\.sql\.hive\.convert
```

### 3.2 Configs with Changed Defaults

| Config | 13.3 Default | 16.4 Default | Migration Action |
|--------|-------------|-------------|------------------|
| `spark.sql.ansi.enabled` | `false` | `true` | **CRITICAL** - See Section 2 |
| `spark.sql.sources.default` | `delta` | `delta` | No change (Databricks) |
| `spark.sql.storeAssignmentPolicy` | `ANSI` | `ANSI` | Already ANSI on Databricks |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `30MB` | Updated in 3.5 | May change join strategies |
| `spark.sql.parquet.datetimeRebaseModeInRead` | `LEGACY` | `CORRECTED` | Affects pre-1582 dates |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | `LEGACY` | `CORRECTED` | Affects pre-1582 dates |
| `spark.sql.parquet.int96RebaseModeInRead` | `LEGACY` | `CORRECTED` | Affects INT96 timestamps |
| `spark.sql.parquet.int96RebaseModeInWrite` | `LEGACY` | `CORRECTED` | Affects INT96 timestamps |
| `spark.sql.legacy.sizeOfNull` | `-1` | Removed (null returns null) | Check `size()` null guards |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` (in some contexts) | Write performance change |
| `spark.sql.sources.partitionOverwriteMode` | `STATIC` | `STATIC` | No change, but verify |

**Detect (datetime rebase configs):**
```regex
spark\.sql\.parquet\.\w*[Rr]ebase|spark\.sql\.avro\.\w*[Rr]ebase|datetimeRebaseMode|int96RebaseMode
```

**Fix for datetime rebase (if pre-1582 dates exist):**
```scala
// If you have historical dates before 1582-10-15 (unlikely for healthcare but possible in test data)
// You may need to explicitly set rebase mode for backward compatibility:
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

// Better fix: Verify your data doesn't have pre-1582 dates, then use CORRECTED (the new default)
```

### 3.3 Configs That Were Changed Between Intermediate Versions (14.3, 15.4)

Some configs changed in intermediate DBR versions between 13.3 and 16.4:

| Config | Changed In | Old Value | New Value |
|--------|-----------|-----------|-----------|
| `spark.sql.ansi.enabled` | DBR 14.3+ | `false` | `true` |
| `spark.databricks.delta.properties.defaults.deletionVectors.enabled` | DBR 14.3+ | `false` | `true` (for new tables) |
| `spark.sql.adaptive.forceOptimizedHashAgg` | Spark 3.5 | Not present | `true` |
| `spark.sql.maxSinglePartitionBytes` | DBR 16.x | Not applicable | Adjusted for Photon |

### 3.4 How to Scan for Config Issues

**Detect all spark.conf.set patterns:**
```regex
spark\.conf\.set\s*\(|\.set\s*\(\s*"spark\.|SET\s+spark\.|--\s*SET\s+spark\.
```

**Detect SQL SET patterns:**
```regex
%sql\s+SET\s|SET\s+spark\.\w|SET\s+hive\.\w|SET\s+mapreduce\.|SET\s+io\.
```

**Scan script for Molina notebooks:**
```scala
// Run this in a notebook on 16.4 to find problematic configs
val problematicConfigs = Seq(
  "spark.sql.legacy.",
  "spark.sql.hive.convertMetastore",
  "spark.sql.parquet.int96AsTimestamp",
  "spark.sql.legacy.sizeOfNull",
  "spark.sql.legacy.timeParserPolicy",
  "spark.sql.legacy.createHiveTableByDefault"
)

// For each notebook source, scan for these patterns
// This is the automated scan approach
```

---

## 4. Deprecated APIs

### 4.1 SQL Functions Deprecated or Changed

| Function | Status in 16.4 | Replacement | Detect Pattern |
|----------|----------------|-------------|----------------|
| `DATE_SUB(date, days)` | Still works | Prefer `date - INTERVAL 'n' DAYS` | `\bDATE_SUB\s*\(` |
| `DATE_ADD(date, days)` | Still works | Prefer `date + INTERVAL 'n' DAYS` | `\bDATE_ADD\s*\(` |
| `APPROX_PERCENTILE` | Renamed | `PERCENTILE_APPROX` (both work) | `\bAPPROX_PERCENTILE\s*\(` |
| `FIRST(col)` without `IGNORE NULLS` | Stricter | Specify `FIRST(col, true)` to ignore nulls | `\bFIRST\s*\(` |
| `LAST(col)` without `IGNORE NULLS` | Stricter | Specify `LAST(col, true)` to ignore nulls | `\bLAST\s*\(` |
| `STACK()` | More strict | Argument types must match exactly | `\bSTACK\s*\(` |
| `SUBSTR(str, pos)` | ANSI behavior | Negative pos wraps from end in ANSI | `\bSUBSTR\s*\(` |
| `ELT()` | ANSI behavior | Out-of-range index throws under ANSI | `\bELT\s*\(` |

**Detect (all deprecated SQL functions):**
```regex
\b(DATE_SUB|DATE_ADD|APPROX_PERCENTILE|FIRST|LAST|STACK|ELT)\s*\(
```

### 4.2 DataFrame Method Changes

| Method | Status | Replacement | Detect Pattern |
|--------|--------|-------------|----------------|
| `Dataset.toJSON()` | Still works | No change | N/A |
| `Dataset.queryExecution.toRdd` | Internal API, may break | Use `Dataset.rdd` | `\.queryExecution` |
| `Dataset.explain(true)` | Deprecated overload | `Dataset.explain("extended")` | `\.explain\(true\)` |
| `SQLContext` | Deprecated | `SparkSession` | `\bSQLContext\b` |
| `HiveContext` | Deprecated | `SparkSession` | `\bHiveContext\b` |
| `DataFrameWriter.insertInto` | Behavior change | Column matching is by name, not position in some cases | `\.insertInto\(` |
| `DataFrameReader.json(RDD)` | Deprecated | `spark.read.json(Dataset[String])` | `\.json\(\s*\w+[Rr]dd` |
| `unionAll()` | Still works but deprecated | Use `union()` | `\.unionAll\(` |

**Detect (deprecated Scala APIs):**
```regex
\b(SQLContext|HiveContext)\b|\.queryExecution\b|\.explain\(\s*true\s*\)|\.unionAll\(|\.json\(\s*\w*[Rr]dd
```

### 4.3 UDF Registration Changes

**Detect:**
```regex
spark\.udf\.register\(|udf\(|\.register\s*\(|UserDefinedFunction
```

**Before (13.3):**
```scala
// UDF registration - still works in 16.4 but some patterns are deprecated
import org.apache.spark.sql.functions.udf

// Old-style UDF with explicit type
val cleanNPI = udf((npi: String) => {
  if (npi == null || npi.trim.isEmpty) null
  else npi.trim.replaceAll("[^0-9]", "")
})

// Register for SQL use
spark.udf.register("clean_npi", cleanNPI)
```

**After (16.4 - recommended pattern):**
```scala
// Preferred: Type-safe UDF registration
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

// Explicit null handling is MORE important under ANSI mode
val cleanNPI: UserDefinedFunction = udf((npi: String) => {
  Option(npi) match {
    case Some(n) if n.trim.nonEmpty => n.trim.replaceAll("[^0-9]", "")
    case _ => null: String
  }
})

// Register with explicit return type for SQL
spark.udf.register("clean_npi", cleanNPI)
```

**ANSI mode impact on UDFs:**
- UDFs that receive unexpected types may throw `SparkException` instead of silently coercing
- Null inputs are NOT affected by ANSI mode (they remain null)
- Return type mismatches are stricter

### 4.4 Legacy Mode Flags That No Longer Exist

```scala
// These flags are no longer available or have no effect in 16.4:
// DO NOT set these -- they will either error or be silently ignored

// spark.sql.legacy.setCommandRejectsSparkCoreConfs -- removed
// spark.sql.legacy.addSingleFileInAddFile -- removed
// spark.sql.legacy.mssqlserver.numericMapping -- removed
// spark.sql.legacy.exponentLiteralAsDecimalEnabled -- removed
// spark.sql.legacy.allowCastNumericToTimestamp -- removed
// spark.sql.legacy.bucketedTableScan.enabled -- removed
```

**Detect:**
```regex
spark\.sql\.legacy\.(setCommandRejectsSparkCoreConfs|addSingleFileInAddFile|mssqlserver|exponentLiteral|allowCastNumericToTimestamp|bucketedTableScan)
```

---

## 5. Delta Lake Changes (13.3 to 16.4)

### 5.1 Protocol Version Changes

| Feature | DBR 13.3 | DBR 16.4 | Protocol Required |
|---------|----------|----------|-------------------|
| minReaderVersion default | 1 | 1 | N/A |
| minWriterVersion default | 2 | 7 (for new tables with features) | N/A |
| Deletion Vectors | Available (opt-in) | Default for new tables | Reader v3, Writer v7 |
| Column Mapping | Available | Available | Reader v2, Writer v5 |
| Row Tracking | Not available | Available (opt-in) | Writer v7 |
| Liquid Clustering | Not available | Available | Reader v3, Writer v7 |

**CRITICAL WARNING:** Protocol upgrades are ONE-WAY. Once a table's protocol is upgraded, older runtimes cannot read/write it.

**Detect (protocol-related operations):**
```regex
ALTER\s+TABLE.*SET\s+TBLPROPERTIES.*delta\.minReader|ALTER\s+TABLE.*SET\s+TBLPROPERTIES.*delta\.minWriter|delta\.enableDeletionVectors|delta\.enableRowTracking|delta\.columnMapping
```

### 5.2 Deletion Vectors

**What changed:** In DBR 16.4, deletion vectors are enabled by default for **new** tables. Existing tables are NOT automatically upgraded.

**Impact:**
- New tables created on 16.4 will have deletion vectors enabled
- Reads from these tables require Reader v3 protocol
- If any 13.3 clusters need to read these tables, they will FAIL

**Detect (new table creation that may get deletion vectors):**
```regex
CREATE\s+TABLE|\.saveAsTable\(|\.write\.\w+\.save\(|CREATE\s+OR\s+REPLACE\s+TABLE
```

**Fix (if backward compatibility needed):**
```sql
-- Disable deletion vectors for a specific table (if 13.3 readers needed during migration)
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

-- Or at table creation time:
CREATE TABLE catalog.schema.table_name (...)
USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
```

```scala
// Scala: Disable at write time
df.write
  .format("delta")
  .option("delta.enableDeletionVectors", "false")
  .saveAsTable("catalog.schema.table_name")
```

**Recommended approach during migration window:**
1. Do NOT disable deletion vectors globally
2. For tables that must be readable by 13.3 AND 16.4 during migration, disable per-table
3. After all jobs are on 16.4, re-enable deletion vectors

### 5.3 Row Tracking

New in DBR 16.4. Not enabled by default but available.

**Enable:**
```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');
```

**Impact:** Increases write latency slightly. Enables row-level change tracking. Useful for CDC patterns.

### 5.4 Column Mapping Defaults

Column mapping allows rename and drop columns on Delta tables. Available in both 13.3 and 16.4 but the default mode may differ.

**Detect:**
```regex
delta\.columnMapping\.mode|ALTER\s+TABLE.*RENAME\s+COLUMN|ALTER\s+TABLE.*DROP\s+COLUMN
```

**Check current setting:**
```sql
DESCRIBE DETAIL catalog.schema.table_name;
-- Look at minReaderVersion and minWriterVersion
```

### 5.5 Liquid Clustering (Replaces ZORDER)

**What changed:** Liquid Clustering is available in 16.4 as a replacement for `ZORDER BY`. It is opt-in.

**Detect (ZORDER usage):**
```regex
\bZORDER\s+BY\b|OPTIMIZE\s+\w+.*ZORDER
```

**Before (13.3):**
```sql
OPTIMIZE catalog.schema.claims ZORDER BY (member_id, service_date)
```

**After (16.4 - optional migration to Liquid Clustering):**
```sql
-- Step 1: Enable liquid clustering on the table
ALTER TABLE catalog.schema.claims
CLUSTER BY (member_id, service_date);

-- Step 2: Trigger optimization (liquid clustering is automatic on writes, but you can force it)
OPTIMIZE catalog.schema.claims;
-- No ZORDER BY needed -- clustering columns are stored in table metadata
```

**DO NOT migrate to Liquid Clustering during the DBR upgrade. This is a separate optimization pass.**
- Liquid Clustering requires Writer v7 protocol
- It fundamentally changes the physical layout of the table
- Plan this as a post-migration optimization

### 5.6 Table Feature Auto-Upgrade Risks

**CRITICAL:** Some operations on 16.4 may silently upgrade table protocol versions:

| Operation | Protocol Upgrade? | Risk |
|-----------|-------------------|------|
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')` | YES - Writer v7, Reader v3 | Breaks 13.3 access |
| `ALTER TABLE ... CLUSTER BY (...)` | YES - Writer v7, Reader v3 | Breaks 13.3 access |
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')` | YES - Writer v7 | Breaks 13.3 writes |
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` | YES - Reader v2, Writer v5 | May break 13.3 access |
| Regular INSERT/UPDATE/DELETE on new tables | NO (unless table already has features) | Safe |
| Creating new tables | Possible (deletion vectors default) | May break 13.3 readers |

**Safeguard scan:**
```regex
ALTER\s+TABLE.*SET\s+TBLPROPERTIES.*delta\.(enable|columnMapping|min)
```

---

## 6. PySpark-Specific Changes (for Mixed-Language Notebooks)

> Some Molina notebooks contain both `%scala` and `%python` cells. These PySpark changes apply to `%python` cells within Scala notebooks.

### 6.1 Arrow-Based UDF Defaults

**Detect:**
```regex
%python|pandas_udf|spark\.sql\.execution\.arrow\.pyspark\.enabled|spark\.sql\.execution\.arrow
```

**Changed defaults:**

| Config | 13.3 | 16.4 |
|--------|------|------|
| `spark.sql.execution.arrow.pyspark.enabled` | `true` | `true` |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | `true` | `true` |
| `spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled` | `false` | `true` |

**Impact of simplified traceback:**
- PySpark UDF errors show cleaner tracebacks
- Some error handling that parsed traceback strings may break
- Generally positive change -- no code fix needed

### 6.2 pandas_udf Improvements

**Before (13.3):**
```python
# %python cell in a Scala notebook
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def normalize_amount(s: pd.Series) -> pd.Series:
    return s / s.max()
```

**After (16.4 - same API but better performance):**
```python
# %python cell - same code works, but:
# 1. Arrow serialization is faster
# 2. Type checking is stricter under ANSI mode
# 3. Null handling may differ if input types don't match schema

@pandas_udf("double")
def normalize_amount(s: pd.Series) -> pd.Series:
    # Explicitly handle nulls for ANSI safety
    if s.empty:
        return s
    max_val = s.max()
    if max_val == 0 or pd.isna(max_val):
        return pd.Series([None] * len(s))
    return s / max_val
```

### 6.3 PySpark Type Coercion Under ANSI Mode

The same ANSI mode rules from Section 2 apply to PySpark. In `%python` cells:

```python
# %python
# These all follow the same ANSI rules as SQL/Scala:

# Before (13.3):
df.withColumn("amount_int", df["amount"].cast("int"))  # null on invalid

# After (16.4):
from pyspark.sql.functions import expr
df.withColumn("amount_int", expr("TRY_CAST(amount AS INT)"))  # null on invalid
```

---

## 7. Scala-Specific Changes

### 7.1 Scala 2.12.15 to 2.12.18 Compatibility

This is a minor version bump within Scala 2.12. Full backward compatibility is maintained. Key changes:

**Bug fixes in 2.12.18:**
- Fixed `StringContext` interpolation edge cases
- Fixed pattern matching exhaustiveness check false negatives
- Fixed implicit resolution in certain shadowing scenarios
- Fixed `LazyList` and `Stream` memory retention issues

**No code changes are required for the Scala version bump alone.**

### 7.2 Dataset API Deprecations

**Detect:**
```regex
\.as\[|Encoder|\.typed\b|\.groupByKey\(|\.mapGroups\(|\.flatMapGroups\(|\.cogroup\(
```

**Deprecated/changed patterns:**

```scala
// The typed Dataset API is stable but some patterns have better alternatives:

// Before: Implicit encoder derivation that may produce warnings
case class Claim(id: Long, amount: Double, code: String)
val ds = df.as[Claim]  // Still works, but encoder derivation warnings may appear

// After: Explicit encoder is preferred for complex types
import org.apache.spark.sql.Encoders
implicit val claimEncoder = Encoders.product[Claim]
val ds = df.as[Claim]

// Before: groupByKey with mapGroups (expensive full shuffle)
ds.groupByKey(_.code).mapGroups { (key, iter) =>
  // process
}

// After: Still works, but consider using DataFrame groupBy + agg for performance
// The typed API serializes/deserializes objects which is slower than Tungsten codegen
```

### 7.3 Type Inference Changes

**Detect:**
```regex
\.select\(|\.withColumn\(|\.agg\(
```

Spark 3.5 has improved type inference that may change column types in some edge cases:

```scala
// Before (13.3): Some expressions resolved as IntegerType
val result = df.selectExpr("1 + 1")  // IntegerType in both versions

// After (16.4): Decimal precision may differ for complex expressions
val result = df.selectExpr("SUM(CAST(amount AS DECIMAL(10,2)))")
// Precision and scale of result may be wider in 3.5

// Fix: Explicitly cast result to desired precision
val result = df.selectExpr("CAST(SUM(CAST(amount AS DECIMAL(10,2))) AS DECIMAL(18,2))")
```

### 7.4 Compiler Warning Changes

New warnings you may see in 16.4 that were not present in 13.3:

| Warning | Meaning | Action |
|---------|---------|--------|
| `match may not be exhaustive` | Pattern match missing cases | Add wildcard case or handle all cases |
| `deprecated symbol` | Using deprecated Spark API | Update to recommended API |
| `adaptation of argument list` | Auto-tupling of function args | Make tuple explicit |
| `type inference fell back` | Complex type inference scenario | Add explicit type annotation |

**These are warnings, not errors. They do not block execution in notebooks.**

---

## 8. ML Runtime Migration (Standard and ML)

### 8.1 Libraries: Standard vs ML Runtime

| Library | Standard 13.3 | Standard 16.4 | ML 13.3 | ML 16.4 |
|---------|--------------|--------------|---------|---------|
| MLlib | Spark 3.4.1 | Spark 3.5.x | Spark 3.4.1 | Spark 3.5.x |
| scikit-learn | Not included | Not included | 1.1.x | 1.3.x+ |
| TensorFlow | Not included | Not included | 2.12.x | 2.15.x+ |
| PyTorch | Not included | Not included | 1.13.x | 2.1.x+ |
| Hugging Face Transformers | Not included | Not included | 4.26.x | 4.36.x+ |
| XGBoost | Not included | Not included | 1.7.x | 2.0.x+ |
| MLflow | Pre-installed | Pre-installed | Pre-installed | Pre-installed (newer) |
| LightGBM | Not included | Not included | 3.3.x | 4.1.x+ |
| Hyperopt | Not included | Not included | 0.2.7 | 0.2.7 |
| pandas | 1.5.x | 2.0.x+ | 1.5.x | 2.0.x+ |
| numpy | 1.23.x | 1.24.x+ | 1.23.x | 1.24.x+ |

### 8.2 MLlib API Changes (Spark 3.4 to 3.5)

**Detect:**
```regex
import\s+org\.apache\.spark\.ml\b|import\s+org\.apache\.spark\.mllib\b|\.fit\(|\.transform\(|Pipeline|CrossValidator|TrainValidationSplit
```

**Key changes:**

```scala
// 1. StringIndexer: handleInvalid default clarification
// No API change, but behavior under ANSI mode is stricter
import org.apache.spark.ml.feature.StringIndexer

val indexer = new StringIndexer()
  .setInputCol("diagnosis_code")
  .setOutputCol("diagnosis_index")
  .setHandleInvalid("keep")  // Explicitly set -- don't rely on default

// 2. VectorAssembler: null handling
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("age", "bmi", "bp"))
  .setOutputCol("features")
  .setHandleInvalid("keep")  // IMPORTANT: set explicitly for ANSI safety

// 3. OneHotEncoder: dropLast default
import org.apache.spark.ml.feature.OneHotEncoder

val encoder = new OneHotEncoder()
  .setInputCols(Array("diagnosis_index"))
  .setOutputCols(Array("diagnosis_vec"))
  .setDropLast(true)  // Explicitly set

// 4. CrossValidator: collectSubModels behavior
// In 3.5, memory management for collected sub-models is improved
// No code change needed

// 5. Pipeline save/load: serialization format is backward compatible
// Pipelines saved on 13.3 can be loaded on 16.4
// Pipelines saved on 16.4 may NOT load on 13.3 (if new features used)
```

**Deprecated MLlib APIs:**

| API | Status | Replacement |
|-----|--------|-------------|
| `spark.mllib` (RDD-based) | Deprecated since 2.0, still available | `spark.ml` (DataFrame-based) |
| `ChiSqSelector` | Renamed | `ChiSqSelector` still works, but use `UnivariateFeatureSelector` |
| `MultilayerPerceptronClassifier.layers` | Changed | `.setLayers()` parameter validation is stricter |

**Detect (deprecated MLlib):**
```regex
import\s+org\.apache\.spark\.mllib\b|mllib\.regression|mllib\.classification|mllib\.clustering|mllib\.recommendation
```

### 8.3 TensorFlow Version Changes (ML Runtime)

**13.3 ML:** TensorFlow 2.12.x
**16.4 ML:** TensorFlow 2.15.x+

**Breaking changes:**
```python
# %python cell in ML runtime notebook

# 1. tf.keras is now keras 3.x (major change)
# Before (13.3):
import tensorflow as tf
model = tf.keras.Sequential([...])

# After (16.4): Same API but keras 3 has some breaking changes:
# - Custom training loops may need updates
# - Some legacy keras.utils functions removed
# - Metric API changes

# 2. TF SavedModel format changes
# Models saved with TF 2.12 load fine on TF 2.15
# Models saved with TF 2.15 may NOT load on TF 2.12

# 3. Deprecated TF APIs removed:
# - tf.compat.v1.* has further removals
# - tf.contrib is fully gone (was removed earlier)
```

**Detect:**
```regex
import\s+tensorflow|from\s+tensorflow|tf\.keras|tf\.compat\.v1|tf\.contrib
```

### 8.4 PyTorch Version Changes (ML Runtime)

**13.3 ML:** PyTorch 1.13.x
**16.4 ML:** PyTorch 2.1.x+

**Breaking changes:**
```python
# %python
# 1. torch.compile() is now stable and default in many cases
# 2. Distributed training API changes
# 3. Some deprecated functions removed

# Detect pattern:
# import torch
# torch.nn.* usage
# torch.distributed.*
```

**Detect:**
```regex
import\s+torch|from\s+torch|torch\.nn|torch\.distributed|torch\.optim
```

### 8.5 Hugging Face Transformers Changes (ML Runtime)

**13.3 ML:** Transformers 4.26.x
**16.4 ML:** Transformers 4.36.x+

**Key changes:**
- Pipeline API has new default behaviors
- Tokenizer fast/slow distinction changes
- Model loading has additional safety checks
- Some model classes renamed

**Detect:**
```regex
from\s+transformers|import\s+transformers|AutoModel|AutoTokenizer|pipeline\s*\(
```

### 8.6 scikit-learn Changes (ML Runtime)

**13.3 ML:** scikit-learn 1.1.x
**16.4 ML:** scikit-learn 1.3.x+

**Breaking changes:**
```python
# %python
# 1. set_output API is now stable
# 2. Some estimator parameter defaults changed
# 3. Deprecated parameters removed

# Key parameter changes:
# - LinearRegression: normalize parameter removed (use StandardScaler)
# - KMeans: n_init default changed from 10 to 'auto'
# - LogisticRegression: Some solver defaults changed
```

**Detect:**
```regex
from\s+sklearn|import\s+sklearn|\.fit\(|\.predict\(|\.transform\(
```

### 8.7 XGBoost Changes (ML Runtime)

**13.3 ML:** XGBoost 1.7.x
**16.4 ML:** XGBoost 2.0.x+

**Breaking changes:**
```python
# %python
# XGBoost 2.0 has significant changes:
# 1. Default tree method changed from 'exact' to 'hist'
# 2. GPU support unified (use device="cuda" instead of tree_method="gpu_hist")
# 3. Some deprecated parameters removed

# Before (13.3):
import xgboost as xgb
model = xgb.XGBClassifier(
    tree_method="gpu_hist",  # Deprecated in 2.0
    gpu_id=0                 # Deprecated in 2.0
)

# After (16.4):
model = xgb.XGBClassifier(
    device="cuda",           # New unified device parameter
    tree_method="hist"       # Default in 2.0
)
```

**Detect:**
```regex
import\s+xgboost|from\s+xgboost|XGBClassifier|XGBRegressor|tree_method.*gpu_hist|gpu_id
```

### 8.8 AutoML Changes

**13.3:** Databricks AutoML based on older API
**16.4:** Updated AutoML with additional model types

```python
# %python
# AutoML API is largely stable but:
# 1. Additional model types available
# 2. Experiment tracking improvements
# 3. Feature store integration changes

from databricks import automl

# Before (13.3):
summary = automl.classify(
    dataset=train_df,
    target_col="readmission",
    timeout_minutes=30
)

# After (16.4): Same API, but additional parameters available
summary = automl.classify(
    dataset=train_df,
    target_col="readmission",
    timeout_minutes=30,
    # New optional parameters may be available
)
```

**Detect:**
```regex
from\s+databricks\s+import\s+automl|automl\.classify|automl\.regress|automl\.forecast
```

### 8.9 Feature Store Changes

**Detect:**
```regex
from\s+databricks\.feature_store|FeatureStoreClient|feature_store|FeatureLookup|create_training_set
```

**Before (13.3):**
```python
# %python
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
training_set = fs.create_training_set(
    df=raw_df,
    feature_lookups=[...],
    label="readmission"
)
```

**After (16.4):**
```python
# %python
# Feature Engineering client is the new recommended API
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
training_set = fe.create_training_set(
    df=raw_df,
    feature_lookups=[...],
    label="readmission"
)

# The old FeatureStoreClient still works but is deprecated
```

---

## 9. Package and Dependency Changes

### 9.1 Reviewing %pip install and Library Installs

**Detect:**
```regex
%pip\s+install|dbutils\.library\.install|spark\.install|%conda|pip\s+install
```

**Scan all notebooks for package installs:**
```scala
// This regex finds all package install commands
val pipInstallPattern = """%pip\s+install\s+(.+)""".r
val condaInstallPattern = """%conda\s+install\s+(.+)""".r
```

**Common packages that may need version pinning or replacement:**

| Package | 13.3 Status | 16.4 Status | Action |
|---------|-------------|-------------|--------|
| `pyspark` | 3.4.1 | 3.5.x | Do NOT install separately -- use runtime version |
| `delta-spark` | 2.4.x | 3.1.x+ | Do NOT install separately -- use runtime version |
| `pyarrow` | Pre-installed | Pre-installed (newer) | Remove explicit install if version-pinned |
| `pandas` | 1.5.x | 2.0.x+ | Check for deprecated pandas APIs |
| `numpy` | 1.23.x | 1.24.x+ | Minor changes, mostly compatible |
| `koalas` | Deprecated | Removed | Use `pyspark.pandas` |
| `databricks-connect` | 13.3.x | 16.4.x | Must match runtime version exactly |
| `mlflow` | Pre-installed | Pre-installed | Remove explicit install -- use runtime version |

### 9.2 Packages That Can Be Replaced with Native Spark

| Package | Native Spark Replacement | Detect Pattern |
|---------|-------------------------|----------------|
| `koalas` | `pyspark.pandas` (built-in since Spark 3.2) | `import\s+databricks\.koalas\|import\s+koalas` |
| `spark-xml` (old versions) | Updated to latest | `com\.databricks:spark-xml` |
| `spark-avro` (old external) | Built into Spark | `com\.databricks:spark-avro` |
| `spark-csv` (old external) | Built into Spark since 2.0 | `com\.databricks:spark-csv` |
| Custom JSON parsers | `from_json` / `schema_of_json` | N/A |
| Custom S3 connectors | Built-in Unity Catalog volumes | N/A |

### 9.3 Maven Coordinate Changes for Scala Libraries

**Detect:**
```regex
%scala\s+.*:.*:.*|libraryDependencies|\.config\(\s*"spark\.jars\.packages"|spark\.jars\.packages
```

**Common Scala library updates:**

| Library | 13.3 Coordinate | 16.4 Coordinate | Notes |
|---------|----------------|----------------|-------|
| Delta Lake | `io.delta:delta-core_2.12:2.4.0` | Built-in (do not add) | Remove from dependencies |
| Spark XML | `com.databricks:spark-xml_2.12:0.16.0` | `com.databricks:spark-xml_2.12:0.18.0` | Version bump |
| Spark Avro | Built-in | Built-in | N/A |
| ScalaTest | `org.scalatest:scalatest_2.12:3.2.x` | `org.scalatest:scalatest_2.12:3.2.x` | Compatible |
| Typesafe Config | `com.typesafe:config:1.4.x` | `com.typesafe:config:1.4.x` | Compatible |
| Jackson | `com.fasterxml.jackson.*:2.14.x` | `com.fasterxml.jackson.*:2.15.x+` | May need update |

**Jackson version conflict warning:**
```scala
// Spark 3.5 bundles Jackson 2.15.x
// If your code pins Jackson to 2.14.x, you may get version conflicts
// Fix: Remove explicit Jackson dependency -- use Spark's bundled version

// Detect:
// spark.jars.packages containing "jackson"
// %pip install containing "jackson"
```

**Detect (Jackson conflicts):**
```regex
jackson-databind|jackson-core|jackson-module|com\.fasterxml\.jackson
```

### 9.4 Library Compatibility Matrix

When evaluating library compatibility:

1. **Check if the library is pre-installed on the runtime:**
   ```scala
   // Run on 16.4 cluster to see pre-installed libraries
   // %sh pip list
   // %sh ls /databricks/jars/
   ```

2. **Check if the library needs Scala 2.12 build:**
   All Scala libraries must use `_2.12` suffix since both 13.3 and 16.4 use Scala 2.12.

3. **Check if the library depends on a specific Spark version:**
   Libraries compiled against Spark 3.4 APIs should work on 3.5 (binary compatible within 3.x).

---

## 10. Spark Config Scan Checklist

### 10.1 All Regex Patterns Organized by Severity

#### CRITICAL Severity (Will cause job failures)

```yaml
CRITICAL_001:
  name: "ANSI Mode - CAST"
  detect_sql: "\\bCAST\\s*\\("
  detect_scala: "\\.cast\\("
  description: "CAST throws on invalid data under ANSI mode"
  fix: "Replace with TRY_CAST where null-on-failure is desired"
  false_positive_note: "Not every CAST needs changing -- only those on potentially invalid data"

CRITICAL_002:
  name: "ANSI Mode - Division"
  detect_sql: "\\b\\w+\\s*/\\s*\\w+"
  detect_scala: "col\\([^)]+\\)\\s*/\\s*col\\("
  description: "Division by zero throws under ANSI mode"
  fix: "Use TRY_DIVIDE or NULLIF guard on denominator"

CRITICAL_003:
  name: "ANSI Mode - to_timestamp"
  detect_sql: "\\bto_timestamp\\s*\\("
  detect_scala: "to_timestamp\\("
  description: "to_timestamp throws on invalid input under ANSI mode"
  fix: "Replace with try_to_timestamp"

CRITICAL_004:
  name: "ANSI Mode - to_date"
  detect_sql: "\\bto_date\\s*\\("
  detect_scala: "to_date\\("
  description: "to_date throws on invalid input under ANSI mode"
  fix: "Replace with try_to_date"

CRITICAL_005:
  name: "ANSI Mode - element_at"
  detect: "\\belement_at\\s*\\("
  description: "element_at throws on out-of-bounds/missing key under ANSI mode"
  fix: "Replace with TRY_ELEMENT_AT"

CRITICAL_006:
  name: "Removed Legacy Config"
  detect: "spark\\.sql\\.legacy\\."
  description: "Legacy configs may be removed in 16.4"
  fix: "Check Section 3 for replacement configs"

CRITICAL_007:
  name: "ANSI Mode explicitly disabled"
  detect: "spark\\.sql\\.ansi\\.enabled.*false|ansi\\.enabled.*false"
  description: "NEVER disable ANSI mode as a permanent fix"
  fix: "Fix the underlying code to be ANSI-safe"
```

#### HIGH Severity (May cause incorrect results or intermittent failures)

```yaml
HIGH_001:
  name: "Array Bracket Access"
  detect_sql: "\\w+\\s*\\[\\s*\\d+\\s*\\]"
  detect_scala: "\\.getItem\\(\\s*\\d+\\s*\\)"
  description: "Array index access throws on out-of-bounds under ANSI mode"
  fix: "Use TRY_ELEMENT_AT (1-indexed) or bounds check"

HIGH_002:
  name: "Map Bracket Access"
  detect_sql: "\\w+\\s*\\[\\s*['\"]"
  detect_scala: "\\.getItem\\(\\s*['\"]"
  description: "Map bracket access throws on missing key under ANSI mode"
  fix: "Use TRY_ELEMENT_AT or map_contains_key guard"

HIGH_003:
  name: "Integer Overflow Risk"
  detect: "\\bSUM\\s*\\(.*\\bAS\\s+INT\\b|IntegerType.*\\+|ShortType.*\\+"
  description: "Integer arithmetic can overflow under ANSI mode"
  fix: "Widen to BIGINT/LongType before arithmetic"

HIGH_004:
  name: "DateTime Rebase Mode"
  detect: "datetimeRebaseModeIn|int96RebaseModeIn"
  description: "Rebase mode defaults changed -- may affect historical dates"
  fix: "Verify data doesn't have pre-1582 dates; set explicit mode if needed"

HIGH_005:
  name: "Size of Null Change"
  detect: "\\bsize\\s*\\(|\\bSIZE\\s*\\("
  description: "size(null) returns null instead of -1 in 16.4"
  fix: "Change size(col) = -1 checks to col IS NULL"

HIGH_006:
  name: "to_number without try"
  detect: "\\bto_number\\s*\\("
  description: "to_number throws on invalid input under ANSI mode"
  fix: "Replace with try_to_number"

HIGH_007:
  name: "Delta Protocol Upgrade"
  detect: "ALTER\\s+TABLE.*SET\\s+TBLPROPERTIES.*delta\\.enable"
  description: "Enabling Delta features upgrades protocol irreversibly"
  fix: "Plan protocol upgrades carefully during migration window"

HIGH_008:
  name: "Deletion Vector Compatibility"
  detect: "CREATE\\s+TABLE|CREATE\\s+OR\\s+REPLACE\\s+TABLE|\\.saveAsTable\\("
  description: "New tables in 16.4 may have deletion vectors enabled by default"
  fix: "Set delta.enableDeletionVectors=false if 13.3 readers needed"
```

#### MEDIUM Severity (May cause warnings or subtle behavior changes)

```yaml
MEDIUM_001:
  name: "Boolean Integer Comparison"
  detect: "(?:WHERE|WHEN|AND|OR)\\s+\\w+\\s*=\\s*[01]\\b"
  description: "Boolean-to-integer comparison may fail under ANSI mode"
  fix: "Use IS TRUE / IS FALSE or = TRUE / = FALSE"

MEDIUM_002:
  name: "String Numeric Comparison"
  detect: "(?:WHERE|WHEN|AND|OR)\\s+\\w+\\s*[><=!]+\\s*'[0-9]+'"
  description: "String-to-numeric implicit conversion may fail"
  fix: "Remove quotes from numeric literals or use TRY_CAST"

MEDIUM_003:
  name: "Deprecated SQL Functions"
  detect: "\\b(APPROX_PERCENTILE|ELT)\\s*\\("
  description: "Some SQL functions have changed behavior"
  fix: "Review Section 4.1 for replacements"

MEDIUM_004:
  name: "Deprecated DataFrame APIs"
  detect: "\\b(SQLContext|HiveContext)\\b|\\.unionAll\\("
  description: "Deprecated APIs that should be updated"
  fix: "Use SparkSession and .union() respectively"

MEDIUM_005:
  name: "ZORDER Usage"
  detect: "\\bZORDER\\s+BY\\b"
  description: "ZORDER still works but Liquid Clustering is available"
  fix: "No immediate action; plan Liquid Clustering migration separately"

MEDIUM_006:
  name: "Feature Store Client (Old)"
  detect: "from\\s+databricks\\.feature_store|FeatureStoreClient"
  description: "Old Feature Store client is deprecated"
  fix: "Migrate to FeatureEngineeringClient"

MEDIUM_007:
  name: "Explicit Partition Count"
  detect: "\\.repartition\\(\\d+\\)|\\.coalesce\\(\\d+\\)"
  description: "AQE may produce different partition counts"
  fix: "Verify downstream systems don't depend on exact file counts"

MEDIUM_008:
  name: "Koalas Import"
  detect: "import\\s+databricks\\.koalas|import\\s+koalas"
  description: "Koalas is deprecated; use pyspark.pandas"
  fix: "Replace with import pyspark.pandas as ps"
```

#### LOW Severity (Informational / optimization opportunities)

```yaml
LOW_001:
  name: "Explain with Boolean"
  detect: "\\.explain\\(\\s*true\\s*\\)"
  description: "Deprecated explain(true) overload"
  fix: "Use .explain(\"extended\")"

LOW_002:
  name: "Jackson Dependency"
  detect: "jackson-databind|jackson-core|jackson-module"
  description: "Jackson version may conflict with Spark 3.5 bundled version"
  fix: "Remove explicit Jackson dependency"

LOW_003:
  name: "Old Delta Package"
  detect: "io\\.delta:delta-core"
  description: "Delta Lake is built into Databricks runtime"
  fix: "Remove from Maven coordinates / pip install"

LOW_004:
  name: "Spark Avro External"
  detect: "com\\.databricks:spark-avro"
  description: "Avro is built into Spark"
  fix: "Remove external dependency"

LOW_005:
  name: "Query Execution Internal API"
  detect: "\\.queryExecution\\b"
  description: "Internal API that may break between versions"
  fix: "Use public Dataset API methods"

LOW_006:
  name: "XGBoost GPU Method"
  detect: "tree_method.*gpu_hist|gpu_id"
  description: "Deprecated XGBoost GPU parameters"
  fix: "Use device='cuda' parameter instead"

LOW_007:
  name: "pip install pyspark"
  detect: "%pip\\s+install\\s+pyspark|pip\\s+install\\s+pyspark"
  description: "PySpark is pre-installed; explicit install may cause version conflict"
  fix: "Remove %pip install pyspark"
```

### 10.2 Combined Scan Script

```scala
// Run this script to scan a notebook source string for all patterns
// Input: notebook source code as a string
// Output: list of findings with severity and recommended fixes

import scala.util.matching.Regex

case class ScanFinding(
  severity: String,
  code: String,
  description: String,
  fix: String,
  lineNumber: Int,
  matchedText: String
)

def scanNotebook(source: String): Seq[ScanFinding] = {
  val lines = source.split("\n")
  val findings = scala.collection.mutable.ArrayBuffer[ScanFinding]()

  val patterns = Seq(
    // CRITICAL
    ("CRITICAL", "CRIT_001", """\bCAST\s*\(""".r, "CAST throws on invalid data under ANSI mode", "Replace with TRY_CAST"),
    ("CRITICAL", "CRIT_002", """\.cast\(""".r, "DataFrame cast() throws on invalid data", "Use expr(\"TRY_CAST(...)\")"),
    ("CRITICAL", "CRIT_003", """\bto_timestamp\s*\(""".r, "to_timestamp throws on invalid input", "Replace with try_to_timestamp"),
    ("CRITICAL", "CRIT_004", """\bto_date\s*\(""".r, "to_date throws on invalid input", "Replace with try_to_date"),
    ("CRITICAL", "CRIT_005", """\belement_at\s*\(""".r, "element_at throws on out-of-bounds/missing key", "Replace with TRY_ELEMENT_AT"),
    ("CRITICAL", "CRIT_006", """spark\.sql\.legacy\.""".r, "Legacy config may be removed", "Check migration guide Section 3"),
    ("CRITICAL", "CRIT_007", """spark\.sql\.ansi\.enabled.*false""".r, "ANSI mode disabled", "Fix code to be ANSI-safe instead"),
    // HIGH
    ("HIGH", "HIGH_001", """\w+\s*\[\s*\d+\s*\]""".r, "Array index access may throw", "Use TRY_ELEMENT_AT"),
    ("HIGH", "HIGH_003", """\bto_number\s*\(""".r, "to_number throws on invalid input", "Replace with try_to_number"),
    ("HIGH", "HIGH_004", """\bsize\s*\(""".r, "size(null) returns null instead of -1", "Check null guards"),
    // MEDIUM
    ("MEDIUM", "MED_001", """(?:WHERE|WHEN|AND|OR)\s+\w+\s*=\s*[01]\b""".r, "Boolean-to-integer comparison", "Use IS TRUE / IS FALSE"),
    ("MEDIUM", "MED_002", """\b(SQLContext|HiveContext)\b""".r, "Deprecated context", "Use SparkSession"),
    ("MEDIUM", "MED_003", """\.unionAll\(""".r, "Deprecated unionAll", "Use .union()"),
    // LOW
    ("LOW", "LOW_001", """\.explain\(\s*true\s*\)""".r, "Deprecated explain(true)", "Use .explain(\"extended\")")
  )

  for ((line, idx) <- lines.zipWithIndex) {
    for ((severity, code, pattern, desc, fix) <- patterns) {
      pattern.findFirstIn(line).foreach { matched =>
        findings += ScanFinding(severity, code, desc, fix, idx + 1, matched)
      }
    }
  }

  findings.toSeq.sortBy(f => (f.severity match {
    case "CRITICAL" => 0
    case "HIGH" => 1
    case "MEDIUM" => 2
    case "LOW" => 3
    case _ => 4
  }, f.lineNumber))
}
```

---

## 11. New Features Available in 16.4 (Awareness)

These features are available for adoption but are NOT required for migration. Document them for the team's awareness.

### 11.1 Predictive I/O

Automatically predicts and prefetches data needed for queries, improving scan performance.

```sql
-- Enabled by default on 16.4 for supported workloads
-- No code changes needed
-- Benefits:
--   - Faster full table scans
--   - Improved predicate pushdown
--   - Better performance for large Delta tables
```

### 11.2 Liquid Clustering

Replaces ZORDER with automatic, incremental clustering.

```sql
-- Create a table with liquid clustering (new tables only)
CREATE TABLE catalog.schema.claims (
  claim_id BIGINT,
  member_id BIGINT,
  service_date DATE,
  amount DECIMAL(18,2)
)
CLUSTER BY (member_id, service_date);

-- Migrate existing table (requires protocol upgrade)
ALTER TABLE catalog.schema.claims
CLUSTER BY (member_id, service_date);

-- Then optimize (one-time to reorganize existing data)
OPTIMIZE catalog.schema.claims;
```

### 11.3 IDENTIFIER() Clause

Dynamically reference table and column names in SQL.

```sql
-- Before: String interpolation (SQL injection risk)
-- spark.sql(s"SELECT * FROM $tableName")

-- After: Safe dynamic SQL with IDENTIFIER()
SELECT * FROM IDENTIFIER('catalog.schema.claims')

-- Dynamic column reference
SELECT IDENTIFIER('member_id') FROM catalog.schema.claims
```

```scala
// Scala usage:
val tableName = "catalog.schema.claims"
spark.sql(s"SELECT * FROM IDENTIFIER('$tableName')")
```

### 11.4 Variant Type (Verify GA Status Before Use)

Native semi-structured data type for JSON-like data. Available in DBR 15.3+. **Verify GA status before adopting** — confirm with Databricks documentation that VARIANT is GA for your DBR version before using in production.

```sql
-- Parse JSON into Variant type (no schema needed)
SELECT PARSE_JSON('{"npi": "1234567890", "name": "Dr. Smith"}') AS provider_data

-- Access fields from Variant
SELECT provider_data:npi::string AS npi
FROM providers_variant

-- Variant preserves exact JSON structure without schema inference
```

### 11.5 Default Column Values

```sql
-- Tables can now have default values
CREATE TABLE catalog.schema.claims (
  claim_id BIGINT,
  status STRING DEFAULT 'PENDING',
  created_at TIMESTAMP DEFAULT current_timestamp(),
  version INT DEFAULT 1
);

-- INSERT without specifying status will use 'PENDING'
INSERT INTO catalog.schema.claims (claim_id) VALUES (12345);
```

### 11.6 Python UDF Improvements

```python
# %python
# Arrow-optimized Python UDFs (available in 16.4)
# These are significantly faster than traditional Python UDFs

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Traditional UDF (still works)
@udf(returnType=StringType())
def clean_npi(npi):
    if npi is None:
        return None
    return npi.strip().replace("-", "")

# Arrow-optimized batch UDF (new recommended pattern)
# Uses pandas_udf for batch processing
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def clean_npi_batch(s: pd.Series) -> pd.Series:
    return s.str.strip().str.replace("-", "", regex=False)
```

### 11.7 Structured Streaming Improvements

```scala
// New in Spark 3.5:

// 1. Async progress tracking (reduces commit latency)
spark.conf.set("spark.sql.streaming.asyncProgressTrackingEnabled", "true")

// 2. State store improvements
// - RocksDB state store is more stable
// - Better memory management for stateful operations

// 3. Watermark propagation in multiple streaming aggregations
// - Watermark now propagates through unions and joins correctly

// 4. Trigger.AvailableNow improvements
// - Better handling of available data detection
// - Reduced latency for "catch up" processing

// Example: Modern streaming pattern
val query = spark.readStream
  .format("delta")
  .table("catalog.schema.claims_raw")
  .withWatermark("event_time", "1 hour")
  .groupBy(window(col("event_time"), "5 minutes"), col("provider_id"))
  .agg(sum("amount").as("total_amount"))
  .writeStream
  .format("delta")
  .outputMode("append")
  .trigger(Trigger.AvailableNow())
  .option("checkpointLocation", "/checkpoints/claims_agg")
  .toTable("catalog.schema.claims_agg")
```

---

## 12. Common Issues After Upgrade (Troubleshooting)

### 12.1 ArithmeticException on Divide

**Error:**
```
org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero.
Use `try_divide` to tolerate divisor being 0 and return NULL instead.
```

**Cause:** ANSI mode is ON. A denominator evaluates to zero.

**Fix:**
```sql
-- SQL
SELECT TRY_DIVIDE(numerator, denominator) FROM table

-- Or with NULLIF
SELECT numerator / NULLIF(denominator, 0) FROM table
```

```scala
// Scala
df.withColumn("ratio", expr("TRY_DIVIDE(numerator, denominator)"))
```

**DO NOT FIX BY:**
```scala
// NEVER do this:
spark.conf.set("spark.sql.ansi.enabled", "false")
```

### 12.2 NumberFormatException on Cast

**Error:**
```
org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'ABC' of the type "STRING" cannot be cast to "INT" because it is malformed.
Use `try_cast` to tolerate malformed input and return NULL instead.
```

**Cause:** ANSI mode is ON. A string value cannot be parsed as the target numeric type.

**Fix:**
```sql
-- SQL
SELECT TRY_CAST(value AS INT) FROM table
```

```scala
// Scala
df.withColumn("int_val", expr("TRY_CAST(value AS INT)"))
```

**Investigation step:** Check the data for invalid values. This error reveals data quality issues that were previously hidden:
```sql
-- Find the bad data
SELECT DISTINCT value
FROM table
WHERE TRY_CAST(value AS INT) IS NULL AND value IS NOT NULL
```

### 12.3 Different Partition Counts from AQE

**Symptom:** Output files have different counts than before. Data is correct but physical layout differs.

**Investigation:**
```scala
// Check actual partition count
val df = spark.read.format("delta").load(path)
println(s"Partitions: ${df.rdd.getNumPartitions}")

// Check AQE settings
println(spark.conf.get("spark.sql.adaptive.enabled"))
println(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))
println(spark.conf.get("spark.sql.adaptive.coalescePartitions.minPartitionSize"))
```

**Fix (if exact file count matters):**
```scala
// Repartition before write
df.repartition(targetPartitions).write.format("delta").save(path)

// Or use Delta optimize after write
spark.sql("OPTIMIZE catalog.schema.table")
```

### 12.4 UDF Null Handling Changes

**Symptom:** UDFs that previously received null inputs now throw NullPointerException.

**Cause:** Under ANSI mode, type coercion is stricter, which can change what types are passed to UDFs.

**Fix:**
```scala
// Always handle nulls explicitly in UDFs
val safeUdf = udf((input: String) => {
  // ALWAYS check for null first
  if (input == null) null
  else {
    // your logic here
    input.trim.toUpperCase
  }
})

// Better: Use Option for null safety
val safeUdf = udf((input: String) => {
  Option(input).map(_.trim.toUpperCase).orNull
})
```

### 12.5 Config Not Found Errors

**Error:**
```
org.apache.spark.sql.AnalysisException: The SQL config 'spark.sql.legacy.sizeOfNull' was removed in the version X.X.X
```

**Cause:** Code explicitly sets a config that has been removed.

**Fix:** Remove the config setting and update code to work with the new default behavior.

```scala
// Before:
spark.conf.set("spark.sql.legacy.sizeOfNull", "true")
// ... code that checks size(array_col) == -1 for null detection

// After: Remove the config and fix the null check
// spark.conf.set("spark.sql.legacy.sizeOfNull", "true")  // REMOVE THIS LINE
// ... change null detection:
df.filter(col("array_col").isNull)  // Instead of size(array_col) == -1
```

### 12.6 Delta Protocol Errors

**Error:**
```
io.delta.exceptions.InvalidProtocolVersionException: Delta table requires reader version 3 but the current reader version is 1.
```

**Cause:** A table was upgraded to a newer protocol (e.g., deletion vectors enabled), but the cluster/runtime doesn't support that protocol version.

**Investigation:**
```sql
-- Check table protocol
DESCRIBE DETAIL catalog.schema.table_name

-- Check table features
SHOW TBLPROPERTIES catalog.schema.table_name
```

**Fix:**
- If the error is on a 13.3 cluster: the table was upgraded by a 16.4 job. You cannot downgrade the table.
  - Solution: Run all jobs accessing this table on 16.4
- If the error is on a 16.4 cluster: this should not happen. Check if a custom Delta version is being loaded.

**Prevention during migration:**
```sql
-- Before migration, record all table protocol versions
SELECT
  table_catalog,
  table_schema,
  table_name,
  min_reader_version,
  min_writer_version
FROM system.information_schema.tables
WHERE data_source_format = 'DELTA'
```

### 12.7 SparkDateTimeException on Date Parsing

**Error:**
```
org.apache.spark.SparkDateTimeException: [CAST_INVALID_INPUT] The value '02/30/2024' of the type "STRING" cannot be cast to "DATE" because it is malformed.
```

**Cause:** ANSI mode rejects invalid dates that previously returned null.

**Fix:**
```sql
-- SQL
SELECT try_to_date('02/30/2024', 'MM/dd/yyyy')  -- Returns null

-- Find all invalid dates in your data:
SELECT date_string, try_to_date(date_string, 'MM/dd/yyyy') AS parsed
FROM claims
WHERE try_to_date(date_string, 'MM/dd/yyyy') IS NULL
  AND date_string IS NOT NULL
```

### 12.8 SparkArrayIndexOutOfBoundsException

**Error:**
```
org.apache.spark.SparkArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX] The index 5 is out of bounds. The array has 3 elements.
Use `try_element_at` to tolerate accessing element at invalid index and return NULL instead.
```

**Fix:**
```sql
-- SQL
SELECT TRY_ELEMENT_AT(diagnosis_codes, 6) FROM encounters  -- 1-indexed

-- Find affected rows:
SELECT * FROM encounters WHERE SIZE(diagnosis_codes) < 6
```

---

## 13. Step-by-Step Migration Process

### Step 1: Archive Original Notebooks

```python
# Run from a control notebook to archive all notebooks in a workspace path
import datetime

archive_date = datetime.datetime.now().strftime("%Y%m%d")
source_path = "/Workspace/Production/ETL"
archive_path = f"/Workspace/Archive/{archive_date}/ETL"

# Use Databricks Workspace API or dbutils
dbutils.notebook.run("archive_helper", 600, {
  "source_path": source_path,
  "archive_path": archive_path
})
```

```bash
# Or using Databricks CLI:
databricks workspace export_dir /Workspace/Production/ETL ./local_archive/ETL --overwrite
databricks workspace import_dir ./local_archive/ETL /Workspace/Archive/20260416/ETL --overwrite
```

### Step 2: Record Baseline Delta Table Versions

```sql
-- Save current protocol versions for ALL Delta tables
CREATE OR REPLACE TABLE catalog.migration.table_baseline AS
SELECT
  table_catalog,
  table_schema,
  table_name,
  current_timestamp() AS baseline_timestamp,
  -- Get protocol info from DESCRIBE DETAIL
  'PENDING' AS migration_status
FROM system.information_schema.tables
WHERE data_source_format = 'DELTA';
```

```scala
// Programmatic baseline capture
val tables = spark.sql("""
  SELECT table_catalog, table_schema, table_name
  FROM system.information_schema.tables
  WHERE data_source_format = 'DELTA'
""").collect()

val baselines = tables.map { row =>
  val fullName = s"${row.getString(0)}.${row.getString(1)}.${row.getString(2)}"
  val detail = spark.sql(s"DESCRIBE DETAIL $fullName").first()
  (fullName, detail.getAs[Long]("numFiles"), detail.getAs[Long]("sizeInBytes"))
}
```

### Step 3: Copy Notebooks to New Folder

```bash
# Copy production notebooks to migration workspace
databricks workspace export_dir /Workspace/Production/ETL ./migration_working/ETL --overwrite

# Create migration branch folder
databricks workspace import_dir ./migration_working/ETL /Workspace/Migration_16.4/ETL --overwrite
```

### Step 4: Scan for All Patterns

```scala
// Run the comprehensive scan from Section 10.2 against all notebooks
// Use the scanNotebook function defined earlier

import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.service.workspace._

val w = new WorkspaceClient()

// Get all notebook paths
val notebookPath = "/Workspace/Migration_16.4/ETL"

// For each notebook, export source and scan
// This is the automated scanning phase that Genie Code executes
```

**Manual scan command:**
```bash
# Export all notebooks and scan locally
databricks workspace export_dir /Workspace/Migration_16.4/ETL ./scan_target --overwrite

# Run grep-based scan for CRITICAL patterns
grep -rn "CAST\s*(" ./scan_target/ > findings_cast.txt
grep -rn "\.cast(" ./scan_target/ >> findings_cast.txt
grep -rn "to_timestamp\s*(" ./scan_target/ > findings_timestamp.txt
grep -rn "to_date\s*(" ./scan_target/ >> findings_timestamp.txt
grep -rn "element_at\s*(" ./scan_target/ > findings_element.txt
grep -rn "spark\.sql\.legacy\." ./scan_target/ > findings_legacy.txt
grep -rn "spark\.sql\.ansi\.enabled.*false" ./scan_target/ > findings_ansi_override.txt
```

### Step 5: Apply Fixes

Apply fixes in this order (most critical first):

1. **Remove ANSI mode overrides** (`spark.sql.ansi.enabled = false`)
2. **Remove/update removed legacy configs** (Section 3.1)
3. **CAST to TRY_CAST** (Section 2.2) -- only for potentially invalid data
4. **Division guards** (Section 2.3)
5. **Date/timestamp parsing** (Section 2.9)
6. **Array/map access** (Sections 2.5, 2.6)
7. **Boolean comparisons** (Section 2.7)
8. **Integer overflow widening** (Section 2.4)
9. **Deprecated API updates** (Section 4)
10. **Package dependency updates** (Section 9)

### Step 6: Run on 16.4 Cluster

```json
// Create a 16.4 test cluster configuration
{
  "cluster_name": "migration-test-16.4",
  "spark_version": "16.4.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 2,
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode"
  },
  "custom_tags": {
    "purpose": "migration-testing"
  }
}
```

```json
// For ML Runtime notebooks:
{
  "cluster_name": "migration-test-16.4-ml",
  "spark_version": "16.4.x-cpu-ml-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 2
}
```

### Step 7: Validate with conversion_validator

```scala
// conversion_validator: Compare 13.3 output with 16.4 output
// Run BOTH versions and compare results

case class ValidationResult(
  tableName: String,
  rowCountMatch: Boolean,
  schemaMatch: Boolean,
  checksumMatch: Boolean,
  sampleDiffCount: Long,
  status: String
)

def validateTable(
  tableName: String,
  baseline13Path: String,
  migrated16Path: String
): ValidationResult = {
  val df13 = spark.read.format("delta").load(baseline13Path)
  val df16 = spark.read.format("delta").load(migrated16Path)

  val rowCountMatch = df13.count() == df16.count()
  val schemaMatch = df13.schema == df16.schema

  // Checksum comparison (hash all columns)
  val checksum13 = df13.selectExpr("md5(concat_ws('|', *))").distinct().count()
  val checksum16 = df16.selectExpr("md5(concat_ws('|', *))").distinct().count()
  val checksumMatch = checksum13 == checksum16

  // Find actual differences
  val diff = df13.exceptAll(df16)
  val sampleDiffCount = diff.count()

  val status = if (rowCountMatch && schemaMatch && checksumMatch) "PASS"
    else if (rowCountMatch && schemaMatch) "WARN - data differences"
    else "FAIL"

  ValidationResult(tableName, rowCountMatch, schemaMatch, checksumMatch, sampleDiffCount, status)
}
```

### Step 8: Generate conversion_report

```scala
// Generate a summary report for all migrated jobs

case class ConversionReport(
  notebookPath: String,
  scanFindings: Int,
  criticalFindings: Int,
  fixesApplied: Int,
  validationStatus: String,
  runtimeUsed: String,
  migrationDate: String,
  notes: String
)

// Write report to Delta table for tracking
val reports = Seq(
  ConversionReport(
    "/Workspace/Migration_16.4/ETL/claims_processor",
    scanFindings = 12,
    criticalFindings = 3,
    fixesApplied = 3,
    validationStatus = "PASS",
    runtimeUsed = "16.4.x-scala2.12",
    migrationDate = "2026-04-16",
    notes = "3 CAST->TRY_CAST conversions applied"
  )
)

spark.createDataFrame(reports)
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("catalog.migration.conversion_reports")
```

### Step 9: Run Parallel SIT Comparison

```scala
// System Integration Testing: Run 13.3 and 16.4 in parallel
// Compare outputs at every stage of the pipeline

// 1. Run the same job on both 13.3 and 16.4 clusters
// 2. Write outputs to separate locations
// 3. Compare with conversion_validator

val sitConfig = Map(
  "source_table" -> "catalog.schema.claims_raw",
  "baseline_output" -> "/mnt/sit/baseline_13.3/",
  "migrated_output" -> "/mnt/sit/migrated_16.4/",
  "comparison_output" -> "/mnt/sit/comparison/"
)

// Run on 13.3:
// dbutils.notebook.run("/Production/ETL/claims_processor", 3600, sitConfig + ("output_path" -> sitConfig("baseline_output")))

// Run on 16.4 (migrated version):
// dbutils.notebook.run("/Migration_16.4/ETL/claims_processor", 3600, sitConfig + ("output_path" -> sitConfig("migrated_output")))

// Compare:
val comparison = validateTable(
  "claims_processed",
  sitConfig("baseline_output"),
  sitConfig("migrated_output")
)

println(s"SIT Result: ${comparison.status}")
println(s"Row count match: ${comparison.rowCountMatch}")
println(s"Schema match: ${comparison.schemaMatch}")
println(s"Checksum match: ${comparison.checksumMatch}")
println(s"Differences found: ${comparison.sampleDiffCount}")
```

---

## 14. Documentation Links

### Official Databricks Documentation

| Resource | URL |
|----------|-----|
| DBR 16.4 LTS Release Notes | https://docs.databricks.com/en/release-notes/runtime/16.4lts.html |
| DBR 15.4 LTS Release Notes | https://docs.databricks.com/en/release-notes/runtime/15.4lts.html |
| DBR 14.3 LTS Release Notes | https://docs.databricks.com/en/release-notes/runtime/14.3lts.html |
| ANSI Mode Documentation | https://docs.databricks.com/en/sql/language-manual/ansi-compliance.html |
| Delta Lake Migration Guide | https://docs.databricks.com/en/delta/index.html |
| Deletion Vectors | https://docs.databricks.com/en/delta/deletion-vectors.html |
| Liquid Clustering | https://docs.databricks.com/en/delta/clustering.html |
| Predictive I/O | https://docs.databricks.com/en/optimizations/predictive-io.html |
| Variant Type | https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html |
| IDENTIFIER Clause | https://docs.databricks.com/en/sql/language-manual/sql-ref-identifier-clause.html |
| Feature Store to Feature Engineering Migration | https://docs.databricks.com/en/machine-learning/feature-store/index.html |

### Apache Spark Documentation

| Resource | URL |
|----------|-----|
| Spark 3.5.0 Release Notes | https://spark.apache.org/releases/spark-release-3-5-0.html |
| Spark 3.5 Migration Guide | https://spark.apache.org/docs/3.5.0/migration-guide.html |
| Spark 3.5 SQL Migration | https://spark.apache.org/docs/3.5.0/sql-migration-guide.html |
| Spark 3.5 ANSI Compliance | https://spark.apache.org/docs/3.5.0/sql-ref-ansi-compliance.html |

### Delta Lake Documentation

| Resource | URL |
|----------|-----|
| Delta Lake Releases | https://github.com/delta-io/delta/releases |
| Delta Protocol Specification | https://github.com/delta-io/delta/blob/master/PROTOCOL.md |

---

## Appendix A: Quick Reference Card

### ANSI-Safe Function Replacements

| Unsafe (throws under ANSI) | Safe Replacement | Notes |
|----------------------------|-----------------|-------|
| `CAST(x AS INT)` | `TRY_CAST(x AS INT)` | Returns null on failure |
| `x / y` | `TRY_DIVIDE(x, y)` | Returns null on zero denominator |
| `to_timestamp(s, fmt)` | `try_to_timestamp(s, fmt)` | Returns null on invalid |
| `to_date(s, fmt)` | `try_to_date(s, fmt)` | Returns null on invalid |
| `to_number(s, fmt)` | `try_to_number(s, fmt)` | Returns null on invalid |
| `element_at(arr, idx)` | `TRY_ELEMENT_AT(arr, idx)` | Returns null on out-of-bounds |
| `element_at(map, key)` | `TRY_ELEMENT_AT(map, key)` | Returns null on missing key |
| `arr[idx]` | `TRY_ELEMENT_AT(arr, idx+1)` | Note: SQL bracket is 0-indexed, TRY_ELEMENT_AT is 1-indexed |
| `map['key']` | `TRY_ELEMENT_AT(map, 'key')` | Returns null on missing key |
| `x + y` (integers) | `TRY_ADD(x, y)` | Returns null on overflow |
| `x - y` (integers) | `TRY_SUBTRACT(x, y)` | Returns null on overflow |
| `x * y` (integers) | `TRY_MULTIPLY(x, y)` | Returns null on overflow |
| `bool_col = 1` | `bool_col IS TRUE` | Proper boolean syntax |
| `bool_col = 0` | `bool_col IS FALSE` | Proper boolean syntax |

### Size of Null Migration

```sql
-- Before (13.3): size(null) returned -1
WHERE SIZE(array_col) = -1  -- null check

-- After (16.4): size(null) returns null
WHERE array_col IS NULL  -- proper null check
-- Or:
WHERE SIZE(array_col) IS NULL  -- also works
```

### Delta Table Protocol Check

```sql
-- Run before migration to baseline all table protocols
SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  d.min_reader_version,
  d.min_writer_version,
  d.num_files,
  d.size_in_bytes
FROM system.information_schema.tables t
CROSS JOIN LATERAL DESCRIBE DETAIL IDENTIFIER(
  CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name)
) d
WHERE t.data_source_format = 'DELTA'
ORDER BY t.table_catalog, t.table_schema, t.table_name;
```

---

## Appendix B: Genie Code Agent Instructions

When using this guide for automated migration, the Genie Code agent should:

1. **Read the entire notebook first** before making any changes.
2. **Classify each finding** by severity using the patterns in Section 10.
3. **Apply fixes in severity order** (CRITICAL first, then HIGH, MEDIUM, LOW).
4. **Preserve comments** -- add `// Migrated: DBR 13.3 -> 16.4` next to each change.
5. **Never disable ANSI mode** -- always use ANSI-safe patterns.
6. **Log all changes** to the conversion_report Delta table.
7. **Flag for human review** any pattern that has multiple valid fixes (e.g., CAST that might be intentionally strict).
8. **Do not change Delta table properties** unless explicitly instructed.
9. **Do not upgrade table protocols** during the code migration phase.
10. **Test each notebook** on a 16.4 cluster after migration.

### Agent Decision Matrix

| Pattern Found | Data Known Clean | Data Potentially Dirty | Unknown |
|--------------|-----------------|----------------------|---------|
| CAST | Keep CAST (validates) | TRY_CAST | TRY_CAST + flag for review |
| Division | Keep / (validated) | TRY_DIVIDE | TRY_DIVIDE + flag for review |
| to_timestamp | Keep (validated) | try_to_timestamp | try_to_timestamp + flag for review |
| Array access | Keep (bounds known) | TRY_ELEMENT_AT | TRY_ELEMENT_AT + flag for review |

**Default stance for healthcare data:** When in doubt, use the TRY_* variant AND flag for human review. Silent data loss (returning null) is preferable to job failure for production healthcare pipelines, but both should be investigated.

---

*Last updated: 2026-04-16*
*Applies to: Molina Healthcare DBR migration project*
*Path: A (Scala 13.3 LTS -> Scala 16.4 LTS, upgrade only)*
