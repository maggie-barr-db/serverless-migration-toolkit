# ANSI Compliance Reference

Complete reference for all ANSI mode behavioral changes between DBR 13.3 (ANSI off by default) and DBR 16.4 / Serverless (ANSI on by default / mandatory).

**Design principle:** NEVER recommend `spark.sql.ansi.enabled = false` as a permanent solution. Always generate ANSI-safe code fixes. ANSI behaviors are safer for healthcare data — silent nulls can mask data quality issues.

---

## ANSI Mode Summary

| Environment | ANSI Default | Can Disable? |
|-------------|-------------|-------------|
| DBR 13.3 LTS (classic) | `false` | Yes |
| DBR 16.4 LTS (classic) | `true` | Yes (but don't) |
| Serverless General Compute | `true` | **No** — mandatory |
| DBSQL Serverless | `true` | **No** — mandatory |

---

## Pattern 1: Type Casting

**Risk:** `CAST(expr AS type)` throws `NumberFormatException` or `SparkArithmeticException` instead of returning null for invalid conversions.

### Affected Conversions

| From → To | Invalid Input Example | ANSI OFF | ANSI ON |
|-----------|----------------------|----------|---------|
| STRING → INT | `"abc"` | `null` | `NumberFormatException` |
| STRING → DOUBLE | `"not_a_number"` | `null` | `NumberFormatException` |
| STRING → DATE | `"99/99/9999"` | `null` | `DateTimeException` |
| STRING → TIMESTAMP | `"invalid"` | `null` | `DateTimeException` |
| DOUBLE → INT | `99999999999.0` | Truncates/wraps | `ArithmeticException` |
| DECIMAL → INT | `999999999999.99` | Truncates | `ArithmeticException` |
| STRING → BOOLEAN | `"maybe"` | `null` | `SparkIllegalArgumentException` |

### Detection

```regex
# SQL
(?i)CAST\s*\(.*?\bAS\b\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\s*\)

# PySpark
\.cast\s*\(\s*["'](?:int|integer|bigint|smallint|tinyint|float|double|decimal|date|timestamp|boolean)["']\s*\)
\.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*\(\s*\)\s*\)

# Scala
\.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*\)
```

### Fix — SQL

```sql
-- BEFORE:
SELECT CAST(amount_str AS INT) FROM claims
SELECT CAST(date_str AS DATE) FROM claims

-- AFTER:
SELECT TRY_CAST(amount_str AS INT) FROM claims
SELECT TRY_CAST(date_str AS DATE) FROM claims
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("amount_int", F.col("amount_str").cast("int"))

# AFTER — Option 1: SQL expression with TRY_CAST:
df = df.withColumn("amount_int", F.expr("TRY_CAST(amount_str AS INT)"))

# AFTER — Option 2: Regex guard (for non-SQL contexts):
df = df.withColumn("amount_int",
    F.when(F.col("amount_str").rlike(r"^-?\d+$"),
           F.col("amount_str").cast("int"))
     .otherwise(F.lit(None).cast("int"))
)
```

### Fix — Scala

```scala
// BEFORE:
df.withColumn("amount_int", $"amount_str".cast(IntegerType))

// AFTER:
df.withColumn("amount_int",
  when($"amount_str".rlike("^-?\\d+$"), $"amount_str".cast(IntegerType))
    .otherwise(lit(null).cast(IntegerType))
)
```

---

## Pattern 2: Division by Zero

**Risk:** Dividing by zero throws `ArithmeticException` instead of returning null.

### Detection

```regex
# SQL — any division
(?i)\b\w+\s*/\s*\w+
(?i)SELECT\s+.*?/\s*.*?\s+FROM

# PySpark/Scala — column division
\.divide\s*\(
/\s*F\.col\(
/\s*\$"
```

### Fix — SQL

```sql
-- BEFORE:
SELECT total_paid / claim_count FROM summary

-- AFTER — Option 1 (preferred):
SELECT TRY_DIVIDE(total_paid, claim_count) FROM summary

-- AFTER — Option 2 (explicit guard):
SELECT CASE WHEN claim_count = 0 THEN NULL ELSE total_paid / claim_count END FROM summary

-- AFTER — Option 3 (NULLIF shorthand):
SELECT total_paid / NULLIF(claim_count, 0) FROM summary
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("avg_paid", F.col("total_paid") / F.col("claim_count"))

# AFTER:
df = df.withColumn("avg_paid",
    F.when(F.col("claim_count") != 0,
           F.col("total_paid") / F.col("claim_count"))
     .otherwise(F.lit(None).cast("double"))
)
```

### Fix — Scala

```scala
// BEFORE:
df.withColumn("avg_paid", $"total_paid" / $"claim_count")

// AFTER:
df.withColumn("avg_paid",
  when($"claim_count" =!= 0, $"total_paid" / $"claim_count")
    .otherwise(lit(null).cast(DoubleType))
)
```

---

## Pattern 3: Integer Overflow

**Risk:** Arithmetic operations that overflow throw `ArithmeticException` instead of wrapping silently.

### Detection

```regex
# SQL — multiplication/addition on numeric columns
(?i)\b(?:SUM|total|count|amount)\b.*?[*+]

# PySpark/Scala — column arithmetic
F\.col\(.*?\)\s*[*+]\s*F\.col
\$".*?"\s*[*+]\s*\$"
```

### Fix — SQL

```sql
-- BEFORE:
SELECT col_a * col_b FROM table  -- both INT, product could overflow

-- AFTER:
SELECT CAST(col_a AS BIGINT) * col_b FROM table
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("product", F.col("col_a") * F.col("col_b"))

# AFTER:
df = df.withColumn("product", F.col("col_a").cast("bigint") * F.col("col_b"))
```

---

## Pattern 4: Array Index Out of Bounds

**Risk:** Accessing array element at an index that doesn't exist throws `ArrayIndexOutOfBoundsException` instead of returning null.

### Detection

```regex
# SQL — array bracket access
\w+\[\d+\]

# PySpark
\.getItem\s*\(
F\.element_at\s*\(
F\.split\s*\(.*?\)\s*\[

# Scala
\.getItem\s*\(
\.apply\s*\(\d+\)
split\(.*?\)\(\d+\)
```

### Fix — SQL

```sql
-- BEFORE:
SELECT array_col[5] FROM table

-- AFTER (element_at is 1-indexed):
SELECT TRY_ELEMENT_AT(array_col, 6) FROM table

-- For split results:
-- BEFORE:
SELECT split(name, ' ')[1] FROM table

-- AFTER:
SELECT TRY_ELEMENT_AT(split(name, ' '), 2) FROM table
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("last_name", F.split(F.col("name"), " ").getItem(1))

# AFTER — Option 1 (TRY_ELEMENT_AT via expr):
df = df.withColumn("last_name",
    F.expr("TRY_ELEMENT_AT(split(name, ' '), 2)")  # 1-indexed
)

# AFTER — Option 2 (bounds check):
df = df.withColumn("last_name",
    F.when(F.size(F.split(F.col("name"), " ")) > 1,
           F.split(F.col("name"), " ").getItem(1))
     .otherwise(F.lit(None))
)
```

### Fix — Scala

```scala
// BEFORE:
df.withColumn("last_name", split($"name", " ")(1))

// AFTER:
df.withColumn("last_name",
  when(size(split($"name", " ")) > 1, split($"name", " ").getItem(1))
    .otherwise(lit(null))
)
```

---

## Pattern 5: Map Key Not Found

**Risk:** Accessing a map with a key that doesn't exist throws `NoSuchElementException` instead of returning null.

### Detection

```regex
# SQL
\w+\['[^']+'\]
\w+\["[^"]+"\]

# PySpark
\.getItem\s*\(\s*["']
F\.element_at\s*\(

# Scala
\.getItem\s*\(\s*"
\.apply\s*\(\s*"
```

### Fix — SQL

```sql
-- BEFORE:
SELECT config_map['missing_key'] FROM table

-- AFTER:
SELECT TRY_ELEMENT_AT(config_map, 'missing_key') FROM table
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("val", F.col("config_map").getItem("key"))

# AFTER:
df = df.withColumn("val", F.expr("TRY_ELEMENT_AT(config_map, 'key')"))
```

### Fix — Scala

```scala
// BEFORE:
df.withColumn("val", $"config_map".getItem("key"))

// AFTER:
df.withColumn("val",
  when(map_contains_key($"config_map", lit("key")), $"config_map".getItem("key"))
    .otherwise(lit(null))
)
```

---

## Pattern 6: Boolean Comparisons

**Risk:** Comparing a BOOLEAN column to an integer literal (`boolean_col = 1`) throws an error in ANSI mode because implicit BOOLEAN→INT cast is not allowed.

### Detection

```regex
# SQL
(?i)\b\w+\s*=\s*[01]\b(?![\.\d])
(?i)\bWHERE\b.*?\b(?:true|false)\b\s*=\s*[01]
(?i)\bWHEN\b.*?=\s*[01]\b

# Note: be careful to distinguish boolean columns from numeric columns.
# Cross-reference with schema.
```

### Fix — SQL

```sql
-- BEFORE:
SELECT * FROM table WHERE is_active = 1
SELECT * FROM table WHERE is_active = 0
SELECT CASE WHEN flag = 1 THEN 'yes' END FROM table

-- AFTER:
SELECT * FROM table WHERE is_active IS TRUE
SELECT * FROM table WHERE is_active IS NOT TRUE  -- or IS FALSE, depending on null intent
SELECT CASE WHEN flag IS TRUE THEN 'yes' END FROM table
```

### Fix — PySpark

```python
# BEFORE:
df = df.filter(F.col("is_active") == 1)
df = df.filter(F.col("is_active") == 0)

# AFTER:
df = df.filter(F.col("is_active") == True)   # Note: PySpark accepts Python True
df = df.filter(F.col("is_active") == False)
# OR:
df = df.filter(F.col("is_active"))            # Truthy filter
df = df.filter(~F.col("is_active"))           # Falsy filter
```

---

## Pattern 7: String-to-Numeric Implicit Conversions

**Risk:** Operations that implicitly convert strings to numbers (e.g., `"5" + 3`) throw errors in ANSI mode if the string is not a valid number.

### Detection

```regex
# SQL — string column in arithmetic
# Hard to detect statically. Look for mixed-type expressions.
(?i)(?:string_col|varchar_col)\s*[+\-*/]\s*\d+

# PySpark/Scala — string column used in arithmetic
# Cross-reference with schema to find string columns in math operations
```

### Fix — SQL

```sql
-- BEFORE (implicit conversion):
SELECT string_amount + 100 FROM table

-- AFTER (explicit safe cast):
SELECT TRY_CAST(string_amount AS DOUBLE) + 100 FROM table
```

---

## Pattern 8: Date/Timestamp Parsing

**Risk:** `to_date()` and `to_timestamp()` throw errors on invalid date/timestamp strings instead of returning null.

### Detection

```regex
# SQL
(?i)\bto_date\s*\(
(?i)\bto_timestamp\s*\(
(?i)\bdate_format\s*\(

# PySpark
F\.to_date\s*\(
F\.to_timestamp\s*\(
```

### Fix — SQL

```sql
-- BEFORE:
SELECT to_date(date_str, 'MM/dd/yyyy') FROM table
SELECT to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') FROM table

-- AFTER:
SELECT try_to_date(date_str, 'MM/dd/yyyy') FROM table
SELECT try_to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') FROM table
```

### Fix — PySpark

```python
# BEFORE:
df = df.withColumn("parsed", F.to_date(F.col("date_str"), "MM/dd/yyyy"))

# AFTER:
df = df.withColumn("parsed", F.expr("try_to_date(date_str, 'MM/dd/yyyy')"))

# Note: There is no F.try_to_date() function in PySpark.
# You must use F.expr() with the SQL function name.
```

---

## Pattern 9: String Concatenation with NULL

**Risk:** In ANSI mode, `||` (string concatenation operator) propagates nulls: `'hello' || NULL` returns `NULL`. In non-ANSI mode, behavior varies.

### Detection

```regex
# SQL — || concatenation
(?i)\|\|

# Also check CONCAT with potentially null arguments:
(?i)\bCONCAT\s*\(
```

### Fix — SQL

```sql
-- BEFORE (may produce null if any part is null):
SELECT first_name || ' ' || last_name FROM members

-- AFTER (null-safe):
SELECT CONCAT_WS(' ', first_name, last_name) FROM members
-- OR:
SELECT COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') FROM members
```

---

## Pattern 10: ROUND with Negative Scale

**Risk:** `ROUND(x, -n)` where -n would round past the size of the number behaves differently.

### Detection

```regex
(?i)\bROUND\s*\(.*?,\s*-\d+\)
```

### Fix

Generally not a common issue. If found, verify the behavior manually.

---

## Complete Scan Checklist

Run all of these regex patterns against every notebook being migrated. Results should be categorized by severity:

### Critical (Will throw exceptions on serverless)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 1 | CAST to numeric | `(?i)CAST\s*\(.*?AS\s+(?:INT\|BIGINT\|DOUBLE\|DECIMAL)` | Pattern 1 |
| 2 | Division | `\b\w+\s*/\s*\w+` | Pattern 2 |
| 3 | Array bracket access | `\w+\[\d+\]` | Pattern 4 |
| 4 | Map bracket access | `\w+\['[^']+'\]` | Pattern 5 |
| 5 | to_date / to_timestamp | `(?i)\bto_(?:date\|timestamp)\s*\(` | Pattern 8 |
| 6 | Boolean = 0/1 | `(?i)\b\w+\s*=\s*[01]\b` | Pattern 6 |

### High (Likely to throw exceptions)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 7 | CAST to DATE/TIMESTAMP | `(?i)CAST\s*\(.*?AS\s+(?:DATE\|TIMESTAMP)` | Pattern 1 |
| 8 | Integer multiplication | `(?i)\b(?:amount\|count\|total)\b.*?[*]` | Pattern 3 |
| 9 | .cast() in PySpark | `\.cast\s*\(` | Pattern 1 |
| 10 | .getItem() | `\.getItem\s*\(` | Patterns 4, 5 |

### Medium (May throw exceptions depending on data)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 11 | String concatenation with || | `\|\|` | Pattern 9 |
| 12 | Implicit type conversion in arithmetic | Manual review needed | Pattern 7 |
| 13 | ROUND with negative scale | `(?i)\bROUND\s*\(.*?-\d+` | Pattern 10 |

---

## ANSI-Safe Function Quick Reference

| Unsafe (ANSI throws) | Safe Replacement | Returns on Invalid Input |
|----------------------|------------------|------------------------|
| `CAST(x AS INT)` | `TRY_CAST(x AS INT)` | `NULL` |
| `CAST(x AS DATE)` | `TRY_CAST(x AS DATE)` | `NULL` |
| `a / b` | `TRY_DIVIDE(a, b)` | `NULL` |
| `a % b` | `a % NULLIF(b, 0)` | `NULL` |
| `array[i]` | `TRY_ELEMENT_AT(array, i+1)` | `NULL` |
| `map['key']` | `TRY_ELEMENT_AT(map, 'key')` | `NULL` |
| `to_date(s, fmt)` | `try_to_date(s, fmt)` | `NULL` |
| `to_timestamp(s, fmt)` | `try_to_timestamp(s, fmt)` | `NULL` |
| `to_number(s, fmt)` | `try_to_number(s, fmt)` | `NULL` |
| `bool_col = 1` | `bool_col IS TRUE` | Correct comparison |
| `bool_col = 0` | `bool_col IS NOT TRUE` | Handles null properly |
| `CONCAT('a', NULL)` | `CONCAT_WS('', 'a', nullable_col)` | Skips nulls |

---

## Healthcare-Specific Guidance

For Molina's regulated healthcare pipelines:

1. **ANSI mode is BETTER for healthcare data.** Silent null returns can mask data quality issues in claims, eligibility, and pharmacy data. Explicit errors surface problems immediately.

2. **TRY_CAST is not a free pass.** When you replace CAST with TRY_CAST, you're now silently producing NULL where previously there was an error. For healthcare data, consider adding a DATA QUALITY CHECK:

```sql
-- After applying TRY_CAST, check for conversion failures:
SELECT COUNT(*) as failed_casts
FROM claims
WHERE amount_str IS NOT NULL
  AND TRY_CAST(amount_str AS DECIMAL(18,2)) IS NULL;

-- If this count is > 0, investigate the source data.
```

3. **TRY_DIVIDE nulls change aggregates.** If `total / count` previously never had zero denominators, replacing with TRY_DIVIDE is safe. But if it DID have zeros and the code relied on the null result, verify downstream behavior.

4. **Date parsing changes affect PHI timelines.** Healthcare dates (DOB, admission, discharge) must parse correctly. After migration, validate all date columns using the conversion_validator Check 7 suite.
