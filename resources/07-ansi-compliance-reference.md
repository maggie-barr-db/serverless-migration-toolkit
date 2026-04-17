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

## Pattern 10: Remainder / Modulo by Zero

**Risk:** The `%` (modulo) operator throws `REMAINDER_BY_ZERO` when the divisor is zero. Also affects `PMOD()`.

**Error:** `[REMAINDER_BY_ZERO] Division by zero. Use try_remainder to tolerate divisor being 0 and return NULL instead. SQLSTATE: 22012`

### Detection

```regex
(?i)\b\w+\s*%\s*\w+
(?i)\bPMOD\s*\(
```

### Fix — SQL

```sql
-- BEFORE:
SELECT value % group_size FROM table

-- AFTER:
SELECT CASE WHEN group_size = 0 THEN NULL ELSE value % group_size END FROM table
-- OR:
SELECT value % NULLIF(group_size, 0) FROM table
```

---

## Pattern 11: Cast Overflow (Numeric Narrowing)

**Risk:** Casting a larger numeric type to a smaller one (e.g., BIGINT → INT, DOUBLE → DECIMAL) throws `CAST_OVERFLOW` if the value doesn't fit. Distinct from Pattern 1 (invalid string input) — this is about valid numbers that are too large.

**Error:** `[CAST_OVERFLOW] The value X of the type Y cannot be cast to Z due to an overflow. Use try_cast to tolerate overflow and return NULL instead.`

### Detection

```regex
# Narrowing casts: BIGINT→INT, DOUBLE→DECIMAL, etc.
(?i)CAST\s*\(.*?\bAS\s+(?:INT|SMALLINT|TINYINT|DECIMAL\s*\(\d+\s*,\s*\d+\))\s*\)
```

### Fix

```sql
-- BEFORE:
SELECT CAST(big_number AS INT) FROM table

-- AFTER:
SELECT TRY_CAST(big_number AS INT) FROM table
```

---

## Pattern 12: Insert Type Overflow

**Risk:** INSERT operations that cause type overflow throw `CAST_OVERFLOW_IN_TABLE_INSERT`. This happens when inserting data that doesn't fit the target column type.

**Error:** `[CAST_OVERFLOW_IN_TABLE_INSERT] Fail to insert a value of type X into the column Y of type Z. Use try_cast on the input value to tolerate overflow.`

### Detection

```regex
(?i)\bINSERT\s+(?:INTO|OVERWRITE)
\.write\..*\.saveAsTable\s*\(
\.write\..*\.insertInto\s*\(
```

### Fix

Apply TRY_CAST on values in the SELECT clause before INSERT, or widen the target column type.

```sql
-- BEFORE:
INSERT INTO target_table SELECT large_value FROM source

-- AFTER:
INSERT INTO target_table SELECT TRY_CAST(large_value AS INT) FROM source
```

---

## Pattern 13: Binary/Short Arithmetic Overflow

**Risk:** Arithmetic on SHORT/BYTE types throws `BINARY_ARITHMETIC_OVERFLOW`. Also affects `UnaryMinus` (negation) and `Abs` on `MIN_VALUE`.

**Error:** `[BINARY_ARITHMETIC_OVERFLOW] X causes overflow. Use <functionName> to ignore overflow.`

### Detection

```regex
# Short/byte arithmetic — cross-reference with schema
# Negation of columns that could be MIN_VALUE
(?i)-\s*F\.col\(
(?i)F\.abs\s*\(
(?i)\bABS\s*\(
```

### Fix

```sql
-- Widen before arithmetic:
SELECT CAST(short_col AS INT) + CAST(other_short AS INT) FROM table

-- For ABS on potential MIN_VALUE:
SELECT CASE WHEN col = -32768 THEN 32768 ELSE ABS(col) END FROM table
```

---

## Pattern 14: Interval Arithmetic

**Risk:** Operations on interval types can throw `INTERVAL_DIVIDED_BY_ZERO` or `INTERVAL_ARITHMETIC_OVERFLOW`.

**Error:** `[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use try_divide to tolerate divisor being 0 and return NULL instead.`

### Detection

```regex
(?i)\bINTERVAL\b.*?[*/]
(?i)make_interval\s*\(
(?i)make_dt_interval\s*\(
(?i)make_ym_interval\s*\(
```

### Fix

```sql
-- Use TRY_DIVIDE for interval division:
SELECT TRY_DIVIDE(interval_col, divisor) FROM table
```

---

## Pattern 15: Decimal Rounding Overflow

**Risk:** When rounding a DECIMAL value, if the result doesn't fit in the target precision/scale, `NUMERIC_VALUE_OUT_OF_RANGE` is thrown. Also affects `CheckOverflow` and `MakeDecimal` internally.

**Error:** `[NUMERIC_VALUE_OUT_OF_RANGE] X cannot be represented as Decimal(precision, scale). SQLSTATE: 22003`

### Detection

```regex
(?i)\bROUND\s*\(
(?i)\bBROUND\s*\(
(?i)CAST\s*\(.*?AS\s+DECIMAL\s*\(\d+\s*,\s*\d+\)\s*\)
```

### Fix

```sql
-- Widen the decimal precision:
-- BEFORE:
SELECT CAST(value AS DECIMAL(10,2)) FROM table  -- fails if value > 99999999.99

-- AFTER:
SELECT TRY_CAST(value AS DECIMAL(10,2)) FROM table
-- OR widen:
SELECT CAST(value AS DECIMAL(18,2)) FROM table
```

---

## Pattern 16: make_timestamp Errors

**Risk:** `make_timestamp()` throws `INVALID_FRACTION_OF_SECOND` if seconds are outside [0, 60], or `DATETIME_FIELD_OUT_OF_BOUNDS` if any field is invalid.

**Error:** `[INVALID_FRACTION_OF_SECOND] The fraction of sec must be in the range [0, 60). Use try_make_timestamp to tolerate invalid input.`

### Detection

```regex
(?i)\bmake_timestamp\s*\(
(?i)\bmake_date\s*\(
```

### Fix

```sql
-- BEFORE:
SELECT make_timestamp(year, month, day, hour, min, sec) FROM table

-- AFTER:
SELECT try_make_timestamp(year, month, day, hour, min, sec) FROM table
```

---

## Pattern 17: URL Parsing

**Risk:** `parse_url()` throws `INVALID_URL` on malformed URLs. `url_decode()` throws `CANNOT_DECODE_URL`.

**Error:** `[INVALID_URL] The URL X is not valid. Use try_parse_url to tolerate invalid URLs and return NULL instead.`

### Detection

```regex
(?i)\bparse_url\s*\(
(?i)\burl_decode\s*\(
```

### Fix

```sql
-- BEFORE:
SELECT parse_url(url_col, 'HOST') FROM table

-- AFTER:
SELECT try_parse_url(url_col, 'HOST') FROM table
```

---

## Pattern 18: SUM Aggregate Overflow

**Risk:** `SUM()` on integer columns can overflow and throw `ARITHMETIC_OVERFLOW` if the total exceeds the type's max value.

**Error:** `[ARITHMETIC_OVERFLOW] X causes overflow. If necessary set spark.sql.ansi.enabled to "false" to bypass this error.`

### Detection

```regex
(?i)\bSUM\s*\(\s*\w+\s*\)
```

### Fix

```sql
-- BEFORE:
SELECT SUM(int_column) FROM large_table

-- AFTER — widen the type before aggregating:
SELECT SUM(CAST(int_column AS BIGINT)) FROM large_table
```

---

## Pattern 19: Conversion to Binary

**Risk:** `to_binary()` throws `CONVERSION_INVALID_INPUT` if the input string can't be converted with the specified format.

**Error:** `[CONVERSION_INVALID_INPUT] The value X of the type Y cannot be converted to Z. Use try_to_binary to tolerate invalid input.`

### Detection

```regex
(?i)\bto_binary\s*\(
```

### Fix

```sql
SELECT try_to_binary(col, 'base64') FROM table
```

---

## Pattern 20: NextDay Invalid Input

**Risk:** `next_day()` throws `SparkIllegalArgumentException` if the day-of-week string is invalid.

### Detection

```regex
(?i)\bnext_day\s*\(
```

### Fix

Validate the day-of-week input before calling, or wrap in a try/except in UDF context.

---

## Complete ANSI Error Catalog

All 25+ error conditions triggered by ANSI mode, with their safe alternatives:

### Errors WITH built-in safe alternatives

| Error Class | Thrown When | Safe Alternative |
|-------------|-----------|-----------------|
| `CAST_OVERFLOW` | Numeric narrowing cast overflows | `TRY_CAST` |
| `CAST_INVALID_INPUT` | String can't parse to target type | `TRY_CAST` |
| `CAST_OVERFLOW_IN_TABLE_INSERT` | INSERT causes type overflow | `TRY_CAST` on input |
| `ARITHMETIC_OVERFLOW` | Add/Subtract/Multiply/Negate/Abs/SUM overflow | Widen type to BIGINT before operation |
| `DIVIDE_BY_ZERO` | Division by zero | `TRY_DIVIDE` |
| `REMAINDER_BY_ZERO` | Modulo by zero | `NULLIF(divisor, 0)` |
| `INVALID_ARRAY_INDEX` | Array out-of-bounds (bracket or `elt()`) | `TRY_ELEMENT_AT` or bounds check |
| `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` | `element_at()` out-of-bounds | `TRY_ELEMENT_AT` |
| `CANNOT_PARSE_TIMESTAMP` | `to_timestamp` / `unix_timestamp` on invalid string | `try_to_timestamp` |
| `INVALID_FRACTION_OF_SECOND` | `make_timestamp` with seconds outside [0, 60) | `try_make_timestamp` |
| `DATETIME_FIELD_OUT_OF_BOUNDS` | `make_date` / `make_timestamp` with invalid fields | `try_make_timestamp` / `try_make_date` (where available) |
| `INVALID_URL` | `parse_url` on malformed URL | `try_parse_url` |
| `CONVERSION_INVALID_INPUT` | `to_binary` on invalid input | `try_to_binary` |
| `NUMERIC_VALUE_OUT_OF_RANGE` | Decimal precision overflow | `TRY_CAST` to wider precision |
| `INTERVAL_DIVIDED_BY_ZERO` | Dividing interval by zero | `TRY_DIVIDE` |
| `INTERVAL_ARITHMETIC_OVERFLOW` | Interval multiply/divide overflow | Use wider types or guard logic |

### Errors WITHOUT built-in safe alternatives (require code guards)

| Error Class | Thrown When | Workaround |
|-------------|-----------|------------|
| `BINARY_ARITHMETIC_OVERFLOW` | Short-type arithmetic overflow | Widen to INT before operation |
| `CANNOT_PARSE_TIME` | Invalid TIME string | Validate format before parsing |
| `CANNOT_DECODE_URL` | `url_decode` on invalid URL | Validate URL format first |
| `INVALID_INTERVAL_WITH_MICROSECONDS_ADDITION` | Adding calendar interval with microseconds to date | Restructure the interval operation |
| `DATETIME_FIELD_OUT_OF_BOUNDS` (without suggestion) | Datetime field out of range (no `try_*` available) | Validate fields before constructing |

---

## Complete Spark Expressions with ANSI-Dependent Behavior

Every expression that changes behavior when ANSI mode is enabled:

| Expression | Error Thrown | Safe Alternative |
|-----------|-------------|-----------------|
| `Cast` | `CAST_OVERFLOW`, `CAST_INVALID_INPUT` | `TRY_CAST` |
| `Add` / `Subtract` / `Multiply` | `ARITHMETIC_OVERFLOW` | Widen types |
| `Divide` / `IntegralDivide` | `DIVIDE_BY_ZERO`, `ARITHMETIC_OVERFLOW` | `TRY_DIVIDE` |
| `Remainder` / `Pmod` | `REMAINDER_BY_ZERO`, `DIVIDE_BY_ZERO` | `NULLIF(divisor, 0)` |
| `UnaryMinus` / `Abs` | `ARITHMETIC_OVERFLOW` | Guard for MIN_VALUE |
| `GetArrayItem` | `INVALID_ARRAY_INDEX` | Bounds check |
| `ElementAt` | `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` | `TRY_ELEMENT_AT` |
| `Elt` | `INVALID_ARRAY_INDEX` | Bounds check |
| `CheckOverflow` / `MakeDecimal` | `NUMERIC_VALUE_OUT_OF_RANGE` | Wider precision |
| `Round` / `BRound` | `ARITHMETIC_OVERFLOW` | Wider precision |
| `Conv` | `ARITHMETIC_OVERFLOW` | Guard for overflow |
| `ToUnixTimestamp` / `UnixTimestamp` | `CANNOT_PARSE_TIMESTAMP` | `try_to_timestamp` |
| `GetTimestamp` | `CANNOT_PARSE_TIMESTAMP` | `try_to_timestamp` |
| `MakeDate` | `DATETIME_FIELD_OUT_OF_BOUNDS` | Validate fields |
| `MakeTimestamp` | `INVALID_FRACTION_OF_SECOND`, `DATETIME_FIELD_OUT_OF_BOUNDS` | `try_make_timestamp` |
| `NextDay` | `SparkIllegalArgumentException` | Validate input |
| `DateAddInterval` | `INVALID_INTERVAL_WITH_MICROSECONDS_ADDITION` | Restructure operation |
| `ParseUrl` | `INVALID_URL` | `try_parse_url` |
| `MultiplyYMInterval` / `DivideYMInterval` | `INTERVAL_ARITHMETIC_OVERFLOW` | Guard for overflow |
| `MultiplyDTInterval` / `DivideDTInterval` | `INTERVAL_ARITHMETIC_OVERFLOW` | Guard for overflow |
| `MakeInterval` | `INTERVAL_ARITHMETIC_OVERFLOW` | Guard for overflow |
| `Sum` (aggregate) | `ARITHMETIC_OVERFLOW` | Widen to BIGINT first |

---

## Complete Scan Checklist

Run all of these regex patterns against every notebook being migrated. Results should be categorized by severity:

### Critical (Will throw exceptions on serverless)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 1 | CAST to numeric | `(?i)CAST\s*\(.*?AS\s+(?:INT\|BIGINT\|DOUBLE\|DECIMAL)` | Pattern 1 |
| 2 | Division | `\b\w+\s*/\s*\w+` | Pattern 2 |
| 3 | Modulo / Remainder | `\b\w+\s*%\s*\w+` | Pattern 10 |
| 4 | Array bracket access | `\w+\[\d+\]` | Pattern 4 |
| 5 | Map bracket access | `\w+\['[^']+'\]` | Pattern 5 |
| 6 | to_date / to_timestamp | `(?i)\bto_(?:date\|timestamp)\s*\(` | Pattern 8 |
| 7 | Boolean = 0/1 | `(?i)\b\w+\s*=\s*[01]\b` | Pattern 6 |
| 8 | SUM on INT columns | `(?i)\bSUM\s*\(\s*\w+\s*\)` | Pattern 18 |

### High (Likely to throw exceptions)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 9 | CAST to DATE/TIMESTAMP | `(?i)CAST\s*\(.*?AS\s+(?:DATE\|TIMESTAMP)` | Pattern 1 |
| 10 | CAST narrowing (BIGINT→INT) | `(?i)CAST\s*\(.*?AS\s+(?:INT\|SMALLINT\|TINYINT)` | Pattern 11 |
| 11 | Integer multiplication | `(?i)\b(?:amount\|count\|total)\b.*?[*]` | Pattern 3 |
| 12 | .cast() in PySpark | `\.cast\s*\(` | Pattern 1 |
| 13 | .getItem() | `\.getItem\s*\(` | Patterns 4, 5 |
| 14 | element_at() | `(?i)\belement_at\s*\(` | Pattern 4/5 |
| 15 | make_timestamp / make_date | `(?i)\bmake_(?:timestamp\|date)\s*\(` | Pattern 16 |
| 16 | parse_url | `(?i)\bparse_url\s*\(` | Pattern 17 |
| 17 | INSERT INTO (type overflow) | `(?i)\bINSERT\s+(?:INTO\|OVERWRITE)` | Pattern 12 |

### Medium (May throw exceptions depending on data)

| # | Pattern | Regex | Fix Reference |
|---|---------|-------|--------------|
| 18 | String concatenation with \|\| | `\|\|` | Pattern 9 |
| 19 | Implicit type conversion in arithmetic | Manual review needed | Pattern 7 |
| 20 | ROUND / BROUND | `(?i)\bB?ROUND\s*\(` | Pattern 15 |
| 21 | DECIMAL precision casts | `(?i)CAST\s*\(.*?AS\s+DECIMAL\s*\(` | Pattern 15 |
| 22 | to_binary | `(?i)\bto_binary\s*\(` | Pattern 19 |
| 23 | Interval arithmetic | `(?i)\bINTERVAL\b.*?[*/]` | Pattern 14 |
| 24 | next_day | `(?i)\bnext_day\s*\(` | Pattern 20 |
| 25 | url_decode | `(?i)\burl_decode\s*\(` | Pattern 17 |
| 26 | ABS on potential MIN_VALUE | `(?i)\bABS\s*\(` | Pattern 13 |

---

## ANSI-Safe Function Quick Reference

| Unsafe (ANSI throws) | Safe Replacement | Returns on Invalid Input |
|----------------------|------------------|------------------------|
| `CAST(x AS INT)` | `TRY_CAST(x AS INT)` | `NULL` |
| `CAST(x AS DATE)` | `TRY_CAST(x AS DATE)` | `NULL` |
| `CAST(x AS DECIMAL(p,s))` | `TRY_CAST(x AS DECIMAL(p,s))` | `NULL` |
| `a / b` | `TRY_DIVIDE(a, b)` | `NULL` |
| `a % b` | `a % NULLIF(b, 0)` | `NULL` |
| `array[i]` | `TRY_ELEMENT_AT(array, i+1)` | `NULL` |
| `element_at(array, i)` | `TRY_ELEMENT_AT(array, i)` | `NULL` |
| `map['key']` | `TRY_ELEMENT_AT(map, 'key')` | `NULL` |
| `to_date(s, fmt)` | `try_to_date(s, fmt)` | `NULL` |
| `to_timestamp(s, fmt)` | `try_to_timestamp(s, fmt)` | `NULL` |
| `to_number(s, fmt)` | `try_to_number(s, fmt)` | `NULL` |
| `to_binary(s, fmt)` | `try_to_binary(s, fmt)` | `NULL` |
| `make_timestamp(...)` | `try_make_timestamp(...)` | `NULL` |
| `parse_url(url, part)` | `try_parse_url(url, part)` | `NULL` |
| `SUM(int_col)` | `SUM(CAST(int_col AS BIGINT))` | Prevents overflow |
| `col_a * col_b` (INT) | `CAST(col_a AS BIGINT) * col_b` | Prevents overflow |
| `bool_col = 1` | `bool_col IS TRUE` | Correct comparison |
| `bool_col = 0` | `bool_col IS NOT TRUE` | Handles null properly |
| `CONCAT('a', NULL)` | `CONCAT_WS('', 'a', nullable_col)` | Skips nulls |
| `INTERVAL / 0` | `TRY_DIVIDE(interval, divisor)` | `NULL` |

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
