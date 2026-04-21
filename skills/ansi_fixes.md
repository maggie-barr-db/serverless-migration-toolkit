# ANSI-Safe Code Fixes

Genie Code skill for applying ANSI-compliant code fixes to SQL, PySpark, and Scala workloads migrating to serverless compute.

---

## Overview

ANSI mode is **mandatory** on serverless compute and **default on DBR 16.4 LTS**. It cannot be disabled on serverless.

| Environment | ANSI Default | Can Disable? |
|---|---|---|
| DBR 13.3 LTS (classic) | `false` | Yes |
| DBR 16.4 LTS (classic) | `true` | Yes (but don't) |
| Serverless General Compute | `true` | **No** |
| DBSQL Serverless | `true` | **No** |

**NEVER recommend `spark.sql.ansi.enabled = false`.** Always fix the code. ANSI mode surfaces real data quality issues that silent nulls would hide.

---

## Quick Reference Table

| Unsafe (throws in ANSI) | Safe Replacement | Returns on Invalid |
|---|---|---|
| `CAST(x AS INT)` | `TRY_CAST(x AS INT)` | `NULL` |
| `CAST(x AS DATE)` | `TRY_CAST(x AS DATE)` | `NULL` |
| `CAST(x AS TIMESTAMP)` | `TRY_CAST(x AS TIMESTAMP)` | `NULL` |
| `CAST(x AS BOOLEAN)` | `TRY_CAST(x AS BOOLEAN)` | `NULL` |
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
| `bool_col = 0` | `bool_col IS NOT TRUE` | Handles NULL properly |
| `'a' \|\| NULL` | `CONCAT_WS('', 'a', nullable_col)` | Skips NULLs |
| `INTERVAL / 0` | `TRY_DIVIDE(interval, divisor)` | `NULL` |

---

## Fix Patterns

### 1. CAST / TRY_CAST (Numeric, Date, Timestamp, Boolean)

**Error:** `[CAST_INVALID_INPUT]` or `[CAST_OVERFLOW]` -- Input cannot parse or overflows the target type.

Applies to all CAST targets: INT, BIGINT, DOUBLE, DECIMAL, DATE, TIMESTAMP, BOOLEAN.

**Detection regex:**
```regex
(?i)CAST\s*\(.*?\bAS\b\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\s*\)
\.cast\s*\(\s*["'](?:int|bigint|float|double|decimal|date|timestamp|boolean)["']\s*\)
\.cast\s*\(\s*(?:IntegerType|LongType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*[\(\)]*\s*\)
```

**SQL:**
```sql
-- BEFORE:
SELECT CAST(amount_str AS INT), CAST(date_str AS DATE), CAST(ts_str AS TIMESTAMP) FROM t
-- AFTER:
SELECT TRY_CAST(amount_str AS INT), TRY_CAST(date_str AS DATE), TRY_CAST(ts_str AS TIMESTAMP) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("amt", F.col("amount_str").cast("int"))
# AFTER:
df = df.withColumn("amt", F.expr("TRY_CAST(amount_str AS INT)"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("amt", $"amount_str".cast(IntegerType))
// AFTER:
df.withColumn("amt", expr("TRY_CAST(amount_str AS INT)"))
```

---

### 2. Division by Zero / TRY_DIVIDE

**Error:** `[DIVIDE_BY_ZERO]` -- Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.

**Detection regex:**
```regex
(?i)\b\w+\s*/\s*\w+
\.divide\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT total_paid / claim_count FROM summary
-- AFTER:
SELECT TRY_DIVIDE(total_paid, claim_count) FROM summary
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("avg", F.col("total") / F.col("cnt"))
# AFTER:
df = df.withColumn("avg",
    F.when(F.col("cnt") != 0, F.col("total") / F.col("cnt"))
     .otherwise(F.lit(None).cast("double"))
)
```

**Scala:**
```scala
// BEFORE:
df.withColumn("avg", $"total" / $"cnt")
// AFTER:
df.withColumn("avg",
  when($"cnt" =!= 0, $"total" / $"cnt")
    .otherwise(lit(null).cast(DoubleType))
)
```

---

### 3. Remainder / Modulo by Zero

**Error:** `[REMAINDER_BY_ZERO]` -- Division by zero. Use `try_remainder` to tolerate divisor being 0 and return NULL instead.

**Detection regex:**
```regex
(?i)\b\w+\s*%\s*\w+
(?i)\bPMOD\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT value % group_size FROM t
-- AFTER:
SELECT value % NULLIF(group_size, 0) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("rem", F.col("value") % F.col("group_size"))
# AFTER:
df = df.withColumn("rem",
    F.when(F.col("group_size") != 0, F.col("value") % F.col("group_size"))
     .otherwise(F.lit(None).cast("int"))
)
```

**Scala:**
```scala
// BEFORE:
df.withColumn("rem", $"value" % $"group_size")
// AFTER:
df.withColumn("rem",
  when($"group_size" =!= 0, $"value" % $"group_size")
    .otherwise(lit(null).cast(IntegerType))
)
```

---

### 4. Array Access / TRY_ELEMENT_AT

**Error:** `[INVALID_ARRAY_INDEX]` -- Array index out of bounds.

**Detection regex:**
```regex
\w+\[\d+\]
\.getItem\s*\(
F\.element_at\s*\(
```

**SQL:**
```sql
-- BEFORE (0-indexed bracket):
SELECT split(name, ' ')[1] FROM t
-- AFTER (1-indexed TRY_ELEMENT_AT):
SELECT TRY_ELEMENT_AT(split(name, ' '), 2) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("last", F.split(F.col("name"), " ").getItem(1))
# AFTER:
df = df.withColumn("last", F.expr("TRY_ELEMENT_AT(split(name, ' '), 2)"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("last", split($"name", " ")(1))
// AFTER:
df.withColumn("last",
  when(size(split($"name", " ")) > 1, split($"name", " ").getItem(1))
    .otherwise(lit(null))
)
```

---

### 5. Map Access / TRY_ELEMENT_AT

**Error:** `[MAP_KEY_DOES_NOT_EXIST]` -- Map key not found.

**Detection regex:**
```regex
\w+\['[^']+'\]
\w+\["[^"]+"\]
\.getItem\s*\(\s*["']
```

**SQL:**
```sql
-- BEFORE:
SELECT config_map['missing_key'] FROM t
-- AFTER:
SELECT TRY_ELEMENT_AT(config_map, 'missing_key') FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("val", F.col("config_map").getItem("key"))
# AFTER:
df = df.withColumn("val", F.expr("TRY_ELEMENT_AT(config_map, 'key')"))
```

**Scala:**
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

### 6. Boolean = INT / IS TRUE

**Error:** Implicit BOOLEAN-to-INT cast not allowed in ANSI mode.

**Detection regex:**
```regex
(?i)\b\w+\s*=\s*[01]\b(?![\.\d])
(?i)\bWHEN\b.*?=\s*[01]\b
```

**SQL:**
```sql
-- BEFORE:
SELECT * FROM t WHERE is_active = 1
SELECT * FROM t WHERE is_active = 0
-- AFTER:
SELECT * FROM t WHERE is_active IS TRUE
SELECT * FROM t WHERE is_active IS NOT TRUE
```

**PySpark:**
```python
# BEFORE:
df = df.filter(F.col("is_active") == 1)
# AFTER:
df = df.filter(F.col("is_active") == True)
# OR:
df = df.filter(F.col("is_active"))
```

**Scala:**
```scala
// BEFORE:
df.filter($"is_active" === 1)
// AFTER:
df.filter($"is_active" === true)
```

---

### 7. to_date / try_to_date

**Error:** `[CANNOT_PARSE_TIMESTAMP]` -- Invalid date string for given format.

**Detection regex:**
```regex
(?i)\bto_date\s*\(
F\.to_date\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT to_date(date_str, 'MM/dd/yyyy') FROM t
-- AFTER:
SELECT try_to_date(date_str, 'MM/dd/yyyy') FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("dt", F.to_date(F.col("date_str"), "MM/dd/yyyy"))
# AFTER (no F.try_to_date exists -- use expr):
df = df.withColumn("dt", F.expr("try_to_date(date_str, 'MM/dd/yyyy')"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("dt", to_date($"date_str", "MM/dd/yyyy"))
// AFTER:
df.withColumn("dt", expr("try_to_date(date_str, 'MM/dd/yyyy')"))
```

---

### 8. to_timestamp / try_to_timestamp

**Error:** `[CANNOT_PARSE_TIMESTAMP]` -- Invalid timestamp string for given format.

**Detection regex:**
```regex
(?i)\bto_timestamp\s*\(
F\.to_timestamp\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') FROM t
-- AFTER:
SELECT try_to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss') FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("ts", F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
# AFTER:
df = df.withColumn("ts", F.expr("try_to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss')"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("ts", to_timestamp($"ts_str", "yyyy-MM-dd HH:mm:ss"))
// AFTER:
df.withColumn("ts", expr("try_to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss')"))
```

---

### 9. make_timestamp / try_make_timestamp

**Error:** `[INVALID_FRACTION_OF_SECOND]` -- Seconds outside [0, 60). `[DATETIME_FIELD_OUT_OF_BOUNDS]` -- Invalid field values.

**Detection regex:**
```regex
(?i)\bmake_timestamp\s*\(
(?i)\bmake_date\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT make_timestamp(yr, mo, dy, hr, mi, sc) FROM t
-- AFTER:
SELECT try_make_timestamp(yr, mo, dy, hr, mi, sc) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("ts", F.expr("make_timestamp(yr, mo, dy, hr, mi, sc)"))
# AFTER:
df = df.withColumn("ts", F.expr("try_make_timestamp(yr, mo, dy, hr, mi, sc)"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("ts", expr("make_timestamp(yr, mo, dy, hr, mi, sc)"))
// AFTER:
df.withColumn("ts", expr("try_make_timestamp(yr, mo, dy, hr, mi, sc)"))
```

---

### 10. Integer Overflow (Type Widening)

**Error:** `[ARITHMETIC_OVERFLOW]` -- Multiplication/addition of INT values overflows.

**Detection regex:**
```regex
(?i)\b(?:SUM|total|count|amount)\b.*?[*+]
F\.col\(.*?\)\s*[*+]\s*F\.col
\$".*?"\s*[*+]\s*\$"
```

**SQL:**
```sql
-- BEFORE:
SELECT col_a * col_b FROM t  -- both INT, product can overflow
-- AFTER:
SELECT CAST(col_a AS BIGINT) * col_b FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("product", F.col("col_a") * F.col("col_b"))
# AFTER:
df = df.withColumn("product", F.col("col_a").cast("bigint") * F.col("col_b"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("product", $"col_a" * $"col_b")
// AFTER:
df.withColumn("product", $"col_a".cast(LongType) * $"col_b")
```

---

### 11. SUM Overflow (Widen to BIGINT)

**Error:** `[ARITHMETIC_OVERFLOW]` -- SUM on INT column exceeds INT max.

**Detection regex:**
```regex
(?i)\bSUM\s*\(\s*\w+\s*\)
```

**SQL:**
```sql
-- BEFORE:
SELECT SUM(int_column) FROM large_table
-- AFTER:
SELECT SUM(CAST(int_column AS BIGINT)) FROM large_table
```

**PySpark:**
```python
# BEFORE:
df.groupBy("grp").agg(F.sum("int_col"))
# AFTER:
df.groupBy("grp").agg(F.sum(F.col("int_col").cast("bigint")))
```

**Scala:**
```scala
// BEFORE:
df.groupBy("grp").agg(sum("int_col"))
// AFTER:
df.groupBy("grp").agg(sum($"int_col".cast(LongType)))
```

---

### 12. Cast Overflow (Numeric Narrowing)

**Error:** `[CAST_OVERFLOW]` -- Value of type BIGINT cannot be cast to INT due to overflow.

**Detection regex:**
```regex
(?i)CAST\s*\(.*?\bAS\s+(?:INT|SMALLINT|TINYINT|DECIMAL\s*\(\d+\s*,\s*\d+\))\s*\)
```

**SQL:**
```sql
-- BEFORE:
SELECT CAST(big_number AS INT) FROM t
-- AFTER:
SELECT TRY_CAST(big_number AS INT) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("narrow", F.col("big_number").cast("int"))
# AFTER:
df = df.withColumn("narrow", F.expr("TRY_CAST(big_number AS INT)"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("narrow", $"big_number".cast(IntegerType))
// AFTER:
df.withColumn("narrow", expr("TRY_CAST(big_number AS INT)"))
```

---

### 13. Insert Type Overflow

**Error:** `[CAST_OVERFLOW_IN_TABLE_INSERT]` -- Fail to insert a value of type X into column Y of type Z.

**Detection regex:**
```regex
(?i)\bINSERT\s+(?:INTO|OVERWRITE)
\.write\..*\.saveAsTable\s*\(
\.write\..*\.insertInto\s*\(
```

**SQL:**
```sql
-- BEFORE:
INSERT INTO target SELECT large_value FROM source
-- AFTER:
INSERT INTO target SELECT TRY_CAST(large_value AS INT) FROM source
```

**PySpark:**
```python
# BEFORE:
df.write.insertInto("target")
# AFTER -- apply TRY_CAST before write:
df = df.withColumn("col", F.expr("TRY_CAST(col AS INT)"))
df.write.insertInto("target")
```

**Scala:**
```scala
// BEFORE:
df.write.insertInto("target")
// AFTER:
df.withColumn("col", expr("TRY_CAST(col AS INT)")).write.insertInto("target")
```

---

### 14. parse_url / try_parse_url

**Error:** `[INVALID_URL]` -- The URL is not valid. Use `try_parse_url` to tolerate invalid URLs and return NULL instead.

**Detection regex:**
```regex
(?i)\bparse_url\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT parse_url(url_col, 'HOST') FROM t
-- AFTER:
SELECT try_parse_url(url_col, 'HOST') FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("host", F.expr("parse_url(url_col, 'HOST')"))
# AFTER:
df = df.withColumn("host", F.expr("try_parse_url(url_col, 'HOST')"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("host", expr("parse_url(url_col, 'HOST')"))
// AFTER:
df.withColumn("host", expr("try_parse_url(url_col, 'HOST')"))
```

---

### 15. to_binary / try_to_binary

**Error:** `[CONVERSION_INVALID_INPUT]` -- Value cannot be converted to binary. Use `try_to_binary` to tolerate invalid input.

**Detection regex:**
```regex
(?i)\bto_binary\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT to_binary(col, 'base64') FROM t
-- AFTER:
SELECT try_to_binary(col, 'base64') FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("bin", F.expr("to_binary(col, 'base64')"))
# AFTER:
df = df.withColumn("bin", F.expr("try_to_binary(col, 'base64')"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("bin", expr("to_binary(col, 'base64')"))
// AFTER:
df.withColumn("bin", expr("try_to_binary(col, 'base64')"))
```

---

### 16. String Concatenation NULL Propagation

**Risk:** `||` operator propagates NULLs in ANSI mode: `'hello' || NULL` returns `NULL`.

**Detection regex:**
```regex
(?i)\|\|
(?i)\bCONCAT\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT first_name || ' ' || last_name FROM members
-- AFTER:
SELECT CONCAT_WS(' ', first_name, last_name) FROM members
-- OR:
SELECT COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') FROM members
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("full", F.concat(F.col("first"), F.lit(" "), F.col("last")))
# AFTER:
df = df.withColumn("full", F.concat_ws(" ", F.col("first"), F.col("last")))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("full", concat($"first", lit(" "), $"last"))
// AFTER:
df.withColumn("full", concat_ws(" ", $"first", $"last"))
```

---

### 17. Decimal Precision Overflow

**Error:** `[NUMERIC_VALUE_OUT_OF_RANGE]` -- Value cannot be represented as Decimal(precision, scale).

**Detection regex:**
```regex
(?i)CAST\s*\(.*?AS\s+DECIMAL\s*\(\d+\s*,\s*\d+\)\s*\)
(?i)\bROUND\s*\(
(?i)\bBROUND\s*\(
```

**SQL:**
```sql
-- BEFORE:
SELECT CAST(value AS DECIMAL(10,2)) FROM t  -- fails if value > 99999999.99
-- AFTER:
SELECT TRY_CAST(value AS DECIMAL(10,2)) FROM t
-- OR widen precision:
SELECT CAST(value AS DECIMAL(18,2)) FROM t
```

**PySpark:**
```python
# BEFORE:
df = df.withColumn("d", F.col("value").cast("decimal(10,2)"))
# AFTER:
df = df.withColumn("d", F.expr("TRY_CAST(value AS DECIMAL(10,2))"))
```

**Scala:**
```scala
// BEFORE:
df.withColumn("d", $"value".cast(DecimalType(10, 2)))
// AFTER:
df.withColumn("d", expr("TRY_CAST(value AS DECIMAL(10,2))"))
```

---

## Complete Error Catalog

### Errors WITH Built-in Safe Alternatives

| Error Class | Thrown When | Safe Alternative |
|---|---|---|
| `CAST_INVALID_INPUT` | String cannot parse to target type | `TRY_CAST` |
| `CAST_OVERFLOW` | Numeric narrowing cast overflows | `TRY_CAST` |
| `CAST_OVERFLOW_IN_TABLE_INSERT` | INSERT causes type overflow | `TRY_CAST` on input |
| `ARITHMETIC_OVERFLOW` | Add/Subtract/Multiply/Negate/Abs/SUM overflow | Widen type to BIGINT |
| `DIVIDE_BY_ZERO` | Division by zero | `TRY_DIVIDE` |
| `REMAINDER_BY_ZERO` | Modulo by zero | `NULLIF(divisor, 0)` |
| `INVALID_ARRAY_INDEX` | Array bracket access out of bounds | `TRY_ELEMENT_AT` or bounds check |
| `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` | `element_at()` out of bounds | `TRY_ELEMENT_AT` |
| `CANNOT_PARSE_TIMESTAMP` | `to_timestamp` / `unix_timestamp` on invalid string | `try_to_timestamp` |
| `INVALID_FRACTION_OF_SECOND` | `make_timestamp` with seconds outside [0, 60) | `try_make_timestamp` |
| `DATETIME_FIELD_OUT_OF_BOUNDS` | `make_date`/`make_timestamp` with invalid fields | `try_make_timestamp` / `try_make_date` |
| `INVALID_URL` | `parse_url` on malformed URL | `try_parse_url` |
| `CONVERSION_INVALID_INPUT` | `to_binary` on invalid input | `try_to_binary` |
| `NUMERIC_VALUE_OUT_OF_RANGE` | Decimal precision overflow | `TRY_CAST` to wider precision |
| `INTERVAL_DIVIDED_BY_ZERO` | Dividing interval by zero | `TRY_DIVIDE` |
| `INTERVAL_ARITHMETIC_OVERFLOW` | Interval multiply/divide overflow | Use wider types or guard logic |

### Errors WITHOUT Built-in Safe Alternatives (Require Code Guards)

| Error Class | Thrown When | Workaround |
|---|---|---|
| `BINARY_ARITHMETIC_OVERFLOW` | SHORT/BYTE arithmetic overflow | Widen to INT before operation |
| `CANNOT_PARSE_TIME` | Invalid TIME string | Validate format before parsing |
| `CANNOT_DECODE_URL` | `url_decode` on invalid URL | Validate URL format first |
| `INVALID_INTERVAL_WITH_MICROSECONDS_ADDITION` | Adding calendar interval with microseconds to date | Restructure interval operation |

### Complete Spark Expressions with ANSI-Dependent Behavior

| Expression | Error Thrown | Safe Alternative |
|---|---|---|
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

## Scan Checklist

### Critical (Will throw on serverless)

| # | Pattern | Regex | Fix |
|---|---|---|---|
| 1 | CAST to numeric | `(?i)CAST\s*\(.*?AS\s+(?:INT\|BIGINT\|DOUBLE\|DECIMAL)` | TRY_CAST |
| 2 | Division | `\b\w+\s*/\s*\w+` | TRY_DIVIDE |
| 3 | Modulo | `\b\w+\s*%\s*\w+` | NULLIF(divisor,0) |
| 4 | Array bracket | `\w+\[\d+\]` | TRY_ELEMENT_AT |
| 5 | Map bracket | `\w+\['[^']+'\]` | TRY_ELEMENT_AT |
| 6 | to_date/to_timestamp | `(?i)\bto_(?:date\|timestamp)\s*\(` | try_to_date/try_to_timestamp |
| 7 | Boolean = 0/1 | `(?i)\b\w+\s*=\s*[01]\b` | IS TRUE / IS NOT TRUE |
| 8 | SUM on INT | `(?i)\bSUM\s*\(\s*\w+\s*\)` | SUM(CAST(col AS BIGINT)) |

### High (Likely to throw)

| # | Pattern | Regex | Fix |
|---|---|---|---|
| 9 | CAST to DATE/TIMESTAMP | `(?i)CAST\s*\(.*?AS\s+(?:DATE\|TIMESTAMP)` | TRY_CAST |
| 10 | Narrowing cast | `(?i)CAST\s*\(.*?AS\s+(?:INT\|SMALLINT\|TINYINT)` | TRY_CAST |
| 11 | INT multiplication | `(?i)\b(?:amount\|count\|total)\b.*?[*]` | Widen to BIGINT |
| 12 | .cast() PySpark | `\.cast\s*\(` | TRY_CAST via expr |
| 13 | .getItem() | `\.getItem\s*\(` | TRY_ELEMENT_AT |
| 14 | element_at() | `(?i)\belement_at\s*\(` | TRY_ELEMENT_AT |
| 15 | make_timestamp | `(?i)\bmake_(?:timestamp\|date)\s*\(` | try_make_timestamp |
| 16 | parse_url | `(?i)\bparse_url\s*\(` | try_parse_url |
| 17 | INSERT overflow | `(?i)\bINSERT\s+(?:INTO\|OVERWRITE)` | TRY_CAST on input |

### Medium (May throw depending on data)

| # | Pattern | Regex | Fix |
|---|---|---|---|
| 18 | String concat \|\| | `\|\|` | CONCAT_WS or COALESCE |
| 19 | ROUND / BROUND | `(?i)\bB?ROUND\s*\(` | Wider precision |
| 20 | DECIMAL precision | `(?i)CAST\s*\(.*?AS\s+DECIMAL\s*\(` | TRY_CAST |
| 21 | to_binary | `(?i)\bto_binary\s*\(` | try_to_binary |
| 22 | Interval arithmetic | `(?i)\bINTERVAL\b.*?[*/]` | TRY_DIVIDE |
| 23 | next_day | `(?i)\bnext_day\s*\(` | Validate input |
| 24 | url_decode | `(?i)\burl_decode\s*\(` | Validate URL first |
| 25 | ABS on MIN_VALUE | `(?i)\bABS\s*\(` | Guard for MIN_VALUE |
| 26 | Implicit string-to-numeric | Manual review | TRY_CAST explicitly |

---

## Healthcare Data Guidance

### Why ANSI Mode is Better for Healthcare

ANSI mode is **safer** for regulated healthcare data pipelines. Silent null returns from failed casts can mask data quality issues in claims, eligibility, and pharmacy data. ANSI mode surfaces these problems immediately rather than propagating corrupt data downstream.

### Data Quality Check After TRY_CAST

When replacing CAST with TRY_CAST, you are now producing NULL where there was previously an error. For healthcare data, **always add a data quality check**:

```sql
-- Check for conversion failures after applying TRY_CAST:
SELECT COUNT(*) AS failed_casts
FROM claims
WHERE amount_str IS NOT NULL
  AND TRY_CAST(amount_str AS DECIMAL(18,2)) IS NULL;

-- If count > 0, investigate the source data.
```

```python
# PySpark data quality check:
failed = df.filter(
    F.col("amount_str").isNotNull() &
    F.expr("TRY_CAST(amount_str AS DECIMAL(18,2))").isNull()
).count()
if failed > 0:
    print(f"WARNING: {failed} rows failed numeric conversion -- investigate source data")
```

### TRY_DIVIDE Changes Aggregates

If `total / count` previously never had zero denominators, TRY_DIVIDE is a safe drop-in. But if zeros existed and downstream code relied on the resulting NULL behavior, verify aggregate results remain correct.

### Date Parsing Affects PHI Timelines

Healthcare dates (DOB, admission, discharge) must parse correctly. After migration, validate all date columns:

```sql
SELECT COUNT(*) AS unparseable_dates
FROM patients
WHERE date_of_birth_str IS NOT NULL
  AND try_to_date(date_of_birth_str, 'yyyy-MM-dd') IS NULL;
```

### Post-Migration Validation

After applying ANSI fixes to any pipeline, run these checks:

1. **Row counts** -- Confirm output row counts match pre-migration baseline
2. **NULL counts** -- Compare NULL counts per column before/after (TRY_CAST introduces new NULLs)
3. **Aggregate sums** -- Verify SUM/AVG/COUNT aggregates match within tolerance
4. **Date ranges** -- Confirm min/max dates have not shifted
5. **Referential integrity** -- Verify join keys still match across tables
