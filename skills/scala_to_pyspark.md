# Scala to PySpark Conversion

This skill teaches you how to convert Databricks Scala notebooks to PySpark. It covers syntax translation, API differences, and patterns that require restructuring rather than simple find-and-replace.

## Conversion Approach

1. Read the entire Scala notebook before making changes — understand the data flow
2. Convert imports first, then schema/type definitions, then business logic
3. Preserve the notebook's cell structure (each `// COMMAND ----------` becomes a new cell)
4. Keep all comments, markdown cells, and `dbutils` calls
5. Do NOT change SQL strings — they work identically in both languages
6. Do NOT optimize or refactor — produce a faithful translation that preserves the original logic

## Import Translations

| Scala | PySpark |
|-------|---------|
| `import org.apache.spark.sql.functions._` | `from pyspark.sql import functions as F` |
| `import org.apache.spark.sql.types._` | `from pyspark.sql.types import *` |
| `import org.apache.spark.sql.{DataFrame, Row, SparkSession}` | `from pyspark.sql import DataFrame, Row, SparkSession` |
| `import org.apache.spark.sql.expressions.{Window, UserDefinedFunction}` | `from pyspark.sql.window import Window` |
| `import scala.util.{Try, Success, Failure}` | *(no equivalent — use try/except inline)* |

**Key rule:** In PySpark, always prefix functions with `F.` (e.g., `F.col()`, `F.when()`, `F.lit()`). Never use `from pyspark.sql.functions import *` — it pollutes the namespace and shadows Python builtins like `sum` and `max`.

## Column Reference Syntax

| Scala | PySpark | Notes |
|-------|---------|-------|
| `$"column_name"` | `F.col("column_name")` | Most common conversion |
| `col("column_name")` | `F.col("column_name")` | Add `F.` prefix |
| `$"col".as("alias")` | `F.col("col").alias("alias")` | `.as()` → `.alias()` |
| `$"col".cast(IntegerType)` | `F.col("col").cast("int")` | Can use string types in PySpark |
| `$"a" === $"b"` | `F.col("a") == F.col("b")` | Triple equals → double equals |
| `$"a" =!= $"b"` | `F.col("a") != F.col("b")` | |
| `$"a" && $"b"` | `(F.col("a")) & (F.col("b"))` | Must use `&` with parentheses |
| `$"a" \|\| $"b"` | `(F.col("a")) \| (F.col("b"))` | Must use `\|` with parentheses |

## Variable Declarations

| Scala | PySpark |
|-------|---------|
| `val x = 5` | `x = 5` |
| `var x = 5` | `x = 5` |
| `val x: String = "hello"` | `x: str = "hello"` *(type hint optional)* |
| `val x: DataFrame = spark.table(...)` | `x = spark.table(...)` |

**Rule:** Drop all type annotations unless you want to add Python type hints for readability. Never required.

## String Interpolation

| Scala | PySpark |
|-------|---------|
| `s"Hello ${name}"` | `f"Hello {name}"` |
| `s"Table: ${catalog}.${schema}.${table}"` | `f"Table: {catalog}.{schema}.{table}"` |
| `f"${value}%10.2f"` | `f"{value:10.2f}"` |

## Case Classes → No Direct Equivalent

Scala case classes used for typed Datasets have no PySpark equivalent. Convert based on usage:

**If used with `.as[CaseClass]` for typed Dataset:**
- Remove the case class entirely
- Remove the `.as[CaseClass]` call — just use DataFrame operations
- The typed Dataset pattern doesn't exist in PySpark

**If used for schema definition:**
- Convert to a `StructType`:
```python
# Scala:
# case class Record(record_id: String, amount: Double)
# val df = spark.read.as[Record]

# PySpark:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
record_schema = StructType([
    StructField("record_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
])
df = spark.read.schema(record_schema).csv(...)
```

**If used as a data container in non-Spark code:**
- Convert to a Python `dataclass` or `namedtuple`

## StructType Schema Definition

| Scala | PySpark |
|-------|---------|
| `StructType(Seq(StructField(...)))` | `StructType([StructField(...)])` |
| `StructField("name", StringType, nullable = true)` | `StructField("name", StringType(), nullable=True)` |

**Key difference:** In PySpark, types need parentheses: `StringType()` not `StringType`.

## Pattern Matching → if/elif or Dictionary

**Simple value matching:**
```python
# Scala:
# x match {
#   case "A" => "Alpha"
#   case "B" => "Beta"
#   case _   => "Unknown"
# }

# PySpark option 1 — if/elif:
if x == "A":
    result = "Alpha"
elif x == "B":
    result = "Beta"
else:
    result = "Unknown"

# PySpark option 2 — dictionary (preferred for simple mappings):
mapping = {"A": "Alpha", "B": "Beta"}
result = mapping.get(x, "Unknown")
```

**Pattern matching inside a function used as a UDF:**
- Convert to if/elif inside the Python function
- Or use a dictionary lookup

**Nested pattern matching with Option:**
```python
# Scala:
# Option(code).filter(_.nonEmpty) match {
#   case Some(c) => c.head.toString match { ... }
#   case None => "Unknown"
# }

# PySpark:
if code and len(code.strip()) > 0:
    first_char = code[0]
    # ... mapping logic
else:
    result = "Unknown"
```

## Option / Some / None → Python None Handling

| Scala | PySpark |
|-------|---------|
| `Option(x)` | `x` *(Python handles None natively)* |
| `Some(value)` | `value` |
| `None` (Scala) | `None` (Python) |
| `.getOrElse(default)` | `x if x is not None else default` |
| `.map(f)` | `f(x) if x is not None else None` |
| `.filter(pred)` | `x if (x is not None and pred(x)) else None` |
| `.flatMap(f)` | `f(x) if x is not None else None` |
| `option.isDefined` | `x is not None` |
| `option.isEmpty` | `x is None` |

**In UDFs:** Scala UDFs that use `Option` should be converted to Python functions that check `if x is None` explicitly.

## Try / Success / Failure → try/except

```python
# Scala:
# Try {
#   riskyOperation()
# } match {
#   case Success(result) => Some(result)
#   case Failure(_) => None
# }

# PySpark:
try:
    result = risky_operation()
except Exception:
    result = None
```

## UDF Conversion

**Simple typed UDF:**
```python
# Scala:
# val myUdf: UserDefinedFunction = udf((s: String) => s.toUpperCase)

# PySpark option 1 — decorator:
@F.udf(returnType=StringType())
def my_udf(s):
    return s.upper() if s else None

# PySpark option 2 — inline:
my_udf = F.udf(lambda s: s.upper() if s else None, StringType())
```

**Multi-parameter UDF:**
```python
# Scala:
# val myUdf = udf((a: String, b: Double, c: String) => { ... })

# PySpark:
@F.udf(returnType=DoubleType())
def my_udf(a, b, c):
    # ... logic
    return result
```

**UDF returning a struct:**
```python
# Scala:
# case class Result(code: String, score: Double)
# val myUdf = udf((x: String) => Result(x, 1.0))

# PySpark:
result_schema = StructType([
    StructField("code", StringType()),
    StructField("score", DoubleType()),
])

@F.udf(returnType=result_schema)
def my_udf(x):
    return Row(code=x, score=1.0)
```

**SQL-registered UDF:**
```python
# Scala:
# spark.udf.register("my_func", (s: String) => s.toUpperCase)

# PySpark:
spark.udf.register("my_func", lambda s: s.upper() if s else None, StringType())
```

**Key rules:**
- Always specify `returnType` in PySpark UDFs
- Always handle `None` inputs — PySpark passes `None` for null values, Scala passes `null`
- Prefer `@F.udf` decorator over `F.udf()` wrapper for readability

## DataFrame API Differences

Most DataFrame operations are identical. Key differences:

| Operation | Scala | PySpark |
|-----------|-------|---------|
| Functions prefix | `col()`, `when()`, `lit()` | `F.col()`, `F.when()`, `F.lit()` |
| Column alias | `.as("name")` | `.alias("name")` |
| `.transform()` | `df.transform(myFunc)` | `my_func(df)` *(just call the function directly — `.transform()` exists in PySpark 3.x but direct call is more Pythonic)* |
| `.foreach` | `df.foreach(row => ...)` | `for row in df.collect(): ...` |
| `.take(n).toSeq` | `df.take(n)` | Already returns a list |
| `.collect().toMap` | — | `{row[0]: row[1] for row in df.collect()}` |
| Chained `.withColumn` | Same syntax | Same syntax |

## Scala Collections → Python

| Scala | PySpark |
|-------|---------|
| `Seq(1, 2, 3)` | `[1, 2, 3]` |
| `Map("a" -> 1, "b" -> 2)` | `{"a": 1, "b": 2}` |
| `Array(1, 2, 3)` | `[1, 2, 3]` |
| `seq.map(f)` | `[f(x) for x in seq]` |
| `seq.filter(pred)` | `[x for x in seq if pred(x)]` |
| `seq.foreach(f)` | `for x in seq: f(x)` |
| `seq.find(pred).map(f).getOrElse(default)` | `next((f(x) for x in seq if pred(x)), default)` |
| `seq.zipWithIndex` | `enumerate(seq)` |
| `(a, b)._1` / `._2` | `a` / `b` or `tup[0]` / `tup[1]` |
| `map.getOrElse(key, default)` | `d.get(key, default)` |

## Lambda Syntax

| Scala | PySpark |
|-------|---------|
| `x => x + 1` | `lambda x: x + 1` |
| `_ + 1` | `lambda x: x + 1` |
| `(a, b) => a + b` | `lambda a, b: a + b` |
| `_.toUpperCase` | `lambda x: x.upper()` |

## Notebook Patterns

| Pattern | Scala | PySpark |
|---------|-------|---------|
| `%run ./other_notebook` | Same | Same — no change needed |
| `dbutils.notebook.run(path, timeout, params)` | Same | Same — no change needed |
| `dbutils.widgets.text(name, default)` | Same | Same — no change needed |
| `dbutils.notebook.exit(message)` | Same | Same — no change needed |
| Language declaration | `// Databricks notebook source` | `# Databricks notebook source` |
| Magic commands | `// MAGIC %md` | `# MAGIC %md` |
| Cell separator | `// COMMAND ----------` | `# COMMAND ----------` |

## Window Functions

Window functions are nearly identical. Only the import and column reference syntax changes:

```python
# Scala:
# val w = Window.partitionBy($"group_id").orderBy($"event_date")
# df.withColumn("rank", row_number().over(w))

# PySpark:
w = Window.partitionBy("group_id").orderBy("event_date")
df = df.withColumn("rank", F.row_number().over(w))
```

Note: In PySpark, `Window.partitionBy` accepts strings directly — no need for `F.col()`.

## Print and Logging

| Scala | PySpark |
|-------|---------|
| `println(s"Count: ${count}")` | `print(f"Count: {count}")` |
| `println(f"${name}%-20s: ${value}%,d")` | `print(f"{name:<20s}: {value:,d}")` |

## Scala Syntactic Sugar That Doesn't Translate Literally

Scala has several syntactic shortcuts where `object(arg)` is sugar for `object.apply(arg)`. In Python, `object(arg)` is always a function call. This mismatch causes **runtime errors** (not compile-time), making them easy to miss during conversion. Always watch for any Scala pattern where parentheses are used to access/index into something.

### Array Element Access

Scala uses `(index)` to access array elements. Python interprets `(index)` as a function call on the Column object, which throws `TypeError: 'Column' object is not callable`.

| Scala | Naïve PySpark (FAILS) | Correct PySpark |
|-------|----------------------|-----------------|
| `split($"col", "\\.")(1)` | `F.split(F.col("col"), "\\.")[1]` ← works | `F.split(F.col("col"), "\\.").getItem(1)` ← also works |
| `split($"col", "\\.")(1)` | `F.split(F.col("col"), "\\.")(1)` ← **FAILS** | Use `[1]` or `.getItem(1)` |
| `arrayCol(0)` | `array_col(0)` ← **FAILS** | `array_col[0]` or `array_col.getItem(0)` |

**Rule:** Any time you see `column_expression(integer)` in Scala, convert to `column_expression[integer]` or `column_expression.getItem(integer)` in PySpark. Never use `(integer)` on a Column.

### Map Column Lookup

Scala's `typedLit(Map(...))` creates a map-typed column that supports `.apply(key)` or `(key)` for lookup. PySpark's `F.create_map()` creates a map column, but there is **no `()` or `[]` shorthand** that accepts another Column as a key. You must use `F.element_at()`.

| Scala | Naïve PySpark (FAILS) | Correct PySpark |
|-------|----------------------|-----------------|
| `typedLit(Map("a"->1, "b"->2))($"key_col")` | `F.create_map(...)[ F.col("key_col")]` ← **FAILS** | `F.element_at(F.create_map(...), F.col("key_col"))` |
| `mapCol($"key")` | `map_col(F.col("key"))` ← **FAILS** | `F.element_at(map_col, F.col("key"))` |
| `mapCol.apply($"key")` | — | `F.element_at(map_col, F.col("key"))` |

**Rule:** Any map column lookup in Scala using `mapCol(keyExpr)` or `mapCol.apply(keyExpr)` must become `F.element_at(map_col, key_expr)` in PySpark. There is no shorthand.

### Struct Field Access

This one usually translates correctly, but be aware:

| Scala | PySpark |
|-------|---------|
| `$"struct_col.field_name"` | `F.col("struct_col.field_name")` |
| `$"struct_col".getField("field_name")` | `F.col("struct_col").getField("field_name")` |

## Numeric Precision and Rounding

Scala and Python have different default rounding behaviors that can cause subtle data differences.

### BigDecimal Rounding

| Scala | Python | Difference |
|-------|--------|------------|
| `BigDecimal(3.315).setScale(2, RoundingMode.HALF_UP)` → `3.32` | `round(3.315, 2)` → `3.31` | Python uses **banker's rounding** (round half to even) by default |

**Fix:** For faithful translation of Scala's HALF_UP rounding in Python UDFs:
```python
from decimal import Decimal, ROUND_HALF_UP

# Instead of: round(x, 2)
# Use:
float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
```

### Float vs Double Precision

Spark operations generally produce the same results regardless of language, but UDFs that do math in Scala (Double) vs Python (float) can have floating-point ordering differences. For aggregates, allow a small tolerance (1e-6) when comparing outputs.

### Integer Division

| Scala | Python | Difference |
|-------|--------|------------|
| `5 / 2` → `2` (integer division) | `5 / 2` → `2.5` (float division) | Python `/` is float division |
| `5 / 2` → `2` | `5 // 2` → `2` | Use `//` for integer division in Python |

**Rule:** In UDFs, if Scala code divides two integers and expects an integer result, use `//` in Python, not `/`.

## Runtime Error Patterns

Beyond the syntactic sugar issues above, these patterns cause crashes that are easy to miss during conversion.

### Boolean Operator Precedence on Columns

Scala's `&&` and `||` have lower precedence than comparison operators. Python's `&` and `|` have **higher** precedence than `>`, `<`, `==`. This silently changes the logic or crashes.

```python
# Scala — works correctly:
# df.filter($"a" > 0 && $"b" < 100)

# Naïve PySpark — WRONG RESULT or error:
df.filter(F.col("a") > 0 & F.col("b") < 100)
# Python evaluates as: a > (0 & b) < 100  — NOT what you want

# Correct PySpark — always parenthesize:
df.filter((F.col("a") > 0) & (F.col("b") < 100))
```

**Rule:** Every `&&` → `&` and `||` → `|` conversion MUST add parentheses around both operands. This applies to `.filter()`, `.when()`, and any Column boolean expression.

### Python Reserved Words as Variable Names

These are valid Scala identifiers but Python reserved words or builtins. They cause `SyntaxError` or shadow builtins:

| Scala Variable | Python Conflict | Fix |
|---------------|-----------------|-----|
| `val type = ...` | `type` is a builtin | Rename to `type_name`, `col_type`, etc. |
| `val class = ...` | `class` is reserved | Rename to `cls`, `class_name` |
| `val import = ...` | `import` is reserved | Rename to `import_path` |
| `val from = ...` | `from` is reserved | Rename to `from_col`, `source` |
| `val in = ...` | `in` is reserved | Rename to `in_val`, `input_val` |
| `val is = ...` | `is` is reserved | Rename to `is_flag`, `is_valid` |
| `val not = ...` | `not` is reserved | Rename to `not_flag` |
| `val lambda = ...` | `lambda` is reserved | Rename to `lambda_val` |
| `val yield = ...` | `yield` is reserved | Rename to `yield_val` |
| `val list = ...` | `list` is a builtin | Rename to `items`, `values` |
| `val map = ...` | `map` is a builtin | Rename to `mapping`, `lookup` |
| `val filter = ...` | `filter` is a builtin | Rename to `filter_expr` |
| `val format = ...` | `format` is a builtin | Rename to `fmt`, `format_str` |

**Rule:** Scan all `val` declarations and function parameter names for Python reserved words and builtins. Rename before converting.

### Column.toString Confusion

```scala
// Scala — gets the string value from a Row:
val name: String = row.getString(0)

// Common mistake — calling str() on a Column object:
name = str(some_column)  # Returns "Column<'name'>" — the Column repr, not data
```

**Rule:** `str()` on a PySpark Column returns the debug representation, not data. To get values, use `.collect()`, `.first()`, or row accessors.

## Silent Data Difference Patterns

These are the most dangerous: the converted code runs successfully but produces different data. The validation step will catch them, but knowing what to look for helps fix them faster.

### Null Propagation in UDFs

The most common source of silent data differences. Scala UDFs receive `null`, Python UDFs receive `None`. The conversion must handle both consistently.

```python
# Scala:
# Option(s).map(_.toUpperCase).getOrElse(null)  → returns null for null input

# WRONG Python — returns empty string instead of None:
s.upper() if s else ""  # "" != None — changes downstream null counts and joins

# CORRECT Python — preserve null:
s.upper() if s is not None else None
```

**Critical distinctions:**
| Scala Pattern | Common Wrong Python | Correct Python | Why It Matters |
|--------------|-------------------|----------------|----------------|
| `.getOrElse(null)` | `x if x else ""` | `x if x is not None else None` | `""` is not null — changes null counts |
| `.getOrElse("")` | `x if x else ""` | `x if x is not None else ""` | OK — but `if x` is falsy for `""` and `0` too |
| `Option(s).filter(_.nonEmpty)` | `if s:` | `if s is not None and len(s.strip()) > 0:` | `if s:` fails for empty string `""` — returns falsy |
| `null` return from UDF | `return None` | `return None` | Correct — but verify every code path |

**Rule:** In every UDF conversion, trace every code path and verify that `null` in Scala maps to `None` in Python, not `""`, `0`, `False`, or any other falsy value. Use `is not None` checks, never bare `if x:` for null testing.

### Regex Escaping Differences

Scala raw strings and Python raw strings have different escaping:

```python
# Scala:
# regexp_replace($"col", "\\s+", " ")  — double-escaped in regular string
# regexp_replace($"col", """\s+""", " ")  — raw string, no escaping needed

# PySpark — Spark SQL functions use Java regex, so escaping is the same:
F.regexp_replace(F.col("col"), "\\s+", " ")  # Works — same as Scala

# BUT in a Python UDF using re module:
re.sub(r"\s+", " ", s)     # Use raw string r"..." — single backslash
re.sub("\\s+", " ", s)     # Also works — double-escaped
re.sub("""\s+""", " ", s)  # Python triple-quote is NOT the same as Scala raw string
```

**Rule:** For `regexp_replace`, `rlike`, and other Spark SQL regex functions — keep the same escaping as Scala. For Python `re` module in UDFs — use `r"..."` raw strings and single-escape.

### Empty Collection Behavior in UDFs

```python
# Scala:
# seq.head  → throws NoSuchElementException on empty
# seq.headOption.getOrElse("default")  → returns "default"

# Python:
lst[0]  # throws IndexError on empty — different exception type
next(iter(lst), "default")  # returns "default"
```

**The risk:** If Scala code has a `try { seq.head } catch { case _: NoSuchElementException => default }`, the Python equivalent `try: lst[0] except IndexError:` works, but if the catch is broader (`case _: Exception`), it catches different things in each language.

**Rule:** Convert `.head` → `lst[0]`, `.headOption` → `lst[0] if lst else None`. Convert `.tail` → `lst[1:]`. Always check if the surrounding error handling catches the right exception type.

### Integer Overflow in UDFs

```python
# Scala Int is 32-bit signed, overflows silently:
# val x: Int = 2147483647 + 1  → -2147483648 (wraps)

# Python int is arbitrary precision:
x = 2147483647 + 1  # → 2147483648 (correct, but different from Scala)
```

**Rule:** If a Scala UDF does integer arithmetic that could overflow, and the pipeline relies on the wrapped value, the Python version will produce a different (mathematically correct) result. This is rare but can happen in hash calculations or bitwise operations. If faithfulness to Scala output is required, add explicit 32-bit wrapping: `result = (result + 2**31) % 2**32 - 2**31`.

### Date Parsing Edge Cases in UDFs

```python
# Scala SimpleDateFormat:
# - setLenient(true) [default]: "02/29/2023" → March 1, 2023 (rolls forward)
# - setLenient(false): "02/29/2023" → throws ParseException

# Python datetime.strptime:
# - "02/29/2023" → ALWAYS raises ValueError (no lenient mode)
```

**Other edge cases:**
| Input | Scala SimpleDateFormat (lenient) | Python strptime |
|-------|--------------------------------|-----------------|
| `"02/29/2023"` (invalid leap year) | Rolls to `03/01/2023` | Raises `ValueError` |
| `"13/01/2024"` (invalid month) | Rolls to `01/01/2025` | Raises `ValueError` |
| `"00/15/2024"` (zero month) | Implementation-dependent | Raises `ValueError` |
| `"2/9/2024"` (no zero-padding) | Parses as Feb 9 | Parses as Feb 9 (both `%m` and `%-m` work) |

**Rule:** If the Scala code uses `setLenient(true)` (the default), invalid dates silently roll forward. Python will raise exceptions. The conversion must add explicit error handling for invalid dates in the Python UDF. If the Scala code uses `setLenient(false)`, behavior is closer but exception types differ.

### Thread Safety (SimpleDateFormat)

```scala
// Scala — SimpleDateFormat is NOT thread-safe:
// Using a shared instance across Spark executors can cause intermittent wrong results
val sdf = new SimpleDateFormat("MM/dd/yyyy")  // shared across partitions = bug

// Python — datetime.strptime IS thread-safe
```

This isn't a conversion issue per se, but it means the Scala baseline output may itself have intermittent corruption if `SimpleDateFormat` was used unsafely. The Python version may actually be more correct.

## Non-Determinism Warnings

These patterns may cause validation to fail intermittently even when the conversion is correct.

### Sort Order and Null Positioning

Spark's default null ordering depends on sort direction:
- `asc`: nulls first (in most Spark versions)
- `desc`: nulls last

This is the same in both Scala and PySpark, but if a pipeline uses `.head` or `.first()` on an unsorted DataFrame, the returned row may differ between runs and between languages.

**Rule:** If validation shows different rows for "first" or "top-N" queries, check if the source DataFrame has a deterministic sort. Add `.orderBy()` before `.first()` or `.head` if needed.

### Distinct and DropDuplicates Row Selection

`.distinct()` and `.dropDuplicates()` are non-deterministic about which duplicate row they keep. If duplicates exist and downstream logic depends on other columns from the kept row, results may differ.

**Rule:** If validation shows differences after `.dropDuplicates()`, check if the pipeline needs a deterministic dedup (e.g., `row_number().over(Window.orderBy(...))` followed by filter).

### HashMap Iteration Order

```python
# Scala — HashMap iteration order is non-deterministic:
# Map("a" -> 1, "b" -> 2).foreach(...)  — order varies between runs

# Python 3.7+ — dict preserves insertion order:
{"a": 1, "b": 2}  — always iterates in insertion order
```

This rarely matters but can affect UDFs that iterate over maps and build strings or concatenate results.

### Floating-Point Aggregation Order

`SUM()` and `AVG()` on floating-point columns can produce slightly different results depending on partition count and aggregation order (floating-point addition is not associative). This is the same in both languages but can cause validation to report tiny differences (< 1e-10) that aren't real bugs.

**Rule:** For numeric comparison in validation, use tolerance-based checks (1e-6 for most cases, 1e-10 for high-precision requirements).

## Things That Stay the Same

- All Spark SQL strings (`spark.sql("SELECT ...")`)
- Delta operations (`MERGE INTO`, `CREATE TABLE`, etc.)
- `spark.conf.set` / `spark.conf.get`
- `spark.read` / `spark.write` API (same method names)
- `.filter()`, `.select()`, `.groupBy()`, `.agg()`, `.join()`, `.orderBy()`
- `.cache()`, `.unpersist()`, `.count()`
- `when().otherwise()` logic (just add `F.` prefix)
- `createOrReplaceTempView()`

## Conversion Checklist

After converting a notebook, verify:

**Column references:**
- [ ] All `$"col"` references converted to `F.col("col")`
- [ ] All `.as("alias")` converted to `.alias("alias")`
- [ ] No bare `col()`, `when()`, `lit()` — all prefixed with `F.`

**Array and Map access (common runtime error source):**
- [ ] All array access `column(index)` converted to `column[index]` or `.getItem(index)` — never `column(index)`
- [ ] All `split(...)(n)` converted to `F.split(...).getItem(n)` or `F.split(...)[n]`
- [ ] All `typedLit(Map(...))` converted to `F.create_map(...)` with `F.element_at()` for lookups
- [ ] All map lookups `mapCol(keyExpr)` or `mapCol.apply(keyExpr)` converted to `F.element_at(map_col, key_expr)`
- [ ] No `Column(...)` calls that are actually meant to be indexing — search for `)(` patterns after split/array operations

**Type system:**
- [ ] All case classes removed or converted to StructType
- [ ] All `.as[CaseClass]` calls removed
- [ ] All UDFs have explicit return types
- [ ] All UDFs handle None inputs

**Numeric precision:**
- [ ] Any `BigDecimal.setScale(n, HALF_UP)` converted to `Decimal(str(x)).quantize(..., ROUND_HALF_UP)` — not `round()`
- [ ] Any integer division in UDFs uses `//` not `/`
- [ ] Floating-point comparisons in tests use tolerance (1e-6)

**Boolean operators:**
- [ ] All `&&` → `&` and `||` → `|` have parentheses around BOTH operands
- [ ] All `!` (not) → `~` with parentheses

**Variable names:**
- [ ] No Python reserved words or builtins used as variable names (`type`, `class`, `from`, `in`, `is`, `list`, `map`, `filter`, `format`)

**Null handling in UDFs:**
- [ ] Every UDF code path returns `None` for null inputs — never `""`, `0`, or `False`
- [ ] Null checks use `is not None`, not bare `if x:` (which is falsy for `""`, `0`, `False`)
- [ ] `.getOrElse(null)` → `if x is not None else None`, NOT `if x else ""`

**Date parsing in UDFs:**
- [ ] Scala `SimpleDateFormat` lenient mode behavior matched (invalid dates roll forward vs raise)
- [ ] Python `strptime` wrapped in try/except for invalid dates
- [ ] Timezone handling explicit if Scala used `SimpleDateFormat` (JVM default TZ)

**Regex:**
- [ ] Spark SQL regex functions (`regexp_replace`, `rlike`) — keep same escaping as Scala
- [ ] Python `re` module in UDFs — use `r"..."` raw strings

**Collections:**
- [ ] `.head` → `lst[0]`, `.headOption` → `lst[0] if lst else None`
- [ ] `.tail` → `lst[1:]`
- [ ] Error handling around collection access catches correct Python exception types

**Language patterns:**
- [ ] All pattern matching converted to if/elif or dict
- [ ] All Option/Try handling converted to None checks / try-except
- [ ] All imports updated
- [ ] All `println` → `print`, string interpolation `s"..."` → `f"..."`
- [ ] All `// COMMAND` → `# COMMAND`, `// MAGIC` → `# MAGIC`
- [ ] Scala comment style `//` converted to Python `#`
