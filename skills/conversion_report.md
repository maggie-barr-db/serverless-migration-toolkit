# Conversion Report

This skill provides the complete framework for generating a detailed report comparing an original pipeline against a converted pipeline. It documents every code change, explains why each change was made, and assesses behavioral risk. All templates and format specifications are self-contained.

## Purpose

When converting pipelines (Scala to PySpark, DBR 13.3 to 16.4, classic to serverless, or any combination), stakeholders need to understand:
- What exactly changed in the code
- Why each change was made
- Whether the change affects behavior or is purely syntactic
- What risk each change carries

This report serves as an audit trail and review document before the converted pipeline goes to production.

---

## Report Structure

The report should be a notebook (or markdown document) with the sections below.

### 1. Executive Summary

A high-level overview of the entire conversion.

```
Pipeline Conversion Report
==========================
Source: [language] on DBR [version] ([N] notebooks)
Target: [language] on DBR [version] ([N] notebooks)
Compute: [classic/serverless]

Total changes: [N]
  - Syntax/language translation:  [N] ([%]) -- no behavioral risk
  - API differences:              [N] ([%]) -- low risk, equivalent APIs
  - ANSI compliance fixes:        [N] ([%]) -- medium risk, changed error handling
  - Runtime config changes:       [N] ([%]) -- low risk, config removals/updates
  - UDF rewrites:                 [N] ([%]) -- high risk, logic translation
  - Serverless adaptations:       [N] ([%]) -- low-medium risk, compute changes
```

### 2. Notebook-by-Notebook Diff

For each notebook, list every change grouped by category. Each change entry must include:

| Field | Description |
|-------|-------------|
| **Location** | Notebook name and cell/line number |
| **Category** | Syntax, API, ANSI, Runtime, UDF, Serverless, or Behavioral |
| **Original Code** | The original code snippet |
| **Converted Code** | The converted code snippet |
| **Reason** | Why this change was made |
| **Risk** | None, Low, Medium, or High |
| **Behavioral Impact** | Does this change the output? Yes/No/Possible |

#### Change Categories

**Syntax (Risk: None)**
Changes that are purely language translation with zero behavioral impact:
- `$"col"` to `F.col("col")`
- `val x = ...` to `x = ...`
- `s"..."` to `f"..."`
- `println` to `print`
- `// comment` to `# comment`
- `// COMMAND` to `# COMMAND`
- Scala imports to PySpark imports
- `.as("alias")` to `.alias("alias")`

**API (Risk: Low)**
Changes where the Scala and PySpark APIs are functionally equivalent but have different method names or signatures:
- `.as[CaseClass]` removed (typed Dataset to untyped DataFrame)
- `.transform(func)` to `func(df)` (direct function call)
- `.take(n).toSeq` to `.take(n)` (already returns list)
- `.collect().toMap` to dict comprehension
- `Seq(...)` to `[...]`

**ANSI Compliance (Risk: Medium)**
Changes made to handle ANSI mode defaults on newer DBR versions:
- `CAST()` to `TRY_CAST()`
- Division to `TRY_DIVIDE()` or null guard
- Array access to bounds check
- Map access to key existence check
- Integer arithmetic to type widening

These changes alter error handling behavior. The original code would silently return null or a coerced value; the new code explicitly handles the edge case. The output should be identical for valid data, but invalid data may be handled differently.

**Runtime (Risk: Low)**
Changes required by DBR version differences:
- Removed deprecated Spark configs
- Updated config defaults
- Removed legacy mode flags

**UDF (Risk: High)**
UDF conversions require the most scrutiny because they translate business logic between languages:
- Scala typed UDFs to Python UDFs with explicit return types
- Pattern matching inside UDFs to if/elif or dict lookups
- `Option`/`Try` handling to None checks / try-except
- Null handling differences (Scala `null` vs Python `None`)

Document each UDF conversion individually with before/after code and an explanation of how null values, edge cases, and error conditions are handled.

**Serverless (Risk: Low-Medium)**
Changes required by the move from classic to serverless compute:
- Job JSON transformation (cluster config to environment_key)
- `os.environ` to `dbutils.widgets.get()`
- Spark config removals (unsupported on serverless)
- Library replacements (JAR to PyPI/wheel)
- Unsupported operation removals (REFRESH TABLE, .persist(), etc.)

**Behavioral (Risk: High)**
Any change that could produce different output:
- Date/timestamp parsing logic changes
- Numeric precision changes (float vs double, rounding)
- Sort order changes (null ordering)
- Join behavior changes (null equality)

### 3. ANSI Compliance Audit

A dedicated section listing every ANSI-sensitive pattern found in the original code and how it was addressed.

```
ANSI Compliance Audit
=====================

| # | Notebook | Line | Pattern | Original | Fix Applied | Risk |
|---|----------|------|---------|----------|-------------|------|
| 1 | notebook_name | 45 | Division | total / count | TRY_DIVIDE(total, count) | Medium -- returns null instead of error for zero denominators |
| 2 | notebook_name | 72 | Cast | CAST(str AS INT) | TRY_CAST(str AS INT) | Medium -- returns null instead of error for non-numeric strings |
| 3 | notebook_name | 23 | Division | paid / billed * 100 | Added CASE WHEN billed = 0 | Medium -- explicit null for zero denominator |
| 4 | notebook_name | 91 | Array access | arr[idx] | TRY(arr[idx]) | Medium -- returns null instead of ArrayIndexOutOfBounds |
| 5 | notebook_name | 15 | Arithmetic | int_a * int_b | CAST(int_a AS BIGINT) * int_b | Low -- prevents ARITHMETIC_OVERFLOW |
```

### 4. UDF Conversion Detail

A dedicated section for every UDF with side-by-side comparison. Use this template for each:

```
UDF: [function_name]
====================
Purpose: [one-line description of what the UDF does]

Original ([source language]):
  [original code block]

Converted ([target language]):
  [converted code block]

Changes:
  - [change 1]
  - [change 2]
  - [change 3]

Null handling:
  - Original: [how nulls are handled]
  - Converted: [how nulls are handled]
  - Empty string: [how empty strings are handled]

Edge cases:
  - [edge case 1 and how it differs]
  - [edge case 2 and how it differs]

Risk: [None/Low/Medium/High] -- [justification]
Validation: Verify with conversion_validator Check 12 (UDF Output Consistency)
```

### 5. Date/Timestamp Conversion Detail

A dedicated section for every date operation with analysis of potential timezone, format, and precision differences. Use this template for each:

```
Date Operation: [description]
=============================
Original ([source language]):
  [original code -- e.g., UDF using SimpleDateFormat or strptime]

Converted ([target language]):
  [converted code]

Differences:
  - [timezone handling difference]
  - [format parsing difference]
  - [null/error handling difference]

Edge cases:
  - Invalid leap year dates (e.g., "02/29/2023"): [original behavior] vs [converted behavior]
  - Invalid months (e.g., "13/01/2024"): [original behavior] vs [converted behavior]
  - Epoch zero dates: [original behavior] vs [converted behavior]

Risk: [None/Low/Medium/High] -- [justification]
Validation: Verify with conversion_validator Check 7 (Date/Timestamp Deep Validation)
```

### 6. Risk Summary Table

A final table ranking all changes by risk level.

```
High Risk Changes (require manual review and validator confirmation):
  1. [UDF name] -- [brief description of conversion]
  2. [UDF name] -- [brief description of conversion]
  3. [Date operation] -- [brief description of change]
  4. [MERGE/complex SQL] -- [brief description of change]

Medium Risk Changes (verify with conversion_validator):
  5. TRY_CAST replacements ([N] occurrences)
  6. TRY_DIVIDE replacements ([N] occurrences)
  7. [UDF name] null handling change

Low Risk / No Risk Changes ([N] syntax + [N] API = [N] total):
  - Language syntax translations
  - Import changes
  - API equivalents
  - Config removals (transparent on serverless)
```

### 7. Serverless-Specific Change Section

When the migration includes a move to serverless compute, add this section.

#### 7a. Job JSON Transformation

Document the before/after job JSON changes:
- `job_clusters` section removed
- `job_cluster_key` removed from each task
- `environment_key` added to each task
- `environments` block added with client version and dependencies path
- `parameters` block added with environment variable substitution
- `queue` and `performance_optimized` settings added

```
Job JSON Changes
================
Before:
  - job_clusters: [N] cluster definitions
  - Each task: job_cluster_key = "cluster_name"

After:
  - job_clusters: REMOVED
  - Each task: environment_key = "env_name"
  - environments block: client="4", dependencies=["requirements.txt path"]
  - parameters: [list of parameters with %env_name% substitution]
```

#### 7b. Environment Variable Migration

For each `os.environ.get()` or `os.environ[]` found in notebook code:

```
| Original | Converted | Job Parameter |
|---|---|---|
| os.environ.get("VAR_NAME") | dbutils.widgets.get("VAR_NAME") | VAR_NAME in parameters block |
```

#### 7c. Spark Config Removals

For each Spark config removed or changed:

```
| Config | Original Value | Action | Reason | Behavioral Impact |
|---|---|---|---|---|
| spark.config.name | value | Removed | Unsupported on serverless | None -- default behavior matches |
```

#### 7d. Unsupported Operation Removals

For each unsupported operation removed:

```
| Original Code | Replacement | Reason | Behavioral Impact |
|---|---|---|---|
| REFRESH TABLE schema.table | Removed | Not needed with Unity Catalog | None |
| df.persist() | Removed | Not supported on serverless | Possible perf change, no data change |
| MSCK REPAIR TABLE | Removed | Legacy Hive operation | None with Delta tables |
```

### 8. Library Replacement Section

Document every library that was replaced during migration.

```
Library Replacement: [library name]
====================================
Original: [original usage code]
Replaced with: [replacement approach]

Code change:
  - Cell [N]: [description of change]
  - Cell [N]: [description of change]

Reason: [why the change was needed -- e.g., JAR not supported on serverless]
Risk: [None/Low/Medium/High] -- [justification and testing notes]
```

For each replacement, include:
- Original library and usage pattern
- Replacement approach with code
- Why the change was needed
- Risk level and testing notes

### 9. Repo-Side Change Manifest

Document all changes that were made in the source control repository (not in Databricks directly):

```
REPO-SIDE CHANGES
=================
Repository: [repo name]
Branch: [branch name]

Files Changed:
  1. [file path]
     - [description of change]
     - [description of change]

  2. [file path]
     - [description of change]

Variable Group / Secret Changes:
  - [description or "No changes needed"]

Deployment Verification:
  - [ ] Deployed to UAT via pipeline
  - [ ] Job ID preserved (not recreated)
  - [ ] Volume path resolves correctly
  - [ ] Requirements.txt picked up by serverless
```

---

## How to Generate the Report

### Approach 1: Read Both Pipelines

1. Read every notebook in the original pipeline
2. Read every notebook in the converted pipeline
3. Compare cell-by-cell, identifying and categorizing each change
4. Generate the report sections above

### Approach 2: Generate During Conversion

If you are the one performing the conversion (using the `scala_to_pyspark` and `dbr_upgrade` skills), log each change as you make it. This produces a more accurate report because you know the intent behind each change.

### Output Format

Generate the report as a Databricks notebook with markdown cells for readability. It should be committed alongside the converted pipeline so reviewers can read it before approving the migration.

---

## Report Checklist

Before finalizing the report, verify every item:

- [ ] Every notebook in the pipeline is covered in Section 2
- [ ] Every UDF has a dedicated detail section (Section 4)
- [ ] Every ANSI fix is listed in the compliance audit (Section 3)
- [ ] Every date/timestamp operation is analyzed (Section 5)
- [ ] Risk levels are assigned to all changes (Section 6)
- [ ] The executive summary totals match the detailed change count (Section 1 vs Section 2)
- [ ] High-risk changes have mitigation steps or validation references
- [ ] (If serverless) Job JSON transformation documented (Section 7a)
- [ ] (If serverless) Environment variable migration documented (Section 7b)
- [ ] (If serverless) Spark config removals documented (Section 7c)
- [ ] (If serverless) Unsupported operations documented (Section 7d)
- [ ] (If serverless) Library replacements documented (Section 8)
- [ ] (If serverless) Repo-side change manifest included (Section 9)
