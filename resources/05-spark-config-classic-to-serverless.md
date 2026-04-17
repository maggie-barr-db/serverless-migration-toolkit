# Spark Configuration: Classic Compute → Serverless

This reference maps every Spark configuration category from classic compute (DBR 13.3 LTS) to serverless general compute (environment version 4). Use this to audit notebooks and cluster configs during migration.

## How to Use This Document

1. **Scan all notebooks** for `spark.conf.set`, `spark.conf.get`, `SET`, and `RESET` patterns
2. **Scan cluster configs** for Spark Config entries
3. **Scan init scripts** for any spark.conf settings
4. For each config found, look it up in the tables below
5. Apply the recommended action

### Detection Patterns

```
# Notebook spark.conf.set calls
spark\.conf\.set\s*\(
spark\.conf\.get\s*\(

# SQL SET statements
(?i)^\s*SET\s+spark\.
(?i)^\s*SET\s+\"spark\.

# SQL RESET statements
(?i)^\s*RESET\s+spark\.

# SparkContext config (Scala)
sc\.getConf\.set\s*\(
spark\.sparkContext\.getConf

# Init script Spark config
spark\.driver\.extraJavaOptions
spark\.executor\.extraJavaOptions
SPARK_DAEMON_JAVA_OPTS
```

---

## Supported Configs on Serverless

These configs CAN be set in notebook code on serverless compute. They may have different defaults than classic compute.

### SQL and Query Execution

| Config | Classic 13.3 Default | Serverless Default | Notes |
|--------|---------------------|-------------------|-------|
| `spark.sql.ansi.enabled` | `false` | `true` (mandatory) | Cannot be set to false on serverless. Fix code instead. |
| `spark.sql.shuffle.partitions` | `200` | Auto-tuned | Can override but serverless auto-tunes. Usually remove. |
| `spark.sql.sources.default` | `parquet` | `delta` | Only affects read/write without explicit format. |
| `spark.sql.adaptive.enabled` | `true` | `true` | AQE is always on and more aggressive on serverless. |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Auto-coalescing is managed. |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Skew handling is managed. |
| `spark.sql.caseSensitive` | `false` | `false` | Can be set if needed. |
| `spark.sql.crossJoin.enabled` | `true` | `true` | Can be set. |
| `spark.sql.legacy.timeParserPolicy` | `EXCEPTION` | `EXCEPTION` | Can be set to `LEGACY` temporarily for migration. |
| `spark.sql.session.timeZone` | JVM default | `Etc/UTC` | Can be set. Important for timestamp handling. |
| `spark.sql.parquet.datetimeRebaseModeInRead` | `EXCEPTION` | `EXCEPTION` | Can be set to `LEGACY` for old Parquet files. |
| `spark.sql.parquet.datetimeRebaseModeInWrite` | `EXCEPTION` | `EXCEPTION` | Can be set to `LEGACY` if needed. |
| `spark.sql.parquet.int96RebaseModeInRead` | `EXCEPTION` | `EXCEPTION` | Can be set. |
| `spark.sql.parquet.int96RebaseModeInWrite` | `EXCEPTION` | `EXCEPTION` | Can be set. |

### Delta Lake Configs

| Config | Classic 13.3 Default | Serverless Default | Notes |
|--------|---------------------|-------------------|-------|
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` | Auto-optimize writes are on by default on serverless. |
| `spark.databricks.delta.autoCompact.enabled` | `false` | `true` | Auto-compact is on by default. |
| `spark.databricks.delta.schema.autoMerge.enabled` | `false` | `false` | Can be set. |
| `spark.databricks.delta.merge.enableLowShuffle` | N/A | `true` | Available on serverless. |
| `spark.databricks.delta.properties.defaults.enableDeletionVectors` | `false` | `true` | Deletion vectors default on for new tables on serverless. |
| `spark.databricks.delta.retentionDurationCheck.enabled` | `true` | `true` | Can be set (use with caution). |

### Data Format Configs

| Config | Classic 13.3 Default | Serverless Default | Notes |
|--------|---------------------|-------------------|-------|
| `spark.sql.parquet.compression.codec` | `snappy` | `zstd` | Can be set. |
| `spark.sql.orc.compression.codec` | `snappy` | `zstd` | Can be set. |
| `spark.sql.jsonGenerator.ignoreNullFields` | `true` | `true` | Can be set. |
| `spark.sql.csv.parser.columnNameOfCorruptRecord` | `_corrupt_record` | `_corrupt_record` | Can be set. |

---

## Unsupported Configs on Serverless

These configs CANNOT be set on serverless. Setting them will either be silently ignored or throw an error.

### Executor/Driver Resource Configs (Serverless Manages These)

| Config | Action | Why |
|--------|--------|-----|
| `spark.executor.memory` | **Remove** | Serverless auto-scales memory. |
| `spark.executor.cores` | **Remove** | Serverless manages core allocation. |
| `spark.executor.instances` | **Remove** | Serverless auto-scales instances. |
| `spark.executor.memoryOverhead` | **Remove** | Managed by serverless. |
| `spark.driver.memory` | **Remove** | Serverless manages driver resources. |
| `spark.driver.cores` | **Remove** | Managed. |
| `spark.driver.maxResultSize` | **Remove** | Serverless manages result size limits. |
| `spark.driver.extraJavaOptions` | **Remove** | No JVM option access on serverless. |
| `spark.executor.extraJavaOptions` | **Remove** | No JVM option access on serverless. |
| `spark.driver.extraClassPath` | **Remove** | Use requirements.txt for dependencies. |
| `spark.executor.extraClassPath` | **Remove** | Use requirements.txt. |

### Dynamic Allocation Configs (Serverless Has Its Own Auto-Scaling)

| Config | Action | Why |
|--------|--------|-----|
| `spark.dynamicAllocation.enabled` | **Remove** | Serverless has its own scaling. |
| `spark.dynamicAllocation.minExecutors` | **Remove** | Managed. |
| `spark.dynamicAllocation.maxExecutors` | **Remove** | Managed. |
| `spark.dynamicAllocation.initialExecutors` | **Remove** | Managed. |
| `spark.dynamicAllocation.executorIdleTimeout` | **Remove** | Managed. |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | **Remove** | Managed. |

### Shuffle and Network Configs (Serverless Manages These)

| Config | Action | Why |
|--------|--------|-----|
| `spark.shuffle.service.enabled` | **Remove** | External shuffle service not applicable. |
| `spark.shuffle.compress` | **Remove** | Managed. |
| `spark.shuffle.spill.compress` | **Remove** | Managed. |
| `spark.network.timeout` | **Remove** | Managed. |
| `spark.rpc.message.maxSize` | **Remove** | Managed. |
| `spark.kryoserializer.buffer.max` | **Remove** | Managed. |

### Serialization Configs

| Config | Action | Why |
|--------|--------|-----|
| `spark.serializer` | **Remove** | Serverless uses its own serialization. |
| `spark.kryo.registrationRequired` | **Remove** | Not applicable. |
| `spark.kryo.classesToRegister` | **Remove** | Not applicable. |

### Storage and Warehouse Configs

| Config | Action | Why |
|--------|--------|-----|
| `spark.sql.warehouse.dir` | **Remove** | Managed by Unity Catalog. |
| `spark.hadoop.fs.defaultFS` | **Remove** | Not applicable. |
| `spark.hadoop.*` | **Remove** | Hadoop configs not applicable on serverless. |
| `spark.databricks.cluster.usageTags.*` | **Remove** | Not applicable on serverless. |
| `fs.azure.account.key.*` | **Remove** | Use Unity Catalog external locations instead. |
| `fs.azure.account.oauth2.*` | **Remove** | Use Unity Catalog. |

### Databricks Cluster Configs

| Config | Action | Why |
|--------|--------|-----|
| `spark.databricks.cluster.profile` | **Remove** | Not applicable. |
| `spark.databricks.passthrough.enabled` | **Remove** | Credential passthrough not applicable; use UC. |
| `spark.databricks.pyspark.enableProcessIsolation` | **Remove** | Managed. |
| `spark.databricks.repl.allowedLanguages` | **Remove** | Not applicable. |
| `spark.databricks.acl.dfAclsEnabled` | **Remove** | Use Unity Catalog permissions. |

---

## Configs That Changed Defaults

These configs exist on both classic and serverless but have different default values. Code that relied on the old default may behave differently.

| Config | Classic 13.3 Default | Serverless Default | Impact | Action |
|--------|---------------------|-------------------|--------|--------|
| `spark.sql.ansi.enabled` | `false` | `true` (mandatory) | **CRITICAL** — silent nulls become exceptions | Fix code with TRY_CAST, TRY_DIVIDE, null guards. See ANSI guide. |
| `spark.sql.sources.default` | `parquet` | `delta` | Affects `spark.read`/`spark.write` without format | Add explicit `.format("parquet")` where needed. |
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` | Writes are auto-optimized | Usually beneficial. Only set to false if causing issues. |
| `spark.databricks.delta.autoCompact.enabled` | `false` | `true` | Auto-compaction runs after writes | Usually beneficial. May increase write latency slightly. |
| `spark.sql.parquet.compression.codec` | `snappy` | `zstd` | Different compression for Parquet | Usually fine. Set explicitly if downstream needs snappy. |
| `spark.sql.session.timeZone` | JVM default (varies) | `Etc/UTC` | **IMPORTANT** — timestamp behavior may change | Set explicitly if code depends on a specific timezone. |

---

## Migration Decision Matrix

For each config found in code, use this decision tree:

```
Is the config in the "Supported" table?
├── YES → Keep it, but check if the default changed
│         └── Default changed? → Evaluate if code depends on old default
│                                ├── YES → Set explicitly to old value OR fix code
│                                └── NO → Remove the explicit set (use new default)
├── NO → Is it in the "Unsupported" table?
│        └── YES → Remove it
│                  └── Does the code depend on this config's behavior?
│                       ├── YES → Rewrite code to not depend on it
│                       └── NO → Safe to remove
└── NOT LISTED → Check Databricks docs for the specific config
                 └── If still unclear, test on serverless. It will error if unsupported.
```

---

## Code-Level Impact Analysis

When you find a config in notebook code, don't just remove it — analyze whether the surrounding code depends on its behavior.

### Example: spark.sql.shuffle.partitions

```python
# Classic compute — explicit partition control:
spark.conf.set("spark.sql.shuffle.partitions", "8")
df = df.repartition(8)  # Forces 8 partitions for downstream write

# Serverless — remove both:
# spark.conf.set is unnecessary (serverless auto-tunes)
# repartition(8) is counterproductive (serverless knows better)
# BUT: check if downstream code expects exactly 8 output files
```

### Example: spark.driver.maxResultSize

```python
# Classic compute:
spark.conf.set("spark.driver.maxResultSize", "4g")
large_result = spark.sql("SELECT * FROM big_table").collect()

# Serverless:
# Remove the config (managed)
# BUT: the .collect() on a large table may still fail
# → Consider whether .collect() is necessary, or use .toPandas() with limits
```

### Example: spark.sql.session.timeZone

```python
# Classic compute (JVM default was US/Eastern):
# All timestamp operations use US/Eastern implicitly

# Serverless (default is UTC):
# Timestamps will be interpreted as UTC!
# → Set explicitly if the pipeline depends on a specific timezone:
spark.conf.set("spark.sql.session.timeZone", "US/Eastern")
```

---

## Init Script Config Migration

Init scripts often set Spark configs. These must be migrated to notebook code or removed entirely.

### Common Init Script Patterns

```bash
# Pattern 1: Setting Spark configs via spark-defaults.conf
echo "spark.executor.memory 16g" >> /databricks/spark/conf/spark-defaults.conf
# Action: Remove (serverless manages resources)

# Pattern 2: Setting environment variables that affect Spark
export SPARK_DAEMON_JAVA_OPTS="-Xmx2g"
# Action: Remove (not applicable)

# Pattern 3: Installing libraries
pip install some-package==1.2.3
# Action: Move to requirements.txt

# Pattern 4: Setting Spark configs for logging/monitoring
echo "spark.extraListeners com.example.MyListener" >> /databricks/spark/conf/spark-defaults.conf
# Action: Remove (custom listeners not supported on serverless)
# → Use System Tables or Azure Diagnostic Settings for monitoring

# Pattern 5: Setting Hadoop/cloud storage credentials
echo "fs.azure.account.key.myaccount.blob.core.windows.net XXXXX" >> /databricks/spark/conf/spark-defaults.conf
# Action: Remove. Use Unity Catalog external locations instead.
```

---

## Audit Notebook Template

Use this template to audit a notebook for Spark config issues:

```python
# Paste in a Databricks notebook to scan for config usage
import re

# Get all spark configs currently set
all_configs = spark.sparkContext.getConf().getAll()
print("=== Current Spark Configs ===")
for key, value in sorted(all_configs):
    print(f"  {key} = {value}")

# Define serverless-incompatible patterns
unsupported_prefixes = [
    "spark.executor.",
    "spark.driver.extra",
    "spark.dynamicAllocation.",
    "spark.shuffle.service.",
    "spark.hadoop.",
    "spark.databricks.cluster.",
    "spark.databricks.passthrough.",
    "fs.azure.",
    "fs.s3a.",
]

print("\n=== Configs Incompatible with Serverless ===")
for key, value in sorted(all_configs):
    for prefix in unsupported_prefixes:
        if key.startswith(prefix):
            print(f"  REMOVE: {key} = {value}")
            break

# Check for ANSI-critical configs
ansi_config = spark.conf.get("spark.sql.ansi.enabled", "not set")
print(f"\n=== ANSI Mode: {ansi_config} ===")
if ansi_config == "false":
    print("  WARNING: ANSI mode is false. Must fix code for serverless (ANSI always ON).")
```

---

## Quick Reference: Most Common Configs to Address

Ranked by frequency of occurrence in Molina's codebase:

| Rank | Config | Frequency | Action |
|------|--------|-----------|--------|
| 1 | `spark.sql.shuffle.partitions` | Very common | Remove — serverless auto-tunes |
| 2 | `spark.sql.ansi.enabled` | Common (set to false) | Remove — fix code instead |
| 3 | `spark.executor.memory/cores` | Common in cluster configs | Remove — serverless manages |
| 4 | `spark.dynamicAllocation.*` | Common in cluster configs | Remove — serverless auto-scales |
| 5 | `spark.databricks.delta.optimizeWrite.enabled` | Moderate | Remove — already true on serverless |
| 6 | `spark.sql.session.timeZone` | Moderate | Keep if pipeline depends on specific TZ |
| 7 | `spark.driver.maxResultSize` | Moderate | Remove — managed |
| 8 | `spark.hadoop.*` / `fs.azure.*` | Moderate | Remove — use Unity Catalog |
| 9 | `spark.serializer` | Low | Remove — managed |
| 10 | `spark.sql.sources.default` | Low | Remove — delta is default on serverless |
