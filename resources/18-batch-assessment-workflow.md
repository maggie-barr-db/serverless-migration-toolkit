# Batch Assessment Workflow

Systematic process for assessing and routing batches of 12-100 Databricks jobs for migration across four paths. This is the operational entry point for every migration batch at the customer.

**Customer context:** the customer, ~5000 jobs, Azure Databricks, Azure DevOps CI/CD with PowerShell deployment scripts. Healthcare data -- silent data changes are unacceptable. The customer uses `USE CATALOG {{env}}_catalog` with 2-part table names -- this is correct Unity Catalog usage and must NOT be flagged as non-UC.

**Two execution tracks:**
1. **Databricks-side** -- notebook code, spark configs, table metadata. Genie Code assesses and assists.
2. **Repo-side (Azure DevOps)** -- job JSON templates, CI/CD pipeline variables, PowerShell deployment scripts. Changed upstream in the repository.

**Migration paths:**

| Path | From | To | What Changes |
|------|------|----|-------------|
| **A** | Scala 13.3 | Scala 16.4 | DBR upgrade only |
| **B** | Scala 13.3 | PySpark Serverless | Language conversion + compute migration |
| **C** | PySpark/SQL 13.3 | PySpark/SQL Serverless | Compute migration (env v4, Python 3.12) |
| **D** | SQL-only | DBSQL Serverless SQL notebooks | SQL warehouse migration |

---

## Table of Contents

1. [Workflow Overview](#1-workflow-overview)
2. [Input Manifest Format](#2-input-manifest-format)
3. [Job Config Extraction via Jobs API](#3-job-config-extraction-via-jobs-api)
4. [Automatic Job Classification](#4-automatic-job-classification)
5. [Feasibility Validation](#5-feasibility-validation)
6. [Path Routing Logic](#6-path-routing-logic)
7. [Notebook Code Scan](#7-notebook-code-scan)
8. [Data Compatibility Checks](#8-data-compatibility-checks)
9. [Two-Track Classification](#9-two-track-classification)
10. [Per-Job Assessment Report](#10-per-job-assessment-report)
11. [Batch Summary Report](#11-batch-summary-report)
12. [Genie Code Recommendation Engine](#12-genie-code-recommendation-engine)
13. [Workflow Phases and Cross-References](#13-workflow-phases-and-cross-references)
14. [Complete Batch Assessment Notebook](#14-complete-batch-assessment-notebook)

---

## 1. Workflow Overview

Each batch goes through a five-step assessment pipeline before any code changes are made:

```
MANIFEST (CSV/JSON)
     │
     ▼
┌─────────────────────┐
│ 1. Extract Job Configs │  ← Jobs API: pull DBR, language, cluster, tasks, notebooks
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 2. Feasibility Check   │  ← Flag impossible/risky combos (Scala+serverless, streaming, GPU)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 3. Path Routing        │  ← Confirm or override prescribed path based on actual job config
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 4. Code + Data Scan    │  ← Scan notebooks for issues; run data compatibility on output tables
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 5. Report Generation   │  ← Per-job reports + batch summary + effort estimates
└─────────────────────┘
```

**Genie Code's role:** Run steps 1-5 for every job in the manifest, produce the reports, and recommend paths and actions. The customer reviews the reports and approves before any execution begins.

---

## 2. Input Manifest Format

The manifest is the single input that drives the entire workflow. It can be provided as CSV or JSON.

### CSV Format

```csv
job_id,job_name,prescribed_path,priority,tower_lead,notes
545285009490448,sample_claims_pipeline,C,HIGH,Team Lead A,claims pipeline
545285009490449,sample_pharmacy_etl,C,HIGH,Team Lead A,pharmacy ETL - runs 6am daily
545285009490450,sample_scala_scoring,B,HIGH,Team Lead B,Scala ML scoring - needs PySpark conversion
545285009490451,sample_scala_refresh,A,MEDIUM,Team Lead B,Scala only - stays on classic 16.4
545285009490452,sample_sql_report,D,MEDIUM,Team Lead A,pure SQL reporting notebook
545285009490453,sample_provider_build,C,LOW,Team Lead A,monthly batch - large runtime
545285009490454,sample_streaming_ingest,C,HIGH,Team Lead A,streaming job - needs feasibility check
545285009490455,sample_gpu_ml_job,C,MEDIUM,Team Lead B,GPU ML runtime - needs evaluation
```

### JSON Format

```json
{
  "batch_id": "batch_001",
  "batch_name": "Claims Tower - Sprint 1",
  "created_date": "2026-04-16",
  "created_by": "Team Lead A",
  "jobs": [
    {
      "job_id": "545285009490448",
      "job_name": "sample_claims_pipeline",
      "prescribed_path": "C",
      "priority": "HIGH",
      "tower_lead": "Team Lead A",
      "notes": "claims pipeline"
    },
    {
      "job_id": "545285009490449",
      "job_name": "sample_pharmacy_etl",
      "prescribed_path": "C",
      "priority": "HIGH",
      "tower_lead": "Team Lead A",
      "notes": "pharmacy ETL - runs 6am daily"
    }
  ]
}
```

### Field Definitions

| Field | Required | Values | Description |
|-------|----------|--------|-------------|
| `job_id` | Yes | Databricks job ID (numeric) | The job ID from the Databricks workspace |
| `job_name` | Yes | String | Human-readable job name for reports |
| `prescribed_path` | Yes | `A`, `B`, `C`, `D` | The customer's intended migration path |
| `priority` | Yes | `HIGH`, `MEDIUM`, `LOW` | Migration priority for ordering work |
| `tower_lead` | Yes | String | The person responsible for this job's tower |
| `notes` | No | String | Free-text context about the job |

### Loading the Manifest

```python
# === Load manifest in a Databricks notebook ===

import json
import csv
from io import StringIO

def load_manifest_csv(csv_path: str) -> list[dict]:
    """Load a manifest from a CSV file (workspace path or Volume path)."""
    if csv_path.startswith("/Volumes"):
        with open(csv_path.replace("/Volumes", "/Volumes"), "r") as f:
            reader = csv.DictReader(f)
            return [row for row in reader]
    else:
        # Workspace file
        content = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        # For workspace files, read via dbutils
        raw = spark.read.text(csv_path).collect()
        lines = [row.value for row in raw]
        reader = csv.DictReader(lines)
        return [row for row in reader]

def load_manifest_json(json_path: str) -> list[dict]:
    """Load a manifest from a JSON file."""
    with open(json_path.replace("/Volumes", "/Volumes"), "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    elif "jobs" in data:
        return data["jobs"]
    else:
        raise ValueError("JSON must be a list of jobs or an object with a 'jobs' key")

# Example usage:
# manifest = load_manifest_csv("/Volumes/prod_catalog/default/migration/batch_001.csv")
# manifest = load_manifest_json("/Volumes/prod_catalog/default/migration/batch_001.json")
```

---

## 3. Job Config Extraction via Jobs API

For each `job_id` in the manifest, pull the full job configuration via the Databricks Jobs API.

### Extraction Code

```python
import requests
import json

def get_job_config(job_id: str) -> dict:
    """Pull full job configuration from the Jobs API."""
    # Get workspace URL and token from the notebook context
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()
    token = ctx.apiToken().get()

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{host}/api/2.1/jobs/get",
        headers=headers,
        params={"job_id": job_id}
    )
    response.raise_for_status()
    return response.json()


def extract_job_metadata(job_config: dict) -> dict:
    """Extract the key metadata fields from a job config."""
    settings = job_config.get("settings", {})
    tasks = settings.get("tasks", [])

    # --- Extract cluster specs ---
    job_clusters = settings.get("job_clusters", [])
    cluster_specs = {}
    for jc in job_clusters:
        key = jc.get("job_cluster_key", "unknown")
        spec = jc.get("new_cluster", {})
        cluster_specs[key] = {
            "spark_version": spec.get("spark_version", ""),
            "node_type_id": spec.get("node_type_id", ""),
            "num_workers": spec.get("num_workers"),
            "autoscale": spec.get("autoscale"),
            "spark_conf": spec.get("spark_conf", {}),
            "init_scripts": spec.get("init_scripts", []),
            "custom_tags": spec.get("custom_tags", {}),
            "spark_env_vars": spec.get("spark_env_vars", {}),
        }

    # --- Extract task metadata ---
    task_details = []
    for task in tasks:
        task_info = {
            "task_key": task.get("task_key", ""),
            "job_cluster_key": task.get("job_cluster_key", ""),
            "existing_cluster_id": task.get("existing_cluster_id", ""),
        }

        # Notebook task
        if "notebook_task" in task:
            nt = task["notebook_task"]
            task_info["type"] = "notebook"
            task_info["notebook_path"] = nt.get("notebook_path", "")
            task_info["base_parameters"] = nt.get("base_parameters", {})
            task_info["source"] = nt.get("source", "WORKSPACE")

        # Spark JAR task
        elif "spark_jar_task" in task:
            jt = task["spark_jar_task"]
            task_info["type"] = "spark_jar"
            task_info["main_class_name"] = jt.get("main_class_name", "")
            task_info["jar_uri"] = jt.get("jar_uri", "")

        # Python wheel task
        elif "python_wheel_task" in task:
            pw = task["python_wheel_task"]
            task_info["type"] = "python_wheel"
            task_info["package_name"] = pw.get("package_name", "")
            task_info["entry_point"] = pw.get("entry_point", "")

        # Spark Python task
        elif "spark_python_task" in task:
            sp = task["spark_python_task"]
            task_info["type"] = "spark_python"
            task_info["python_file"] = sp.get("python_file", "")

        # SQL task
        elif "sql_task" in task:
            sq = task["sql_task"]
            task_info["type"] = "sql"
            task_info["query_id"] = sq.get("query", {}).get("query_id", "")
            task_info["warehouse_id"] = sq.get("warehouse_id", "")

        # Pipeline task
        elif "pipeline_task" in task:
            pt = task["pipeline_task"]
            task_info["type"] = "pipeline"
            task_info["pipeline_id"] = pt.get("pipeline_id", "")

        else:
            task_info["type"] = "unknown"

        # Libraries on the task
        task_info["libraries"] = task.get("libraries", [])

        task_details.append(task_info)

    # --- Extract libraries at job level ---
    job_libraries = []
    for jc in job_clusters:
        spec = jc.get("new_cluster", {})
        # Cluster-level libraries are not in new_cluster, check task-level
    for task in tasks:
        for lib in task.get("libraries", []):
            job_libraries.append(lib)

    # --- Build the metadata ---
    metadata = {
        "job_id": str(job_config.get("job_id", "")),
        "job_name": settings.get("name", ""),
        "job_clusters": cluster_specs,
        "tasks": task_details,
        "libraries": job_libraries,
        "schedule": settings.get("schedule", {}),
        "max_concurrent_runs": settings.get("max_concurrent_runs", 1),
        "tags": settings.get("tags", {}),
        "timeout_seconds": settings.get("timeout_seconds", 0),
        "environments": settings.get("environments", []),
        "queue": settings.get("queue", {}),
    }

    return metadata


def extract_all_notebook_paths(metadata: dict) -> list[str]:
    """Get all notebook paths referenced by the job."""
    paths = []
    for task in metadata.get("tasks", []):
        if task.get("type") == "notebook" and task.get("notebook_path"):
            paths.append(task["notebook_path"])
    return paths


def extract_dbr_versions(metadata: dict) -> list[str]:
    """Get all DBR versions used by the job."""
    versions = set()
    for key, spec in metadata.get("job_clusters", {}).items():
        sv = spec.get("spark_version", "")
        if sv:
            versions.add(sv)
    return list(versions)


def extract_spark_configs(metadata: dict) -> dict:
    """Get all spark configs across all cluster specs."""
    all_configs = {}
    for key, spec in metadata.get("job_clusters", {}).items():
        for k, v in spec.get("spark_conf", {}).items():
            all_configs[k] = v
    return all_configs


def extract_init_scripts(metadata: dict) -> list[dict]:
    """Get all init scripts across all cluster specs."""
    scripts = []
    for key, spec in metadata.get("job_clusters", {}).items():
        for script in spec.get("init_scripts", []):
            scripts.append(script)
    return scripts
```

### Batch Extraction

```python
def extract_batch_configs(manifest: list[dict]) -> dict:
    """Extract job configs for every job in the manifest."""
    results = {}
    errors = []

    for entry in manifest:
        job_id = entry["job_id"]
        job_name = entry.get("job_name", job_id)
        try:
            raw_config = get_job_config(job_id)
            metadata = extract_job_metadata(raw_config)
            metadata["prescribed_path"] = entry.get("prescribed_path", "")
            metadata["priority"] = entry.get("priority", "MEDIUM")
            metadata["tower_lead"] = entry.get("tower_lead", "")
            metadata["notes"] = entry.get("notes", "")
            results[job_id] = metadata
            print(f"  OK: {job_name} ({job_id})")
        except Exception as e:
            error_msg = f"FAILED: {job_name} ({job_id}): {e}"
            errors.append(error_msg)
            print(f"  {error_msg}")

    print(f"\nExtracted {len(results)} of {len(manifest)} jobs. Errors: {len(errors)}")
    return results
```

---

## 4. Automatic Job Classification

After extracting configs, automatically classify each job by language, DBR version, compute type, and special characteristics.

```python
import re

def classify_job(metadata: dict) -> dict:
    """Classify a job based on its extracted metadata."""
    classification = {
        "languages": set(),
        "dbr_versions": [],
        "compute_type": "unknown",   # classic, serverless, interactive
        "has_jars": False,
        "has_init_scripts": False,
        "has_streaming": False,
        "has_gpu": False,
        "has_ml_runtime": False,
        "task_types": set(),
        "notebook_count": 0,
        "library_count": 0,
    }

    # --- DBR versions ---
    dbr_versions = extract_dbr_versions(metadata)
    classification["dbr_versions"] = dbr_versions

    # --- Detect ML runtime ---
    for v in dbr_versions:
        if "-ml-" in v.lower():
            classification["has_ml_runtime"] = True

    # --- Detect GPU ---
    for key, spec in metadata.get("job_clusters", {}).items():
        node_type = spec.get("node_type_id", "").lower()
        if any(gpu in node_type for gpu in ["gpu", "nc", "nd", "nv", "a10", "t4", "v100"]):
            classification["has_gpu"] = True

    # --- Task type analysis ---
    for task in metadata.get("tasks", []):
        task_type = task.get("type", "unknown")
        classification["task_types"].add(task_type)

        if task_type == "notebook":
            classification["notebook_count"] += 1
        elif task_type == "spark_jar":
            classification["has_jars"] = True
        elif task_type == "pipeline":
            classification["has_streaming"] = True  # DLT pipelines may be streaming

    # --- Init scripts ---
    init_scripts = extract_init_scripts(metadata)
    if init_scripts:
        classification["has_init_scripts"] = True

    # --- Libraries ---
    for lib in metadata.get("libraries", []):
        classification["library_count"] += 1
        if "jar" in lib:
            classification["has_jars"] = True

    # --- Language detection from DBR version string ---
    for v in dbr_versions:
        if "scala" in v.lower() or v.endswith("-scala2.12"):
            classification["languages"].add("scala")
        # Most DBR versions don't encode language; will be refined by notebook scan

    # --- Compute type ---
    if metadata.get("environments"):
        classification["compute_type"] = "serverless"
    else:
        has_cluster = any(
            task.get("job_cluster_key") or task.get("existing_cluster_id")
            for task in metadata.get("tasks", [])
        )
        classification["compute_type"] = "classic" if has_cluster else "unknown"

    # Convert sets to lists for JSON serialization
    classification["languages"] = list(classification["languages"])
    classification["task_types"] = list(classification["task_types"])

    return classification


def detect_notebook_language(notebook_path: str) -> str:
    """Detect the primary language of a notebook by exporting and scanning it."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()
    token = ctx.apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        f"{host}/api/2.0/workspace/get-status",
        headers=headers,
        params={"path": notebook_path}
    )
    if response.status_code == 200:
        info = response.json()
        language = info.get("language", "").upper()
        if language in ("SCALA", "PYTHON", "SQL", "R"):
            return language
    return "UNKNOWN"


def detect_streaming_in_notebooks(notebook_content: str) -> bool:
    """Check if notebook content contains streaming patterns."""
    streaming_patterns = [
        r"\.readStream",
        r"\.writeStream",
        r"spark\.readStream",
        r"spark\.streams",
        r"streamingQuery",
        r"\.trigger\(",
        r"\.awaitTermination",
        r"foreachBatch",
        r"\.start\(\)",
        r"isStreaming",
    ]
    for pattern in streaming_patterns:
        if re.search(pattern, notebook_content, re.IGNORECASE):
            return True
    return False
```

---

## 5. Feasibility Validation

Flag impossible or high-risk combinations before routing. Each check produces a `BLOCK`, `WARN`, or `PASS` status.

### Feasibility Rules

| # | Rule | Status | Description |
|---|------|--------|-------------|
| F1 | Scala + serverless without conversion | **BLOCK** | Serverless does not support Scala. Must be Path B (convert first), not C. |
| F2 | Streaming job + serverless | **BLOCK** | Structured Streaming is not supported on serverless general compute. Job must stay on classic or use DLT. |
| F3 | GPU/ML runtime + serverless | **WARN** | GPU instances are not available on serverless. Evaluate if ML runtime features are actually needed. May need to stay on classic. |
| F4 | JAR task + serverless | **BLOCK** | JAR libraries are not supported in serverless notebooks. Rewrite in Python or keep on classic compute. |
| F5 | ThreadPoolExecutor usage | **WARN** | Concurrent notebook/task execution via ThreadPoolExecutor degrades performance on serverless. Recommend Databricks workflow for-each tasks instead. |
| F6 | Runtime > 4 hours on classic | **WARN** | Serverless sessions have idle timeout behavior. Long-running jobs may need session keep-alive or task decomposition. |
| F7 | Init scripts present | **WARN** | Init scripts are not supported on serverless. Dependencies must move to requirements.txt or the environment pane. |
| F8 | RDD API usage | **WARN** | RDD operations (sc.textFile, sc.parallelize, rdd.map, etc.) are not supported on serverless. Must rewrite as DataFrame operations. |
| F9 | R language notebooks | **BLOCK** | R is not supported on serverless. Must stay on classic compute. |
| F10 | Global temp views | **WARN** | Global temp views are not supported on serverless. Convert to session-scoped temp views or CTEs. |

### Feasibility Check Code

```python
def check_feasibility(metadata: dict, classification: dict) -> list[dict]:
    """Run all feasibility checks. Returns a list of findings."""
    findings = []
    prescribed = metadata.get("prescribed_path", "").upper()

    # F1: Scala + serverless without conversion
    if "scala" in [l.lower() for l in classification.get("languages", [])]:
        if prescribed in ("C", "D"):
            findings.append({
                "rule": "F1",
                "status": "BLOCK",
                "message": (
                    f"Scala job prescribed for Path {prescribed} (serverless) "
                    "but Scala is not supported on serverless. "
                    "Must be Path B (convert to PySpark first) or Path A (stay on classic)."
                ),
                "recommended_path": "B",
            })
        elif prescribed == "A":
            findings.append({
                "rule": "F1",
                "status": "PASS",
                "message": "Scala job on Path A (classic 16.4) -- no serverless conflict.",
            })

    # F2: Streaming + serverless
    if classification.get("has_streaming") and prescribed in ("C", "D"):
        findings.append({
            "rule": "F2",
            "status": "BLOCK",
            "message": (
                "Streaming job prescribed for serverless. Structured Streaming is not "
                "supported on serverless general compute. Must stay on classic compute or use DLT."
            ),
        })

    # F3: GPU/ML runtime + serverless
    if classification.get("has_gpu") and prescribed in ("C", "D"):
        findings.append({
            "rule": "F3",
            "status": "WARN",
            "message": (
                "GPU/ML runtime job prescribed for serverless. GPU instances are not "
                "available on serverless. Evaluate if ML runtime features are actually "
                "needed or if the workload can be converted to CPU-only."
            ),
        })
    elif classification.get("has_ml_runtime") and not classification.get("has_gpu"):
        if prescribed in ("C", "D"):
            findings.append({
                "rule": "F3",
                "status": "WARN",
                "message": (
                    "ML runtime detected but no GPU node type. Check if ML runtime "
                    "libraries (sklearn, torch, etc.) are needed. If only PySpark/pandas, "
                    "serverless is fine with requirements.txt for ML packages."
                ),
            })

    # F4: JAR tasks + serverless
    if classification.get("has_jars") and prescribed in ("C", "D"):
        findings.append({
            "rule": "F4",
            "status": "WARN",
            "message": (
                "Job uses JAR tasks or JAR libraries. JAR tasks on serverless are in "
                "public preview. Evaluate if a Python rewrite is feasible. "
                "If not, can use the JAR task feature or keep this task on classic."
            ),
        })

    # F5: ThreadPoolExecutor -- checked during notebook scan (see Section 7)

    # F6: Long runtime -- checked via job run history
    timeout = metadata.get("timeout_seconds", 0)
    if timeout > 14400 and prescribed in ("C", "D"):  # > 4 hours
        findings.append({
            "rule": "F6",
            "status": "WARN",
            "message": (
                f"Job timeout is set to {timeout}s ({timeout/3600:.1f} hours). "
                "Long-running jobs on serverless may encounter session idle timeouts. "
                "Consider decomposing into smaller tasks or adding session keep-alive."
            ),
        })

    # F7: Init scripts
    if classification.get("has_init_scripts") and prescribed in ("C", "D"):
        findings.append({
            "rule": "F7",
            "status": "WARN",
            "message": (
                "Job uses init scripts which are not supported on serverless. "
                "Dependencies must be migrated to requirements.txt or the environment pane."
            ),
        })

    # F8: RDD API -- checked during notebook scan (see Section 7)

    # F9: R language
    if "r" in [l.lower() for l in classification.get("languages", [])]:
        if prescribed in ("C", "D"):
            findings.append({
                "rule": "F9",
                "status": "BLOCK",
                "message": "R language notebooks cannot run on serverless. Must stay on classic compute.",
            })

    # If no findings, everything passed
    if not findings:
        findings.append({
            "rule": "ALL",
            "status": "PASS",
            "message": "All feasibility checks passed.",
        })

    return findings


def feasibility_passed(findings: list[dict]) -> bool:
    """Return True if no BLOCK findings exist."""
    return not any(f["status"] == "BLOCK" for f in findings)
```

---

## 6. Path Routing Logic

After feasibility validation, confirm or override the prescribed path based on actual job characteristics.

### Routing Decision Tree

```
FOR each job in manifest:

    languages = detect_languages(job)
    prescribed = manifest.prescribed_path

    IF any language is Scala:
        IF prescribed in (C, D):
            → OVERRIDE to Path B (must convert to PySpark first, then serverless)
            → NOTE: "Scala cannot run on serverless. Changed from Path {prescribed} to Path B."
        ELIF prescribed == B:
            → CONFIRM Path B (Scala → PySpark + serverless)
        ELIF prescribed == A:
            → CONFIRM Path A (Scala → Scala 16.4, stays on classic)
        ELSE:
            → FLAG for manual review

    ELIF all languages are SQL-only:
        IF prescribed == D:
            → CONFIRM Path D (SQL → DBSQL serverless)
        ELIF prescribed == C:
            → RECOMMEND Path D (all cells are SQL; DBSQL is more cost-effective)
            → NOTE: "All notebooks are SQL-only. Recommend Path D (DBSQL) instead of Path C."
        ELSE:
            → CONFIRM prescribed path

    ELIF languages include Python/PySpark:
        IF prescribed == C:
            IF all_cells_are_sql_only(job):
                → RECOMMEND Path D
                → NOTE: "All cells are SQL despite Python notebook type. Recommend Path D."
            ELSE:
                → CONFIRM Path C (PySpark/SQL → serverless)
        ELIF prescribed == D:
            IF has_pyspark_code(job):
                → OVERRIDE to Path C
                → NOTE: "Contains PySpark code. Changed from Path D to Path C."
            ELSE:
                → CONFIRM Path D
        ELSE:
            → CONFIRM prescribed path

    ELSE:
        → FLAG for manual review
```

### Routing Code

```python
def route_job(metadata: dict, classification: dict, notebook_languages: dict) -> dict:
    """
    Determine the correct migration path based on job analysis.

    Args:
        metadata: Extracted job metadata with prescribed_path
        classification: Auto-classification results
        notebook_languages: Dict of {notebook_path: language} from workspace API

    Returns:
        Dict with recommended_path, prescribed_path, override_reason, confidence
    """
    prescribed = metadata.get("prescribed_path", "").upper()
    all_languages = set()

    # Combine classification languages with notebook-level detection
    for lang in classification.get("languages", []):
        all_languages.add(lang.upper())
    for path, lang in notebook_languages.items():
        all_languages.add(lang.upper())

    result = {
        "prescribed_path": prescribed,
        "recommended_path": prescribed,
        "override": False,
        "override_reason": "",
        "confidence": "HIGH",
    }

    # --- Scala jobs ---
    if "SCALA" in all_languages:
        if prescribed in ("C", "D"):
            result["recommended_path"] = "B"
            result["override"] = True
            result["override_reason"] = (
                f"Scala cannot run on serverless. Changed from Path {prescribed} to Path B "
                "(convert to PySpark, then serverless)."
            )
        elif prescribed == "B":
            result["override_reason"] = "Confirmed: Scala → PySpark + serverless."
        elif prescribed == "A":
            result["override_reason"] = "Confirmed: Scala stays on classic, upgrade to 16.4."
        else:
            result["confidence"] = "LOW"
            result["override_reason"] = f"Scala job with unexpected prescribed path '{prescribed}'. Manual review needed."
        return result

    # --- SQL-only jobs ---
    all_sql = all(lang == "SQL" for lang in all_languages) and len(all_languages) > 0
    if all_sql:
        if prescribed == "D":
            result["override_reason"] = "Confirmed: SQL-only → DBSQL serverless."
        elif prescribed == "C":
            result["recommended_path"] = "D"
            result["override"] = True
            result["override_reason"] = (
                "All notebooks are SQL-only. Recommend Path D (DBSQL serverless) "
                "instead of Path C for better cost efficiency."
            )
            result["confidence"] = "MEDIUM"  # Customer may have reasons for Path C
        return result

    # --- PySpark/Python jobs ---
    if "PYTHON" in all_languages:
        if prescribed == "C":
            result["override_reason"] = "Confirmed: PySpark/SQL → serverless."
        elif prescribed == "D":
            # Check if there is actual PySpark code (not just SQL in Python notebooks)
            result["override_reason"] = (
                "Prescribed Path D but job has Python notebooks. "
                "Will verify during code scan whether all cells are SQL-only."
            )
            result["confidence"] = "MEDIUM"
        return result

    # --- Fallback ---
    result["confidence"] = "LOW"
    result["override_reason"] = f"Could not determine languages. Manual review needed."
    return result
```

---

## 7. Notebook Code Scan

For each job, export and scan all referenced notebooks for migration-relevant issues. Only scan for patterns relevant to the job's confirmed migration path.

### What to Scan Per Path

| Path | ANSI Issues | Serverless Restrictions | Spark Config | Library Issues | Performance Anti-Patterns |
|------|-------------|------------------------|-------------|----------------|--------------------------|
| **A** (Scala 16.4) | Yes | No | Yes (classic configs) | Yes (Scala deps) | No |
| **B** (Scala→PySpark serverless) | Yes | Yes | Yes (serverless configs) | Yes (Python replacements) | Yes |
| **C** (PySpark serverless) | Yes | Yes | Yes (serverless configs) | Yes (Python deps) | Yes |
| **D** (SQL→DBSQL) | Yes | N/A (DBSQL) | N/A | N/A | Yes (query optimization) |

### Notebook Export

```python
import base64

def export_notebook(notebook_path: str) -> str:
    """Export a notebook and return its content as a string."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()
    token = ctx.apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        f"{host}/api/2.0/workspace/export",
        headers=headers,
        params={"path": notebook_path, "format": "SOURCE"}
    )
    response.raise_for_status()
    content_b64 = response.json().get("content", "")
    return base64.b64decode(content_b64).decode("utf-8")


def split_into_cells(content: str) -> list[dict]:
    """Split notebook content into cells with cell numbers."""
    # Databricks notebook cells are separated by # COMMAND ----------
    cell_separator = re.compile(r"#\s*COMMAND\s*-+")
    raw_cells = cell_separator.split(content)
    cells = []
    for i, cell_text in enumerate(raw_cells):
        cell_text = cell_text.strip()
        if cell_text:
            # Detect magic commands
            magic = None
            if cell_text.startswith("%sql"):
                magic = "sql"
            elif cell_text.startswith("%scala"):
                magic = "scala"
            elif cell_text.startswith("%python"):
                magic = "python"
            elif cell_text.startswith("%md"):
                magic = "markdown"
            elif cell_text.startswith("%sh"):
                magic = "shell"
            elif cell_text.startswith("%run"):
                magic = "run"
            elif cell_text.startswith("%pip"):
                magic = "pip"

            cells.append({
                "cell_number": i + 1,
                "magic": magic,
                "content": cell_text,
                "line_count": len(cell_text.splitlines()),
            })
    return cells
```

### Scan Pattern Library

```python
# === ANSI compliance patterns (all paths) ===
# Reference: 07-ansi-compliance-reference.md

ANSI_PATTERNS = {
    "CAST_to_type": {
        "severity": "CRITICAL",
        "category": "ANSI",
        "sql_pattern": r"(?i)CAST\s*\(.*?\bAS\b\s+(?:INT|INTEGER|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|DATE|TIMESTAMP|BOOLEAN)\s*\)",
        "pyspark_pattern": r'\.cast\s*\(\s*["\'](?:int|integer|bigint|smallint|tinyint|float|double|decimal|date|timestamp|boolean)["\']',
        "scala_pattern": r"\.cast\s*\(\s*(?:IntegerType|LongType|ShortType|ByteType|FloatType|DoubleType|DecimalType|DateType|TimestampType|BooleanType)\s*\)",
        "fix": "Use TRY_CAST (SQL), add when/otherwise null guard (PySpark), or add regex guard (Scala)",
        "reference": "07-ansi-compliance-reference.md, Pattern 1",
    },
    "division_by_zero": {
        "severity": "CRITICAL",
        "category": "ANSI",
        "sql_pattern": r"(?i)(?:\/\s*\w+|\/\s*\()",
        "pyspark_pattern": r"(?:F\.col\([^)]+\)\s*\/|\.divide\()",
        "fix": "Use TRY_DIVIDE (SQL) or add CASE WHEN divisor = 0 THEN NULL guard",
        "reference": "07-ansi-compliance-reference.md, Pattern 2",
    },
    "array_index_access": {
        "severity": "HIGH",
        "category": "ANSI",
        "sql_pattern": r"(?i)\w+\s*\[\s*\d+\s*\]",
        "pyspark_pattern": r"\.getItem\(\d+\)|\.getField\(",
        "fix": "Use TRY_ELEMENT_AT or add bounds check",
        "reference": "07-ansi-compliance-reference.md, Pattern 3",
    },
    "map_key_access": {
        "severity": "HIGH",
        "category": "ANSI",
        "sql_pattern": r"(?i)\w+\s*\[\s*['\"]",
        "pyspark_pattern": r"\.getItem\(\s*['\"]",
        "fix": "Use TRY_ELEMENT_AT or check key existence first",
        "reference": "07-ansi-compliance-reference.md, Pattern 4",
    },
    "integer_overflow": {
        "severity": "HIGH",
        "category": "ANSI",
        "sql_pattern": r"(?i)(?:SUM|COUNT)\s*\(.*?\)\s*\+|CAST\s*\(.*?\bAS\b\s+(?:INT|SMALLINT|TINYINT)\s*\)",
        "pyspark_pattern": r'\.cast\s*\(\s*["\'](?:int|short|byte)["\']',
        "fix": "Use BIGINT for aggregation results, add overflow guards",
        "reference": "07-ansi-compliance-reference.md, Pattern 5",
    },
    "boolean_int_comparison": {
        "severity": "HIGH",
        "category": "ANSI",
        "sql_pattern": r"(?i)\w+\s*=\s*[01]\b(?!\s*\.\d)|\b[01]\s*=\s*\w+",
        "pyspark_pattern": r"==\s*(?:True|False|0|1)\b",
        "fix": "Use IS TRUE / IS FALSE (SQL) or == True / == False (PySpark)",
        "reference": "07-ansi-compliance-reference.md, Pattern 6",
    },
    "to_timestamp_invalid": {
        "severity": "CRITICAL",
        "category": "ANSI",
        "sql_pattern": r"(?i)(?:to_timestamp|to_date)\s*\(",
        "pyspark_pattern": r"F\.to_timestamp\(|F\.to_date\(",
        "fix": "Use try_to_timestamp / try_to_date",
        "reference": "07-ansi-compliance-reference.md, Pattern 7",
    },
}

# === Serverless restriction patterns (Paths B, C) ===
# Reference: 03-pyspark-sql-13.3-to-pyspark-sql-serverless.md, 05-spark-config-classic-to-serverless.md

SERVERLESS_PATTERNS = {
    "persist_cache": {
        "severity": "MEDIUM",
        "category": "SERVERLESS",
        "sql_pattern": r"(?i)(?:CACHE\s+TABLE|UNCACHE\s+TABLE)",
        "pyspark_pattern": r"\.persist\(|\.cache\(|\.unpersist\(",
        "scala_pattern": r"\.persist\(|\.cache\(|\.unpersist\(",
        "fix": "Remove. Serverless manages memory automatically.",
        "reference": "03-pyspark-sql-13.3-to-pyspark-sql-serverless.md, Section 4",
    },
    "refresh_table": {
        "severity": "MEDIUM",
        "category": "SERVERLESS",
        "sql_pattern": r"(?i)REFRESH\s+TABLE",
        "pyspark_pattern": r"spark\.catalog\.refreshTable|spark\.sql\([\"']REFRESH TABLE",
        "fix": "Remove. Serverless auto-handles catalog cache invalidation.",
    },
    "msck_repair": {
        "severity": "CRITICAL",
        "category": "SERVERLESS",
        "sql_pattern": r"(?i)MSCK\s+REPAIR\s+TABLE",
        "pyspark_pattern": r"spark\.sql\([\"']MSCK REPAIR",
        "fix": "Remove. Not needed with Delta tables in Unity Catalog.",
    },
    "rdd_api": {
        "severity": "CRITICAL",
        "category": "SERVERLESS",
        "pyspark_pattern": r"sc\.textFile|sc\.parallelize|\.rdd\.|sc\.wholeTextFiles|sc\.hadoopFile",
        "scala_pattern": r"sc\.textFile|sc\.parallelize|\.rdd\.|sc\.wholeTextFiles",
        "fix": "Rewrite as DataFrame operations. See 03 resource, Section 4.",
    },
    "global_temp_view": {
        "severity": "HIGH",
        "category": "SERVERLESS",
        "sql_pattern": r"(?i)(?:CREATE\s+(?:OR\s+REPLACE\s+)?GLOBAL\s+TEMP(?:ORARY)?\s+VIEW|global_temp\.)",
        "pyspark_pattern": r"\.createGlobalTempView\(|\.createOrReplaceGlobalTempView\(",
        "fix": "Convert to session-scoped temp views (createOrReplaceTempView) or CTEs.",
    },
    "env_var_access": {
        "severity": "HIGH",
        "category": "SERVERLESS",
        "pyspark_pattern": r"os\.environ\.get\(|os\.environ\[|os\.getenv\(",
        "fix": "Replace with dbutils.widgets.get(). See 03 resource, Section 6.",
    },
    "spark_conf_set_ansi": {
        "severity": "HIGH",
        "category": "CONFIG",
        "pyspark_pattern": r"spark\.conf\.set\s*\(\s*[\"']spark\.sql\.ansi\.enabled[\"']\s*,\s*[\"']false[\"']",
        "sql_pattern": r"(?i)SET\s+spark\.sql\.ansi\.enabled\s*=\s*false",
        "fix": "Remove. ANSI mode is mandatory on serverless. Fix code to be ANSI-safe instead.",
        "reference": "05-spark-config-classic-to-serverless.md",
    },
    "unsupported_spark_configs": {
        "severity": "HIGH",
        "category": "CONFIG",
        "pyspark_pattern": r"spark\.conf\.set\s*\(\s*[\"'](?:spark\.executor\.|spark\.driver\.|spark\.dynamicAllocation\.|spark\.shuffle\.)",
        "sql_pattern": r"(?i)SET\s+(?:spark\.executor\.|spark\.driver\.|spark\.dynamicAllocation\.|spark\.shuffle\.)",
        "fix": "Remove. Infrastructure configs are managed by serverless. See 05 resource.",
        "reference": "05-spark-config-classic-to-serverless.md",
    },
    "thread_pool_executor": {
        "severity": "HIGH",
        "category": "SERVERLESS",
        "pyspark_pattern": r"ThreadPoolExecutor|concurrent\.futures|multiprocessing\.Pool|threading\.Thread",
        "fix": "Serverless throttles parallelism. Use Databricks workflow for-each tasks instead.",
    },
    "materialized_view": {
        "severity": "CRITICAL",
        "category": "SERVERLESS",
        "sql_pattern": r"(?i)(?:CREATE|REFRESH)\s+MATERIALIZED\s+VIEW",
        "fix": "Materialized views must use SQL Warehouse, not serverless general compute.",
    },
    "shell_commands": {
        "severity": "HIGH",
        "category": "SERVERLESS",
        "pyspark_pattern": r"(?m)^%sh\b|subprocess\.(?:run|call|Popen)|os\.system\(",
        "fix": "Shell commands have limited functionality on serverless. Evaluate necessity.",
    },
}

# === Performance anti-patterns (Paths B, C, D) ===
# Reference: 09-performance-optimization-patterns.md

PERFORMANCE_PATTERNS = {
    "unnecessary_count": {
        "severity": "LOW",
        "category": "PERFORMANCE",
        "pyspark_pattern": r"\.count\(\)\s*>\s*0|display\(.*\.count\(\)",
        "fix": "Use .first() is not None instead of .count() > 0. Remove display(df.count()).",
        "reference": "09-performance-optimization-patterns.md",
    },
    "collect_large": {
        "severity": "MEDIUM",
        "category": "PERFORMANCE",
        "pyspark_pattern": r"\.collect\(\)",
        "fix": "Review if collect() is needed. May OOM on serverless with large datasets. Use .take(N) or .toPandas() with limits.",
    },
    "display_count": {
        "severity": "LOW",
        "category": "PERFORMANCE",
        "pyspark_pattern": r"display\(\s*\w+\.count\(\)\s*\)",
        "fix": "Remove count display for performance. Use .limit(N) for previewing data.",
    },
}

# === Library patterns ===
# Reference: 06-package-dependency-analysis.md

LIBRARY_PATTERNS = {
    "pip_install": {
        "severity": "MEDIUM",
        "category": "LIBRARY",
        "pyspark_pattern": r"(?m)^%pip\s+install\s+(.+)$",
        "fix": "Move to requirements.txt. See 06 resource.",
    },
    "dbutils_library": {
        "severity": "HIGH",
        "category": "LIBRARY",
        "pyspark_pattern": r"dbutils\.library\.(?:install|restartPython)",
        "fix": "Deprecated. Use %pip install or requirements.txt.",
    },
    "crealytics_excel": {
        "severity": "HIGH",
        "category": "LIBRARY",
        "pyspark_pattern": r"com\.crealytics|spark-excel|crealytics",
        "scala_pattern": r"com\.crealytics",
        "fix": "Not available on serverless. Use pandas + openpyxl. See 06 resource.",
    },
}
```

### Running the Scan

```python
def scan_notebook(content: str, cells: list[dict], path: str, primary_language: str) -> list[dict]:
    """
    Scan a notebook for migration issues.

    Args:
        content: Full notebook text
        cells: Parsed cells from split_into_cells()
        path: Migration path (A, B, C, D)
        primary_language: PYTHON, SCALA, or SQL

    Returns:
        List of finding dicts
    """
    findings = []

    # Select which pattern sets to apply based on path
    pattern_sets = []
    pattern_sets.append(("ANSI", ANSI_PATTERNS))  # All paths need ANSI checks

    if path in ("B", "C"):
        pattern_sets.append(("SERVERLESS", SERVERLESS_PATTERNS))
        pattern_sets.append(("PERFORMANCE", PERFORMANCE_PATTERNS))
        pattern_sets.append(("LIBRARY", LIBRARY_PATTERNS))
    elif path == "D":
        pattern_sets.append(("PERFORMANCE", PERFORMANCE_PATTERNS))
    elif path == "A":
        # Path A only needs ANSI + config patterns, no serverless patterns
        # But include the config-related serverless patterns
        config_patterns = {
            k: v for k, v in SERVERLESS_PATTERNS.items()
            if v.get("category") == "CONFIG"
        }
        if config_patterns:
            pattern_sets.append(("CONFIG", config_patterns))

    for set_name, patterns in pattern_sets:
        for pattern_name, pattern_def in patterns.items():
            # Select the right regex based on language and cell magic
            for cell in cells:
                cell_lang = cell.get("magic", primary_language.lower())
                if cell_lang in ("markdown", "run", "pip"):
                    continue

                pattern_key = None
                if cell_lang == "sql" or (cell_lang is None and primary_language == "SQL"):
                    pattern_key = "sql_pattern"
                elif cell_lang == "scala" or (cell_lang is None and primary_language == "SCALA"):
                    pattern_key = "scala_pattern"
                else:
                    pattern_key = "pyspark_pattern"

                regex = pattern_def.get(pattern_key)
                if not regex:
                    continue

                matches = list(re.finditer(regex, cell["content"]))
                for match in matches:
                    # Find line number within the cell
                    line_in_cell = cell["content"][:match.start()].count("\n") + 1
                    matched_text = match.group(0)[:80]

                    findings.append({
                        "pattern": pattern_name,
                        "severity": pattern_def["severity"],
                        "category": pattern_def.get("category", set_name),
                        "cell_number": cell["cell_number"],
                        "line_in_cell": line_in_cell,
                        "matched_text": matched_text,
                        "fix": pattern_def["fix"],
                        "reference": pattern_def.get("reference", ""),
                    })

    return findings


def scan_all_notebooks_for_job(metadata: dict, path: str) -> dict:
    """Scan all notebooks referenced by a job."""
    notebook_paths = extract_all_notebook_paths(metadata)
    all_findings = {}

    for nb_path in notebook_paths:
        try:
            content = export_notebook(nb_path)
            language = detect_notebook_language(nb_path)
            cells = split_into_cells(content)

            # Also check for streaming during scan
            if detect_streaming_in_notebooks(content):
                all_findings.setdefault(nb_path, []).append({
                    "pattern": "streaming_detected",
                    "severity": "CRITICAL",
                    "category": "FEASIBILITY",
                    "cell_number": 0,
                    "line_in_cell": 0,
                    "matched_text": "Structured Streaming patterns detected",
                    "fix": "Streaming is not supported on serverless general compute.",
                })

            findings = scan_notebook(content, cells, path, language)
            all_findings[nb_path] = all_findings.get(nb_path, []) + findings
            print(f"  Scanned {nb_path}: {len(findings)} findings")

        except Exception as e:
            print(f"  ERROR scanning {nb_path}: {e}")
            all_findings[nb_path] = [{
                "pattern": "scan_error",
                "severity": "CRITICAL",
                "category": "ERROR",
                "cell_number": 0,
                "line_in_cell": 0,
                "matched_text": str(e)[:80],
                "fix": "Manual review required -- notebook could not be exported or scanned.",
            }]

    return all_findings
```

### Detecting %run Dependencies

```python
def find_run_dependencies(content: str, base_path: str) -> list[str]:
    """Find all %run references in a notebook and resolve to absolute paths."""
    run_pattern = r'(?m)^%run\s+["\']?([^"\'#\n]+)'
    matches = re.findall(run_pattern, content)

    resolved = []
    for match in matches:
        match = match.strip()
        if match.startswith("/"):
            resolved.append(match)
        elif match.startswith("./") or match.startswith("../"):
            # Resolve relative path
            import os
            parent = os.path.dirname(base_path)
            resolved.append(os.path.normpath(os.path.join(parent, match)))
        else:
            # Assume relative to parent directory
            import os
            parent = os.path.dirname(base_path)
            resolved.append(os.path.normpath(os.path.join(parent, match)))

    return resolved


def get_all_notebooks_recursive(metadata: dict) -> list[str]:
    """Get all notebooks including %run dependencies (recursive)."""
    direct_paths = extract_all_notebook_paths(metadata)
    all_paths = set(direct_paths)
    to_scan = list(direct_paths)

    while to_scan:
        current = to_scan.pop(0)
        try:
            content = export_notebook(current)
            deps = find_run_dependencies(content, current)
            for dep in deps:
                if dep not in all_paths:
                    all_paths.add(dep)
                    to_scan.append(dep)
        except Exception:
            pass  # Notebook may not exist or be accessible

    return list(all_paths)
```

---

## 8. Data Compatibility Checks

Run data compatibility checks against every output table for every job. This references the data compatibility check patterns from the PLAN.md Data Compatibility Skill specification (F1-F9).

### Per-Table Assessment

```python
def assess_table(table_name: str) -> dict:
    """
    Run data compatibility checks on a single table.
    Reference: PLAN.md, Data Compatibility Skill (F1-F9)
    """
    result = {
        "table_name": table_name,
        "risk": "LOW",
        "findings": [],
    }

    try:
        # F1: Table type classification
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        table_type = "managed" if detail["location"].startswith("dbfs:/") or "managed" in str(detail["location"]).lower() else "external"
        table_format = detail["format"]

        result["table_type"] = table_type
        result["format"] = table_format

        if table_type == "external":
            result["findings"].append({
                "check": "F1_table_type",
                "severity": "MEDIUM",
                "message": (
                    f"External table. No Predictive Optimization. "
                    f"Needs explicit OPTIMIZE, VACUUM, ANALYZE before migration."
                ),
            })

        # F2: Delta protocol version
        if table_format == "delta":
            props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            props_dict = {row["key"]: row["value"] for row in props}

            min_reader = props_dict.get("delta.minReaderVersion", "1")
            min_writer = props_dict.get("delta.minWriterVersion", "2")

            if int(min_reader) < 2 or int(min_writer) < 5:
                result["findings"].append({
                    "check": "F2_protocol_version",
                    "severity": "LOW",
                    "message": (
                        f"Protocol: reader={min_reader}, writer={min_writer}. "
                        f"New DBR may auto-upgrade protocol on write. "
                        f"Verify no older readers depend on this table."
                    ),
                })

            # F3: Table properties audit
            row_tracking = props_dict.get("delta.enableRowTracking", "false")
            if row_tracking.lower() == "true":
                result["findings"].append({
                    "check": "F3_row_tracking",
                    "severity": "HIGH",
                    "message": (
                        "Row tracking enabled. Creates _metadata column that can "
                        "conflict with code referencing _metadata. Cross-check notebook code."
                    ),
                })

        # F4: Datetime column analysis
        schema = spark.table(table_name).schema
        datetime_cols = [
            f.name for f in schema.fields
            if str(f.dataType) in ("DateType", "TimestampType", "TimestampNTZType")
        ]
        if datetime_cols:
            # Sample for invalid dates
            sample_checks = []
            for col_name in datetime_cols[:5]:  # Limit to first 5
                try:
                    null_count = spark.sql(
                        f"SELECT COUNT(*) as cnt FROM {table_name} WHERE `{col_name}` IS NULL"
                    ).collect()[0]["cnt"]
                    total_count = spark.sql(
                        f"SELECT COUNT(*) as cnt FROM {table_name}"
                    ).collect()[0]["cnt"]
                    if total_count > 0 and null_count / total_count > 0.1:
                        sample_checks.append(f"{col_name} ({null_count}/{total_count} nulls)")
                except Exception:
                    pass

            if sample_checks:
                result["findings"].append({
                    "check": "F4_datetime_nulls",
                    "severity": "MEDIUM",
                    "message": f"High null rate in datetime columns: {', '.join(sample_checks)}. Check for invalid date strings in source data that may throw under ANSI mode.",
                })

        # F5: BOOLEAN column detection
        bool_cols = [f.name for f in schema.fields if str(f.dataType) == "BooleanType"]
        if bool_cols:
            result["findings"].append({
                "check": "F5_boolean_columns",
                "severity": "MEDIUM",
                "message": (
                    f"BOOLEAN columns found: {', '.join(bool_cols[:10])}. "
                    f"Check notebook code for BOOLEAN = INT comparisons (e.g., flag = 1). "
                    f"Under ANSI mode, use IS TRUE / IS FALSE."
                ),
            })

        # F8: File layout health
        if table_format == "delta":
            try:
                file_count = detail["numFiles"]
                size_bytes = detail["sizeInBytes"]
                if file_count and file_count > 0:
                    avg_file_mb = (size_bytes / file_count) / (1024 * 1024) if size_bytes else 0
                    if avg_file_mb < 32 and file_count > 100:
                        result["findings"].append({
                            "check": "F8_small_files",
                            "severity": "HIGH",
                            "message": (
                                f"Small file problem: {file_count} files, avg {avg_file_mb:.1f}MB. "
                                f"Run OPTIMIZE before migration for better serverless performance."
                            ),
                        })
                    elif file_count > 10000:
                        result["findings"].append({
                            "check": "F8_many_files",
                            "severity": "MEDIUM",
                            "message": (
                                f"High file count: {file_count} files. "
                                f"Consider OPTIMIZE and VACUUM before migration."
                            ),
                        })
            except Exception:
                pass

        # F9: Risk classification
        severities = [f["severity"] for f in result["findings"]]
        if "CRITICAL" in severities:
            result["risk"] = "CRITICAL"
        elif "HIGH" in severities:
            result["risk"] = "HIGH"
        elif "MEDIUM" in severities:
            result["risk"] = "MEDIUM"
        else:
            result["risk"] = "LOW"

    except Exception as e:
        result["risk"] = "UNKNOWN"
        result["findings"].append({
            "check": "table_access_error",
            "severity": "CRITICAL",
            "message": f"Could not access table: {e}",
        })

    return result


def assess_all_tables_for_job(metadata: dict) -> dict:
    """
    Identify and assess all output tables for a job.

    NOTE: Output table detection requires scanning notebook code for
    write operations (.write, .save, INSERT INTO, MERGE INTO, CREATE TABLE AS).
    """
    all_findings = {}

    # Detect output tables from notebook code
    output_tables = detect_output_tables(metadata)

    for table_name in output_tables:
        try:
            result = assess_table(table_name)
            all_findings[table_name] = result
            risk = result["risk"]
            finding_count = len(result["findings"])
            print(f"  {table_name}: {risk} risk ({finding_count} findings)")
        except Exception as e:
            print(f"  ERROR assessing {table_name}: {e}")
            all_findings[table_name] = {
                "table_name": table_name,
                "risk": "UNKNOWN",
                "findings": [{"check": "error", "severity": "CRITICAL", "message": str(e)}],
            }

    return all_findings


def detect_output_tables(metadata: dict) -> list[str]:
    """Scan notebook code to find all tables written by the job."""
    output_tables = set()

    notebook_paths = extract_all_notebook_paths(metadata)
    for nb_path in notebook_paths:
        try:
            content = export_notebook(nb_path)

            # SQL write patterns
            sql_write_patterns = [
                r"(?i)INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([`\w]+\.[`\w]+(?:\.[`\w]+)?)",
                r"(?i)MERGE\s+INTO\s+([`\w]+\.[`\w]+(?:\.[`\w]+)?)",
                r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\w]+\.[`\w]+(?:\.[`\w]+)?)\s+(?:AS|USING|\()",
                r"(?i)CREATE\s+TABLE\s+([`\w]+\.[`\w]+(?:\.[`\w]+)?)\s+AS",
            ]

            # PySpark write patterns
            pyspark_write_patterns = [
                r'\.saveAsTable\(\s*["\']([^"\']+)',
                r'\.insertInto\(\s*["\']([^"\']+)',
                r'\.save\(\s*["\']([^"\']+)',
                r'\.format\(["\']delta["\']\).*?\.save\(\s*["\']([^"\']+)',
            ]

            for pattern in sql_write_patterns + pyspark_write_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    table = match.replace("`", "").strip()
                    # Skip temp tables and CTEs
                    if not table.startswith("tmp_") and "temp" not in table.lower():
                        output_tables.add(table)

        except Exception:
            pass

    return list(output_tables)
```

---

## 9. Two-Track Classification

Every finding must be classified as Databricks-side or Repo-side so work can be assigned to the correct track.

### Classification Rules

| Finding Type | Track | Who Fixes | Examples |
|-------------|-------|-----------|----------|
| ANSI code fix | **Databricks-side** | Genie Code | TRY_CAST, null guards, IS TRUE |
| Unsupported operation removal | **Databricks-side** | Genie Code | .persist(), REFRESH TABLE, MSCK REPAIR |
| Spark config in notebook | **Databricks-side** | Genie Code | spark.conf.set() calls |
| Environment variable migration | **Databricks-side** | Genie Code | os.environ → dbutils.widgets |
| Package migration | **Databricks-side** | Genie Code | %pip install → requirements.txt |
| Table metadata issues | **Databricks-side** | Genie Code / Manual | OPTIMIZE, ANALYZE, row tracking |
| Job JSON transformation | **Repo-side** | DevOps / Script | Remove job_clusters, add environments block |
| CI/CD variable additions | **Repo-side** | DevOps | Add env_name, requirements path |
| PowerShell script updates | **Repo-side** | DevOps | %env_name% replacement |
| Init script removal | **Both** | DevOps + Genie Code | Remove from JSON + move deps to requirements.txt |
| Library migration | **Both** | DevOps + Genie Code | Remove from cluster spec + add to requirements.txt |

### Classification Code

```python
def classify_finding_track(finding: dict) -> str:
    """Classify a finding as 'databricks', 'repo', or 'both'."""
    category = finding.get("category", "")
    pattern = finding.get("pattern", "")

    # Repo-side patterns
    repo_patterns = {
        "job_json_transform", "cicd_variable", "powershell_fix",
        "deployment_script", "pipeline_variable",
    }

    # Both-side patterns
    both_patterns = {
        "init_script", "library_migration", "jar_dependency",
    }

    if pattern in repo_patterns:
        return "repo"
    elif pattern in both_patterns:
        return "both"
    else:
        # Default: most code-level findings are Databricks-side
        return "databricks"


def add_repo_findings(metadata: dict, classification: dict) -> list[dict]:
    """Generate repo-side findings based on job config analysis."""
    findings = []
    prescribed = metadata.get("prescribed_path", "").upper()

    if prescribed in ("B", "C", "D"):
        # Job JSON needs serverless transformation
        if metadata.get("job_clusters"):
            findings.append({
                "pattern": "job_json_transform",
                "severity": "CRITICAL",
                "category": "REPO",
                "track": "repo",
                "message": (
                    "Job JSON needs serverless transformation: "
                    "remove job_clusters block, add environments block with "
                    'client: "4" and requirements.txt Volume path, '
                    "replace job_cluster_key with environment_key on each task."
                ),
                "reference": "03-pyspark-sql-13.3-to-pyspark-sql-serverless.md, Section 7",
            })

        # CI/CD variable for env_name
        findings.append({
            "pattern": "cicd_variable",
            "severity": "HIGH",
            "category": "REPO",
            "track": "repo",
            "message": (
                "CI/CD variable 'env_name' must be added for environment-based "
                "substitution in the environments block (requirements.txt path uses %env%)."
            ),
        })

        # PowerShell %env_name% replacement
        findings.append({
            "pattern": "powershell_fix",
            "severity": "HIGH",
            "category": "REPO",
            "track": "repo",
            "message": (
                "PowerShell deployment script needs %env_name% replacement handling "
                "for the serverless environments block."
            ),
        })

    elif prescribed == "A":
        # Path A: only DBR version update in job JSON
        findings.append({
            "pattern": "job_json_transform",
            "severity": "HIGH",
            "category": "REPO",
            "track": "repo",
            "message": (
                "Job JSON needs spark_version updated from 13.3.x to 16.4.x "
                "in the job_clusters/new_cluster block."
            ),
        })

    # Init scripts to remove from JSON
    if classification.get("has_init_scripts") and prescribed in ("B", "C", "D"):
        findings.append({
            "pattern": "init_script",
            "severity": "HIGH",
            "category": "REPO",
            "track": "both",
            "message": (
                "Init scripts must be removed from job JSON and dependencies "
                "migrated to requirements.txt (Databricks-side)."
            ),
        })

    return findings
```

---

## 10. Per-Job Assessment Report

Generate a structured report for each job in the batch.

### Report Template

```
JOB ASSESSMENT: {job_name} ({job_id})
═══════════════════════════════════════════════════
Prescribed Path: {prescribed_path} ({path_description})
Recommended Path: {recommended_path} ({recommendation_reason})
Feasibility: {PASS|WARN|BLOCK}
Priority: {priority}
Tower Lead: {tower_lead}
Notebooks: {notebook_count}
Output Tables: {table_count}

DATABRICKS-SIDE FINDINGS:
  CRITICAL: ({count})
    - {description} (cell {N}, line {N}) → {fix}
    - {description} (cell {N}, line {N}) → {fix}
  HIGH: ({count})
    - {description} (cell {N}, line {N}) → {fix}
  MEDIUM: ({count})
    - {description} (cell {N}, line {N}) → {fix}
  LOW: ({count})
    - {description} (cell {N}, line {N}) → {fix}

REPO-SIDE FINDINGS:
  CRITICAL: ({count})
    - {description}
  HIGH: ({count})
    - {description}

TABLE FINDINGS:
  {table_name}: {risk} risk ({details})
  {table_name}: {risk} risk ({details})

EFFORT ESTIMATE: {LOW|MEDIUM|HIGH}
  Databricks-side: ~{hours} hours
  Repo-side: ~{hours} hours
```

### Report Generation Code

```python
PATH_DESCRIPTIONS = {
    "A": "Scala 13.3 → Scala 16.4 (DBR upgrade only)",
    "B": "Scala 13.3 → PySpark Serverless (language + compute conversion)",
    "C": "PySpark/SQL 13.3 → PySpark/SQL Serverless (compute migration)",
    "D": "SQL-only → DBSQL Serverless SQL notebooks",
}


def estimate_effort(db_findings: list[dict], repo_findings: list[dict], table_findings: dict) -> dict:
    """Estimate effort based on findings."""
    # Databricks-side: based on finding severity and count
    db_hours = 0
    for f in db_findings:
        if f["severity"] == "CRITICAL":
            db_hours += 0.5
        elif f["severity"] == "HIGH":
            db_hours += 0.25
        elif f["severity"] == "MEDIUM":
            db_hours += 0.1
        else:
            db_hours += 0.05

    # Repo-side: based on whether JSON transform is needed
    repo_hours = 0
    for f in repo_findings:
        if f["severity"] == "CRITICAL":
            repo_hours += 0.5
        elif f["severity"] == "HIGH":
            repo_hours += 0.25
        else:
            repo_hours += 0.1

    # Table prep: based on table risk
    table_hours = 0
    for table_name, assessment in table_findings.items():
        risk = assessment.get("risk", "LOW")
        if risk == "CRITICAL":
            table_hours += 1.0
        elif risk == "HIGH":
            table_hours += 0.5
        elif risk == "MEDIUM":
            table_hours += 0.25

    total = db_hours + repo_hours + table_hours

    if total < 1:
        category = "LOW"
    elif total < 4:
        category = "MEDIUM"
    else:
        category = "HIGH"

    return {
        "category": category,
        "total_hours": round(total, 1),
        "databricks_hours": round(db_hours, 1),
        "repo_hours": round(repo_hours, 1),
        "table_hours": round(table_hours, 1),
    }


def generate_job_report(
    metadata: dict,
    classification: dict,
    routing: dict,
    feasibility: list[dict],
    notebook_findings: dict,
    table_findings: dict,
    repo_findings: list[dict],
) -> str:
    """Generate the per-job assessment report."""
    job_id = metadata.get("job_id", "unknown")
    job_name = metadata.get("job_name", "unknown")
    prescribed = metadata.get("prescribed_path", "?")
    recommended = routing.get("recommended_path", prescribed)
    priority = metadata.get("priority", "MEDIUM")
    tower_lead = metadata.get("tower_lead", "unassigned")

    # Feasibility status
    if any(f["status"] == "BLOCK" for f in feasibility):
        feas_status = "BLOCK"
    elif any(f["status"] == "WARN" for f in feasibility):
        feas_status = "WARN"
    else:
        feas_status = "PASS"

    # Flatten all Databricks-side findings
    db_findings = []
    for nb_path, findings in notebook_findings.items():
        for f in findings:
            f["notebook_path"] = nb_path
            f["track"] = classify_finding_track(f)
            if f["track"] in ("databricks", "both"):
                db_findings.append(f)

    # Group by severity
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    notebook_count = len(notebook_findings)
    table_count = len(table_findings)

    lines = []
    lines.append(f"JOB ASSESSMENT: {job_name} ({job_id})")
    lines.append("=" * 60)
    lines.append(f"Prescribed Path: {prescribed} ({PATH_DESCRIPTIONS.get(prescribed, 'Unknown')})")
    if routing.get("override"):
        lines.append(f"Recommended Path: {recommended} ({PATH_DESCRIPTIONS.get(recommended, 'Unknown')})")
        lines.append(f"  ** OVERRIDE: {routing.get('override_reason', '')}")
    else:
        lines.append(f"Recommended Path: {recommended} (confirmed)")
    lines.append(f"Feasibility: {feas_status}")
    if feas_status != "PASS":
        for f in feasibility:
            if f["status"] in ("BLOCK", "WARN"):
                lines.append(f"  {f['status']}: {f['message']}")
    lines.append(f"Priority: {priority}")
    lines.append(f"Tower Lead: {tower_lead}")
    lines.append(f"Notebooks: {notebook_count}")
    lines.append(f"Output Tables: {table_count}")
    lines.append("")

    # Databricks-side findings
    lines.append("DATABRICKS-SIDE FINDINGS:")
    for severity in severity_order:
        sev_findings = [f for f in db_findings if f["severity"] == severity]
        if sev_findings:
            lines.append(f"  {severity}: ({len(sev_findings)})")
            for f in sev_findings:
                nb = f.get("notebook_path", "").split("/")[-1]
                cell = f.get("cell_number", "?")
                line = f.get("line_in_cell", "?")
                matched = f.get("matched_text", "")[:50]
                fix = f.get("fix", "")
                lines.append(f"    - [{nb}] {f['pattern']}: {matched}")
                lines.append(f"      Cell {cell}, line {line} -> {fix}")
    if not db_findings:
        lines.append("  None -- clean code!")
    lines.append("")

    # Repo-side findings
    lines.append("REPO-SIDE FINDINGS:")
    for severity in severity_order:
        sev_findings = [f for f in repo_findings if f["severity"] == severity]
        if sev_findings:
            lines.append(f"  {severity}: ({len(sev_findings)})")
            for f in sev_findings:
                lines.append(f"    - {f['message']}")
    if not repo_findings:
        lines.append("  None")
    lines.append("")

    # Table findings
    lines.append("TABLE FINDINGS:")
    if table_findings:
        for table_name, assessment in sorted(table_findings.items()):
            risk = assessment.get("risk", "UNKNOWN")
            finding_msgs = [f.get("message", "") for f in assessment.get("findings", [])]
            detail = "; ".join(finding_msgs[:3]) if finding_msgs else "no issues"
            lines.append(f"  {table_name}: {risk} risk ({detail})")
    else:
        lines.append("  No output tables detected -- verify manually")
    lines.append("")

    # Effort estimate
    effort = estimate_effort(db_findings, repo_findings, table_findings)
    lines.append(f"EFFORT ESTIMATE: {effort['category']} (~{effort['total_hours']} hours total)")
    lines.append(f"  Databricks-side: ~{effort['databricks_hours']} hours")
    lines.append(f"  Repo-side: ~{effort['repo_hours']} hours")
    lines.append(f"  Table prep: ~{effort['table_hours']} hours")

    return "\n".join(lines)
```

### Example Per-Job Report Output

```
JOB ASSESSMENT: sample_claims_pipeline (545285009490448)
============================================================
Prescribed Path: C (PySpark/SQL 13.3 → PySpark/SQL Serverless (compute migration))
Recommended Path: C (confirmed)
Feasibility: PASS
Priority: HIGH
Tower Lead: Team Lead A
Notebooks: 4
Output Tables: 3

DATABRICKS-SIDE FINDINGS:
  CRITICAL: (3)
    - [01_bronze] CAST_to_type: CAST(claim_amount AS INT)
      Cell 12, line 5 -> Use TRY_CAST (SQL), add when/otherwise null guard (PySpark)
    - [02_silver] CAST_to_type: CAST(date_str AS TIMESTAMP)
      Cell 45, line 3 -> Use TRY_CAST (SQL), add when/otherwise null guard (PySpark)
    - [01_bronze] msck_repair: MSCK REPAIR TABLE claims_raw
      Cell 23, line 1 -> Remove. Not needed with Delta tables in Unity Catalog.
  HIGH: (2)
    - [02_silver] spark_conf_set_ansi: spark.conf.set("spark.sql.ansi.enabled", "false"
      Cell 3, line 1 -> Remove. ANSI mode is mandatory on serverless. Fix code to be ANSI-safe instead.
    - [03_gold] env_var_access: os.environ.get("ENV_NAME")
      Cell 2, line 4 -> Replace with dbutils.widgets.get(). See 03 resource, Section 6.
  MEDIUM: (2)
    - [02_silver] persist_cache: .persist()
      Cell 67, line 1 -> Remove. Serverless manages memory automatically.
    - [03_gold] boolean_int_comparison: is_active = 1
      Cell 15, line 8 -> Use IS TRUE / IS FALSE (SQL) or == True / == False (PySpark)
  LOW: (1)
    - [03_gold] unnecessary_count: display(df.count())
      Cell 20, line 1 -> Use .first() is not None instead of .count() > 0. Remove display(df.count()).

REPO-SIDE FINDINGS:
  CRITICAL: (1)
    - Job JSON needs serverless transformation: remove job_clusters block, add environments block with client: "4" and requirements.txt Volume path, replace job_cluster_key with environment_key on each task.
  HIGH: (2)
    - CI/CD variable 'env_name' must be added for environment-based substitution in the environments block (requirements.txt path uses %env%).
    - PowerShell deployment script needs %env_name% replacement handling for the serverless environments block.

TABLE FINDINGS:
  claims_bronze: LOW risk (no issues)
  claims_silver: MEDIUM risk (BOOLEAN columns found: is_active, is_primary. Check notebook code for BOOLEAN = INT comparisons.)
  claims_gold: HIGH risk (Small file problem: 2847 files, avg 12.3MB. Run OPTIMIZE before migration for better serverless performance.)

EFFORT ESTIMATE: MEDIUM (~2.8 hours total)
  Databricks-side: ~1.5 hours
  Repo-side: ~0.8 hours
  Table prep: ~0.5 hours
```

---

## 11. Batch Summary Report

Aggregate all per-job reports into a batch-level summary.

### Summary Template

```
BATCH SUMMARY: {batch_name} ({total_jobs} jobs)
═══════════════════════════════════════════════════
Assessment Date: {date}
Created By: {assessor}

PATH DISTRIBUTION:
  Path A (Scala → 16.4):         {count} jobs
  Path B (Scala → PySpark SL):   {count} jobs
  Path C (PySpark → Serverless):  {count} jobs
  Path D (SQL → DBSQL):          {count} jobs

FEASIBILITY:
  PASS:  {count} jobs
  WARN:  {count} jobs (proceed with caution)
  BLOCK: {count} jobs (cannot proceed as prescribed)

FINDING DISTRIBUTION:
  CRITICAL: {count} findings across {job_count} jobs
  HIGH:     {count} findings across {job_count} jobs
  MEDIUM:   {count} findings across {job_count} jobs
  LOW:      {count} findings across {job_count} jobs

MOST COMMON ISSUES (top 10):
  1. {pattern_name} ({count} occurrences across {job_count} jobs)
  2. {pattern_name} ({count} occurrences across {job_count} jobs)
  ...

EFFORT ESTIMATE:
  Low effort (< 1 hour):   {count} jobs
  Medium effort (1-4 hrs): {count} jobs
  High effort (> 4 hrs):   {count} jobs

  Total estimated hours:   {total_hours}
  Databricks-side hours:   {db_hours}
  Repo-side hours:         {repo_hours}
  Table prep hours:        {table_hours}

REPO CHANGE MANIFEST:
  Job JSONs needing transformation:     {count}
  CI/CD variables to add:               env_name (all {serverless_count} serverless jobs)
  PowerShell script fix needed:         %env_name% replacement
  Init scripts to remove:               {count}

PRIORITY BREAKDOWN:
  HIGH priority:   {count} jobs ({list})
  MEDIUM priority: {count} jobs ({list})
  LOW priority:    {count} jobs ({list})

BLOCKED JOBS (require manual resolution):
  {job_name}: {block_reason}
  {job_name}: {block_reason}
```

### Summary Generation Code

```python
from collections import Counter
from datetime import datetime


def generate_batch_summary(
    batch_name: str,
    job_reports: dict,
    all_metadata: dict,
    all_classifications: dict,
    all_routings: dict,
    all_feasibility: dict,
    all_notebook_findings: dict,
    all_table_findings: dict,
    all_repo_findings: dict,
) -> str:
    """Generate the batch summary report."""
    total_jobs = len(job_reports)

    # Path distribution (use recommended, not prescribed)
    path_counts = Counter()
    for job_id, routing in all_routings.items():
        path_counts[routing.get("recommended_path", "?")] += 1

    # Feasibility
    feas_counts = Counter()
    blocked_jobs = []
    for job_id, findings in all_feasibility.items():
        if any(f["status"] == "BLOCK" for f in findings):
            feas_counts["BLOCK"] += 1
            for f in findings:
                if f["status"] == "BLOCK":
                    job_name = all_metadata.get(job_id, {}).get("job_name", job_id)
                    blocked_jobs.append((job_name, f["message"]))
        elif any(f["status"] == "WARN" for f in findings):
            feas_counts["WARN"] += 1
        else:
            feas_counts["PASS"] += 1

    # Finding distribution
    severity_counts = Counter()
    severity_job_counts = {s: set() for s in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]}
    pattern_counts = Counter()
    pattern_job_counts = {}

    for job_id, nb_findings in all_notebook_findings.items():
        for nb_path, findings in nb_findings.items():
            for f in findings:
                sev = f.get("severity", "LOW")
                severity_counts[sev] += 1
                severity_job_counts[sev].add(job_id)

                pat = f.get("pattern", "unknown")
                pattern_counts[pat] += 1
                pattern_job_counts.setdefault(pat, set()).add(job_id)

    # Add repo findings to counts
    for job_id, findings in all_repo_findings.items():
        for f in findings:
            sev = f.get("severity", "LOW")
            severity_counts[sev] += 1
            severity_job_counts[sev].add(job_id)

    # Effort distribution
    effort_dist = Counter()
    total_hours = 0
    total_db_hours = 0
    total_repo_hours = 0
    total_table_hours = 0

    for job_id in job_reports:
        db_findings_flat = []
        for nb_path, findings in all_notebook_findings.get(job_id, {}).items():
            db_findings_flat.extend(findings)
        repo = all_repo_findings.get(job_id, [])
        tables = all_table_findings.get(job_id, {})

        effort = estimate_effort(db_findings_flat, repo, tables)
        effort_dist[effort["category"]] += 1
        total_hours += effort["total_hours"]
        total_db_hours += effort["databricks_hours"]
        total_repo_hours += effort["repo_hours"]
        total_table_hours += effort["table_hours"]

    # Repo change manifest
    serverless_count = path_counts.get("B", 0) + path_counts.get("C", 0) + path_counts.get("D", 0)
    json_transform_count = sum(
        1 for job_id, findings in all_repo_findings.items()
        if any(f.get("pattern") == "job_json_transform" for f in findings)
    )
    init_script_count = sum(
        1 for job_id, cls in all_classifications.items()
        if cls.get("has_init_scripts")
    )

    # Priority breakdown
    priority_groups = {"HIGH": [], "MEDIUM": [], "LOW": []}
    for job_id, metadata in all_metadata.items():
        pri = metadata.get("priority", "MEDIUM")
        job_name = metadata.get("job_name", job_id)
        priority_groups.setdefault(pri, []).append(job_name)

    # Build the summary
    lines = []
    lines.append(f"BATCH SUMMARY: {batch_name} ({total_jobs} jobs)")
    lines.append("=" * 60)
    lines.append(f"Assessment Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    lines.append("PATH DISTRIBUTION:")
    lines.append(f"  Path A (Scala -> 16.4):          {path_counts.get('A', 0)} jobs")
    lines.append(f"  Path B (Scala -> PySpark SL):    {path_counts.get('B', 0)} jobs")
    lines.append(f"  Path C (PySpark -> Serverless):   {path_counts.get('C', 0)} jobs")
    lines.append(f"  Path D (SQL -> DBSQL):           {path_counts.get('D', 0)} jobs")
    lines.append("")

    lines.append("FEASIBILITY:")
    lines.append(f"  PASS:  {feas_counts.get('PASS', 0)} jobs")
    lines.append(f"  WARN:  {feas_counts.get('WARN', 0)} jobs (proceed with caution)")
    lines.append(f"  BLOCK: {feas_counts.get('BLOCK', 0)} jobs (cannot proceed as prescribed)")
    lines.append("")

    lines.append("FINDING DISTRIBUTION:")
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = severity_counts.get(sev, 0)
        job_count = len(severity_job_counts.get(sev, set()))
        lines.append(f"  {sev:8s}: {count} findings across {job_count} jobs")
    lines.append("")

    lines.append("MOST COMMON ISSUES (top 10):")
    for i, (pattern, count) in enumerate(pattern_counts.most_common(10), 1):
        job_count = len(pattern_job_counts.get(pattern, set()))
        lines.append(f"  {i:2d}. {pattern} ({count} occurrences across {job_count} jobs)")
    lines.append("")

    lines.append("EFFORT ESTIMATE:")
    lines.append(f"  Low effort (< 1 hour):   {effort_dist.get('LOW', 0)} jobs")
    lines.append(f"  Medium effort (1-4 hrs): {effort_dist.get('MEDIUM', 0)} jobs")
    lines.append(f"  High effort (> 4 hrs):   {effort_dist.get('HIGH', 0)} jobs")
    lines.append("")
    lines.append(f"  Total estimated hours:   {round(total_hours, 1)}")
    lines.append(f"  Databricks-side hours:   {round(total_db_hours, 1)}")
    lines.append(f"  Repo-side hours:         {round(total_repo_hours, 1)}")
    lines.append(f"  Table prep hours:        {round(total_table_hours, 1)}")
    lines.append("")

    lines.append("REPO CHANGE MANIFEST:")
    lines.append(f"  Job JSONs needing transformation:     {json_transform_count}")
    lines.append(f"  CI/CD variables to add:               env_name (all {serverless_count} serverless jobs)")
    lines.append(f"  PowerShell script fix needed:         %env_name% replacement")
    lines.append(f"  Init scripts to remove:               {init_script_count}")
    lines.append("")

    lines.append("PRIORITY BREAKDOWN:")
    for pri in ["HIGH", "MEDIUM", "LOW"]:
        jobs = priority_groups.get(pri, [])
        if jobs:
            job_list = ", ".join(jobs[:5])
            if len(jobs) > 5:
                job_list += f", ... (+{len(jobs) - 5} more)"
            lines.append(f"  {pri} priority: {len(jobs)} jobs ({job_list})")
    lines.append("")

    if blocked_jobs:
        lines.append("BLOCKED JOBS (require manual resolution):")
        for job_name, reason in blocked_jobs:
            lines.append(f"  {job_name}: {reason[:100]}")
    else:
        lines.append("BLOCKED JOBS: None -- all jobs can proceed")

    return "\n".join(lines)
```

### Example Batch Summary Output

```
BATCH SUMMARY: Claims Tower - Sprint 1 (25 jobs)
============================================================
Assessment Date: 2026-04-16 14:30

PATH DISTRIBUTION:
  Path A (Scala -> 16.4):          3 jobs
  Path B (Scala -> PySpark SL):    2 jobs
  Path C (PySpark -> Serverless):   18 jobs
  Path D (SQL -> DBSQL):           2 jobs

FEASIBILITY:
  PASS:  21 jobs
  WARN:  3 jobs (proceed with caution)
  BLOCK: 1 jobs (cannot proceed as prescribed)

FINDING DISTRIBUTION:
  CRITICAL : 12 findings across 8 jobs
  HIGH     : 34 findings across 15 jobs
  MEDIUM   : 67 findings across 22 jobs
  LOW      : 89 findings across 25 jobs

MOST COMMON ISSUES (top 10):
   1. CAST_to_type (18 occurrences across 14 jobs)
   2. unsupported_spark_configs (15 occurrences across 12 jobs)
   3. refresh_table (12 occurrences across 9 jobs)
   4. persist_cache (10 occurrences across 8 jobs)
   5. boolean_int_comparison (8 occurrences across 6 jobs)
   6. env_var_access (7 occurrences across 5 jobs)
   7. to_timestamp_invalid (6 occurrences across 4 jobs)
   8. pip_install (5 occurrences across 5 jobs)
   9. unnecessary_count (5 occurrences across 4 jobs)
  10. division_by_zero (4 occurrences across 3 jobs)

EFFORT ESTIMATE:
  Low effort (< 1 hour):   10 jobs
  Medium effort (1-4 hrs): 12 jobs
  High effort (> 4 hrs):   3 jobs

  Total estimated hours:   52.5
  Databricks-side hours:   31.2
  Repo-side hours:         14.8
  Table prep hours:        6.5

REPO CHANGE MANIFEST:
  Job JSONs needing transformation:     22
  CI/CD variables to add:               env_name (all 22 serverless jobs)
  PowerShell script fix needed:         %env_name% replacement
  Init scripts to remove:               4

PRIORITY BREAKDOWN:
  HIGH priority: 12 jobs (sample_claims_pipeline, sample_pharmacy_etl, sample_streaming_ingest, sample_scala_scoring, wf_claims_adjudication, ... (+7 more))
  MEDIUM priority: 10 jobs (sample_scala_refresh, sample_sql_report, sample_gpu_ml_job, wf_auth_daily_refresh, wf_referral_tracking, ... (+5 more))
  LOW priority: 3 jobs (sample_provider_build, wf_archive_claims_quarterly, wf_reporting_snapshot)

BLOCKED JOBS (require manual resolution):
  sample_streaming_ingest: Streaming job prescribed for serverless. Structured Streaming is not supported on serverless general compute. Must stay on classic c
```

---

## 12. Genie Code Recommendation Engine

When Genie Code assesses a job, it should go beyond flagging issues and actively recommend the migration approach.

### Recommendation Categories

For each job, Genie Code produces recommendations in six areas:

#### 12.1 Path Recommendation

Even if the customer prescribed a path, Genie Code may recommend a different one based on the actual job characteristics:

```python
def recommend_path(metadata: dict, classification: dict, notebook_findings: dict) -> dict:
    """
    Produce a path recommendation with justification.
    May agree with or override the customer's prescribed path.
    """
    prescribed = metadata.get("prescribed_path", "").upper()
    routing = route_job(metadata, classification, {})  # Uses routing logic from Section 6

    recommendation = {
        "prescribed": prescribed,
        "recommended": routing["recommended_path"],
        "agrees": prescribed == routing["recommended_path"],
        "confidence": routing["confidence"],
        "justification": routing["override_reason"],
    }

    # Additional recommendation: if all cells are SQL, suggest Path D
    total_cells = 0
    sql_cells = 0
    for nb_path, findings in notebook_findings.items():
        # This would need the cells data -- simplified here
        pass

    return recommendation
```

#### 12.2 Specific Code Changes

For each finding, provide the exact code change with cell and line reference:

```python
def recommend_code_changes(notebook_findings: dict) -> list[dict]:
    """Produce specific code change recommendations per finding."""
    changes = []
    for nb_path, findings in notebook_findings.items():
        for f in findings:
            change = {
                "notebook": nb_path,
                "cell": f.get("cell_number"),
                "line": f.get("line_in_cell"),
                "current_code": f.get("matched_text", ""),
                "pattern": f.get("pattern"),
                "severity": f.get("severity"),
                "fix_description": f.get("fix"),
                "reference": f.get("reference", ""),
            }

            # Generate specific replacement based on pattern
            if f["pattern"] == "CAST_to_type":
                change["replacement_hint"] = "Replace CAST(...) with TRY_CAST(...) in SQL, or add when/otherwise guard in PySpark"
            elif f["pattern"] == "persist_cache":
                change["replacement_hint"] = "Delete this line entirely. Serverless manages caching."
            elif f["pattern"] == "msck_repair":
                change["replacement_hint"] = "Delete this line entirely. Not needed with Delta/UC."
            elif f["pattern"] == "spark_conf_set_ansi":
                change["replacement_hint"] = "Delete this line. Fix downstream ANSI issues instead."
            elif f["pattern"] == "env_var_access":
                change["replacement_hint"] = 'Replace os.environ.get("VAR") with dbutils.widgets.get("VAR")'
            elif f["pattern"] == "refresh_table":
                change["replacement_hint"] = "Delete this line. Serverless auto-invalidates catalog cache."
            elif f["pattern"] == "to_timestamp_invalid":
                change["replacement_hint"] = "Replace to_timestamp() with try_to_timestamp(), to_date() with try_to_date()"
            elif f["pattern"] == "rdd_api":
                change["replacement_hint"] = "Rewrite as DataFrame operation. sc.parallelize(list) -> spark.createDataFrame(list, schema)"
            elif f["pattern"] == "boolean_int_comparison":
                change["replacement_hint"] = "Replace col = 1 with col IS TRUE, col = 0 with col IS FALSE"
            elif f["pattern"] == "global_temp_view":
                change["replacement_hint"] = "Replace createGlobalTempView with createOrReplaceTempView"

            changes.append(change)

    return changes
```

#### 12.3 Package Replacements

```python
# Reference: 06-package-dependency-analysis.md
PACKAGE_REPLACEMENTS = {
    "com.crealytics.spark.excel": {
        "replacement": "openpyxl + pandas",
        "note": "Read Excel via pandas, convert to Spark DataFrame",
        "reference": "06-package-dependency-analysis.md",
    },
    "spark-xml": {
        "replacement": "lxml + pandas or spark.read.format('xml')",
        "note": "Native XML reader available in newer DBR",
    },
    "koalas": {
        "replacement": "pyspark.pandas",
        "note": "Koalas is now pyspark.pandas (built-in since DBR 10+)",
    },
    "databricks-connect": {
        "replacement": "databricks-connect (updated version)",
        "note": "Must use version matching target DBR",
    },
}
```

#### 12.4 Config Removals

```python
# Reference: 05-spark-config-classic-to-serverless.md
CONFIGS_TO_REMOVE_SERVERLESS = [
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.driver.memory",
    "spark.driver.cores",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.shuffle.service.enabled",
    "spark.sql.ansi.enabled",  # Cannot set to false on serverless
    "spark.databricks.cluster.profile",
    "spark.master",
]

CONFIGS_TO_REVIEW_SERVERLESS = [
    "spark.sql.shuffle.partitions",  # Serverless auto-tunes; usually remove
    "spark.sql.autoBroadcastJoinThreshold",  # Serverless manages; usually remove
    "spark.sql.files.maxPartitionBytes",  # Serverless manages; usually remove
]
```

#### 12.5 Performance Optimizations

```python
# Reference: 09-performance-optimization-patterns.md
PERFORMANCE_RECOMMENDATIONS = {
    "remove_persist_cache": {
        "condition": "Any .persist() or .cache() call",
        "action": "Remove. Serverless manages memory and disk spilling automatically.",
        "reference": "09-performance-optimization-patterns.md",
    },
    "remove_manual_partitioning": {
        "condition": ".repartition(N) with hardcoded N",
        "action": "Remove or use .repartition(col) for data locality. Serverless auto-tunes partition counts.",
    },
    "replace_count_with_first": {
        "condition": ".count() > 0 for existence checks",
        "action": "Replace with .first() is not None or .limit(1).count() > 0",
    },
    "add_optimize": {
        "condition": "Tables with many small files",
        "action": "Run OPTIMIZE on output tables before migration cutover.",
    },
    "liquid_clustering": {
        "condition": "Tables using ZORDER",
        "action": "Consider migrating to Liquid Clustering for better serverless performance. Not required for migration.",
    },
}
```

#### 12.6 Pre-Migration Table Actions

```python
def recommend_table_actions(table_findings: dict) -> list[dict]:
    """Recommend pre-migration actions based on table assessment."""
    actions = []

    for table_name, assessment in table_findings.items():
        for finding in assessment.get("findings", []):
            check = finding.get("check", "")

            if check == "F8_small_files":
                actions.append({
                    "table": table_name,
                    "action": f"OPTIMIZE {table_name}",
                    "reason": "Small file problem detected. Run OPTIMIZE before migration.",
                    "priority": "HIGH",
                    "when": "BEFORE migration",
                })
                actions.append({
                    "table": table_name,
                    "action": f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS",
                    "reason": "Update statistics for query optimizer on serverless.",
                    "priority": "MEDIUM",
                    "when": "BEFORE migration",
                })

            elif check == "F1_table_type" and "External" in finding.get("message", ""):
                actions.append({
                    "table": table_name,
                    "action": f"OPTIMIZE {table_name}",
                    "reason": "External table -- no Predictive Optimization. Explicit OPTIMIZE needed.",
                    "priority": "MEDIUM",
                    "when": "BEFORE migration",
                })

            elif check == "F3_row_tracking":
                actions.append({
                    "table": table_name,
                    "action": "Cross-check notebook code for _metadata column references",
                    "reason": "Row tracking creates _metadata column that may conflict with code.",
                    "priority": "HIGH",
                    "when": "DURING assessment",
                })

    return actions
```

---

## 13. Workflow Phases and Cross-References

The batch assessment workflow is Phase 1 of a three-phase migration process.

### Phase 1: Assessment (This Document)

- Input: Manifest of jobs to migrate
- Process: Extract configs, validate feasibility, route paths, scan code, assess tables
- Output: Per-job reports, batch summary, effort estimates, repo change manifest
- Tools: Jobs API, Workspace API, Spark SQL (for table checks)

### Phase 2: Execution (Apply Changes)

After assessment reports are reviewed and approved, apply the changes per the path-specific scenario guide:

| Path | Execution Guide | Key Actions |
|------|----------------|-------------|
| **A** | `01-scala-13.3-to-scala-16.4.md` | ANSI fixes, deprecated API updates, spark config changes |
| **B** | `02-scala-13.3-to-pyspark-serverless.md` | Scala-to-PySpark conversion + all Path C changes |
| **C** | `03-pyspark-sql-13.3-to-pyspark-sql-serverless.md` | ANSI fixes, serverless restrictions, env var migration, job JSON transform |
| **D** | `04-sql-to-dbsql-serverless.md` | SQL rewriting for DBSQL, warehouse configuration |

Supporting resources applied during execution:

| Resource | When Used |
|----------|-----------|
| `05-spark-config-classic-to-serverless.md` | Paths B, C -- config migration |
| `06-package-dependency-analysis.md` | Paths B, C -- dependency migration |
| `07-ansi-compliance-reference.md` | All paths -- ANSI code fixes |
| `09-performance-optimization-patterns.md` | Paths B, C, D -- performance tuning |
| `10-ml-runtime-migration.md` | Jobs with ML runtime |
| `11-archiving-workflow.md` | All paths -- archive before modification |

### Phase 3: Validation

After execution, validate every migrated job:

| Validation Step | Resource/Skill |
|----------------|----------------|
| Archive originals | `11-archiving-workflow.md` |
| Run validation checks | `08-testing-validation-framework.md` |
| 14-check comparison | `conversion_validator` skill |
| Conversion report | `conversion_report` skill |

### Handoff Between Phases

```
PHASE 1 OUTPUT                    PHASE 2 INPUT
═══════════════                   ═════════════
Per-job report        ────────►   Genie Code uses report findings
  with cell/line refs              to apply specific fixes

Batch summary         ────────►   Tower leads review and approve
  with effort estimates            execution plan

Repo change manifest  ────────►   DevOps team uses manifest to
  with JSON transforms             script repo-side changes

Table action list     ────────►   DBAs run OPTIMIZE/ANALYZE
  with pre-migration SQL           before migration begins


PHASE 2 OUTPUT                    PHASE 3 INPUT
═══════════════                   ═════════════
Migrated notebooks    ────────►   Conversion validator runs
  in archive/migrated/             14-check comparison

Updated job JSON      ────────►   Test job runs on
  in Azure DevOps                  serverless compute

requirements.txt      ────────►   Dependency validation
  in Volume path                   during test runs
```

---

## 14. Complete Batch Assessment Notebook

This is the end-to-end orchestrator notebook. Copy this into a Databricks notebook to run the full batch assessment.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Assessment Workflow
# MAGIC
# MAGIC Run this notebook to assess a batch of jobs for migration.
# MAGIC Provide the manifest path as a widget parameter.

# COMMAND ----------

# Setup: install no extra packages -- uses only built-in libraries
import requests
import json
import re
import base64
import csv
from collections import Counter
from datetime import datetime
from io import StringIO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("manifest_path", "", "Manifest Path (CSV or JSON)")
dbutils.widgets.text("batch_name", "Batch Assessment", "Batch Name")
dbutils.widgets.text("output_path", "/Volumes/prod_catalog/default/migration/assessments", "Output Path")

manifest_path = dbutils.widgets.get("manifest_path")
batch_name = dbutils.widgets.get("batch_name")
output_path = dbutils.widgets.get("output_path")

print(f"Manifest: {manifest_path}")
print(f"Batch: {batch_name}")
print(f"Output: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Manifest

# COMMAND ----------

# Paste the load_manifest_csv and load_manifest_json functions from Section 2 here

if manifest_path.endswith(".json"):
    manifest = load_manifest_json(manifest_path)
else:
    manifest = load_manifest_csv(manifest_path)

print(f"Loaded {len(manifest)} jobs from manifest")
for entry in manifest:
    print(f"  {entry['job_name']} ({entry['job_id']}) -> Path {entry['prescribed_path']} [{entry['priority']}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract Job Configs

# COMMAND ----------

# Paste the get_job_config, extract_job_metadata, and helper functions from Section 3 here

print("Extracting job configs...")
all_metadata = extract_batch_configs(manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Classify Jobs

# COMMAND ----------

# Paste the classify_job and detect_notebook_language functions from Section 4 here

all_classifications = {}
for job_id, metadata in all_metadata.items():
    classification = classify_job(metadata)
    # Detect notebook languages
    for task in metadata.get("tasks", []):
        if task.get("type") == "notebook" and task.get("notebook_path"):
            lang = detect_notebook_language(task["notebook_path"])
            if lang != "UNKNOWN":
                classification["languages"].append(lang)
    classification["languages"] = list(set(classification["languages"]))
    all_classifications[job_id] = classification
    print(f"  {metadata['job_name']}: languages={classification['languages']}, dbr={classification['dbr_versions']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Feasibility Validation

# COMMAND ----------

# Paste the check_feasibility function from Section 5 here

all_feasibility = {}
for job_id, metadata in all_metadata.items():
    classification = all_classifications[job_id]
    findings = check_feasibility(metadata, classification)
    all_feasibility[job_id] = findings
    status = "BLOCK" if any(f["status"] == "BLOCK" for f in findings) else \
             "WARN" if any(f["status"] == "WARN" for f in findings) else "PASS"
    print(f"  {metadata['job_name']}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Path Routing

# COMMAND ----------

# Paste the route_job function from Section 6 here

all_routings = {}
for job_id, metadata in all_metadata.items():
    classification = all_classifications[job_id]
    # Build notebook_languages dict
    nb_langs = {}
    for task in metadata.get("tasks", []):
        if task.get("type") == "notebook" and task.get("notebook_path"):
            nb_langs[task["notebook_path"]] = detect_notebook_language(task["notebook_path"])
    routing = route_job(metadata, classification, nb_langs)
    all_routings[job_id] = routing
    if routing.get("override"):
        print(f"  {metadata['job_name']}: {routing['prescribed_path']} -> {routing['recommended_path']} (OVERRIDE: {routing['override_reason'][:80]})")
    else:
        print(f"  {metadata['job_name']}: {routing['recommended_path']} (confirmed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Notebook Code Scan

# COMMAND ----------

# Paste scan functions from Section 7 here

all_notebook_findings = {}
for job_id, metadata in all_metadata.items():
    # Skip blocked jobs
    if not feasibility_passed(all_feasibility.get(job_id, [])):
        print(f"  SKIPPING {metadata['job_name']} (blocked)")
        all_notebook_findings[job_id] = {}
        continue

    path = all_routings[job_id].get("recommended_path", "C")
    print(f"\nScanning {metadata['job_name']} (Path {path})...")
    findings = scan_all_notebooks_for_job(metadata, path)
    all_notebook_findings[job_id] = findings

    total = sum(len(f) for f in findings.values())
    print(f"  Total findings: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Data Compatibility Checks

# COMMAND ----------

# Paste table assessment functions from Section 8 here

all_table_findings = {}
for job_id, metadata in all_metadata.items():
    if not feasibility_passed(all_feasibility.get(job_id, [])):
        all_table_findings[job_id] = {}
        continue

    print(f"\nAssessing tables for {metadata['job_name']}...")
    table_findings = assess_all_tables_for_job(metadata)
    all_table_findings[job_id] = table_findings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Generate Repo-Side Findings

# COMMAND ----------

# Paste add_repo_findings function from Section 9 here

all_repo_findings = {}
for job_id, metadata in all_metadata.items():
    classification = all_classifications[job_id]
    repo_findings = add_repo_findings(metadata, classification)
    all_repo_findings[job_id] = repo_findings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate Per-Job Reports

# COMMAND ----------

# Paste generate_job_report function from Section 10 here

job_reports = {}
for job_id, metadata in all_metadata.items():
    report = generate_job_report(
        metadata=metadata,
        classification=all_classifications[job_id],
        routing=all_routings[job_id],
        feasibility=all_feasibility[job_id],
        notebook_findings=all_notebook_findings.get(job_id, {}),
        table_findings=all_table_findings.get(job_id, {}),
        repo_findings=all_repo_findings.get(job_id, []),
    )
    job_reports[job_id] = report

# Print all reports
for job_id, report in job_reports.items():
    print(report)
    print("\n" + "-" * 60 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Generate Batch Summary

# COMMAND ----------

# Paste generate_batch_summary function from Section 11 here

summary = generate_batch_summary(
    batch_name=batch_name,
    job_reports=job_reports,
    all_metadata=all_metadata,
    all_classifications=all_classifications,
    all_routings=all_routings,
    all_feasibility=all_feasibility,
    all_notebook_findings=all_notebook_findings,
    all_table_findings=all_table_findings,
    all_repo_findings=all_repo_findings,
)

print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Save Results

# COMMAND ----------

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
batch_dir = f"{output_path}/{batch_name.replace(' ', '_')}_{timestamp}"

# Save batch summary
dbutils.fs.put(f"{batch_dir}/batch_summary.txt", summary, overwrite=True)

# Save per-job reports
for job_id, report in job_reports.items():
    job_name = all_metadata[job_id].get("job_name", job_id)
    dbutils.fs.put(f"{batch_dir}/jobs/{job_name}_report.txt", report, overwrite=True)

# Save structured data as JSON for downstream tooling
structured_output = {
    "batch_name": batch_name,
    "assessment_date": timestamp,
    "job_count": len(job_reports),
    "metadata": {k: v for k, v in all_metadata.items()},
    "classifications": all_classifications,
    "routings": all_routings,
    "feasibility": all_feasibility,
    "repo_findings": all_repo_findings,
    # Notebook findings and table findings are large -- save separately if needed
}

dbutils.fs.put(
    f"{batch_dir}/assessment_data.json",
    json.dumps(structured_output, indent=2, default=str),
    overwrite=True
)

print(f"\nResults saved to: {batch_dir}")
print(f"  batch_summary.txt")
print(f"  jobs/<job_name>_report.txt (x{len(job_reports)})")
print(f"  assessment_data.json")
```

---

## Quick Reference: Assessment Checklist

Use this checklist when manually reviewing assessment results before approving execution.

```
BATCH ASSESSMENT REVIEW CHECKLIST
==================================

[ ] Manifest loaded correctly -- all job_ids resolved
[ ] No BLOCK feasibility findings (or blocks resolved manually)
[ ] Path overrides reviewed and approved by tower lead
[ ] CRITICAL findings have clear fix paths (no ambiguous recommendations)
[ ] Streaming jobs excluded from serverless paths
[ ] GPU/ML runtime jobs evaluated for serverless feasibility
[ ] Repo change manifest reviewed by DevOps lead
[ ] Pre-migration table actions (OPTIMIZE, ANALYZE) scheduled
[ ] Effort estimates reviewed -- high-effort jobs have owners
[ ] Archive directory structure created (see 11-archiving-workflow.md)
[ ] Test environment identified for parallel SIT (see 08-testing-validation-framework.md)

APPROVAL:
  Tower Lead: ____________  Date: __________
  DevOps Lead: ___________  Date: __________
  Migration Lead: ________  Date: __________
```
