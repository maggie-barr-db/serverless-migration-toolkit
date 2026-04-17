# Archiving Workflow

This guide defines the standard process for archiving original notebooks before migration. Every notebook must be archived before Genie Code makes any modifications, ensuring a clean copy exists for rollback, side-by-side comparison, and audit.

---

## Molina's Archiving Model

Molina uses two archiving mechanisms, and they serve different purposes:

### 1. Git-Based Archiving (Primary — Source of Truth)

The Azure DevOps repo IS the archive. Original code lives on the main/release branch. Migration changes go on feature branches. The original code is preserved in git history.

```
Azure DevOps Repo:
├── main (or release branch)           ← Original code = the archive
├── migration/batch_01/job_name_1      ← Feature branch with changes
├── migration/batch_01/job_name_2
└── ...
```

**This is the source of truth.** Repo files have `%placeholder%` tokens — they're parameterized and environment-portable. All migration changes are committed here.

### 2. Workspace Staging (Secondary — For Testing Only)

Genie Code creates staging copies in the UAT workspace for validation testing. These copies have hardcoded environment values (from CI/CD deployment) and are **temporary and disposable**.

```
/Workspace/Migration/staging/{job_name}/
├── notebook_1.py              ← Modified copy with hardcoded UAT values
├── notebook_2.py              ← NOT for committing back to repo
└── change_manifest.md         ← The change report (THIS goes to the developer)
```

**Never commit workspace staging files to the repo.** They contain resolved environment values (`uat_catalog` instead of `%env_name%_catalog`) that would break other environments.

---

## Why Archive

1. **Rollback safety** — If migration fails, the original is on the main branch in the repo
2. **Side-by-side comparison** — Reviewers can diff the feature branch PR against main
3. **Audit trail** — Regulated healthcare data requires change documentation (the PR + conversion report)
4. **Parallel SIT** — Original job continues running from main branch deployment while migrated job is tested from staging copies
5. **Conversion report** — Generated from comparing workspace originals vs staging copies

---

## Archive Directory Structure

```
/Workspace/Archive/migration_2026/
├── batch_{batch_number}/
│   ├── {job_name}/
│   │   ├── original/              ← Exact copies, never modified
│   │   │   ├── 01_bronze.py
│   │   │   ├── 02_silver.py
│   │   │   ├── 03_gold.py
│   │   │   └── utils/
│   │   │       └── config.py
│   │   ├── migrated/              ← Working copies with all changes
│   │   │   ├── 01_bronze.py
│   │   │   ├── 02_silver.py
│   │   │   ├── 03_gold.py
│   │   │   └── utils/
│   │   │       └── config.py
│   │   ├── validation/            ← Validation notebooks and reports
│   │   │   ├── validate_outputs.py
│   │   │   └── conversion_report.py
│   │   └── manifest.json          ← Migration metadata
│   └── batch_manifest.json        ← Batch-level metadata
```

---

## Step-by-Step Archiving Process

### Step 1: Identify All Notebooks for the Job

```python
# For each job, get ALL notebook paths (including %run dependencies)
import requests

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def get_job_notebooks(job_id):
    """Get all notebook paths referenced by a job."""
    response = requests.get(
        f"https://{host}/api/2.1/jobs/get",
        headers={"Authorization": f"Bearer {token}"},
        params={"job_id": job_id}
    )
    job = response.json()
    
    notebook_paths = set()
    for task in job.get("settings", {}).get("tasks", []):
        if "notebook_task" in task:
            path = task["notebook_task"]["notebook_path"]
            notebook_paths.add(path)
    
    return job["settings"]["name"], sorted(notebook_paths)

job_name, paths = get_job_notebooks("123456789")
print(f"Job: {job_name}")
for p in paths:
    print(f"  {p}")
```

### Step 2: Discover %run Dependencies

```python
def find_run_dependencies(notebook_path, discovered=None):
    """Recursively find all %run dependencies."""
    if discovered is None:
        discovered = set()
    if notebook_path in discovered:
        return discovered
    discovered.add(notebook_path)
    
    # Export notebook content
    response = requests.get(
        f"https://{host}/api/2.0/workspace/export",
        headers={"Authorization": f"Bearer {token}"},
        params={"path": notebook_path, "format": "SOURCE"}
    )
    
    if response.status_code == 200:
        import base64, re
        content = base64.b64decode(response.json()["content"]).decode("utf-8")
        
        # Find %run commands
        run_pattern = re.compile(r'%run\s+([^\s]+)')
        for match in run_pattern.finditer(content):
            dep_path = match.group(1)
            # Resolve relative paths
            if dep_path.startswith("./") or dep_path.startswith("../"):
                base_dir = "/".join(notebook_path.split("/")[:-1])
                dep_path = f"{base_dir}/{dep_path}"
            find_run_dependencies(dep_path, discovered)
    
    return discovered
```

### Step 3: Copy Notebooks to Archive

```python
import base64

def copy_notebook(source_path, dest_path):
    """Copy a notebook from source to destination."""
    # Export
    export_resp = requests.get(
        f"https://{host}/api/2.0/workspace/export",
        headers={"Authorization": f"Bearer {token}"},
        params={"path": source_path, "format": "SOURCE"}
    )
    
    if export_resp.status_code != 200:
        print(f"ERROR: Could not export {source_path}: {export_resp.text}")
        return False
    
    content = export_resp.json()["content"]
    language = export_resp.json().get("language", "PYTHON")
    
    # Create destination directory
    dest_dir = "/".join(dest_path.split("/")[:-1])
    requests.post(
        f"https://{host}/api/2.0/workspace/mkdirs",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": dest_dir}
    )
    
    # Import to destination
    import_resp = requests.post(
        f"https://{host}/api/2.0/workspace/import",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "path": dest_path,
            "content": content,
            "language": language,
            "overwrite": False,  # Do NOT overwrite existing archives
            "format": "SOURCE"
        }
    )
    
    if import_resp.status_code == 200:
        print(f"  Archived: {source_path} → {dest_path}")
        return True
    else:
        print(f"  ERROR: {import_resp.text}")
        return False

def archive_job(job_id, batch_number):
    """Archive all notebooks for a job."""
    job_name, task_paths = get_job_notebooks(job_id)
    
    # Find all dependencies
    all_paths = set()
    for path in task_paths:
        all_paths.update(find_run_dependencies(path))
    
    archive_base = f"/Workspace/Archive/migration_2026/batch_{batch_number}/{job_name}"
    
    results = []
    for source_path in sorted(all_paths):
        # Determine relative path from common prefix
        # Example: /Repos/production/claims/01_bronze → 01_bronze
        relative = source_path.split("/")[-1]  # Simplest: just the notebook name
        
        # Copy to original/ (archive)
        orig_dest = f"{archive_base}/original/{relative}"
        success_orig = copy_notebook(source_path, orig_dest)
        
        # Copy to migrated/ (working copy)
        migrated_dest = f"{archive_base}/migrated/{relative}"
        success_migrated = copy_notebook(source_path, migrated_dest)
        
        results.append({
            "source": source_path,
            "original_archive": orig_dest,
            "migrated_copy": migrated_dest,
            "archived": success_orig,
            "copied": success_migrated
        })
    
    return job_name, results
```

### Step 4: Create manifest.json

```python
import json
from datetime import datetime

def create_manifest(job_id, job_name, batch_number, archive_results, migration_path, output_tables):
    """Create the migration manifest for a job."""
    
    # Get baseline versions for all output tables
    baseline_versions = {}
    for table_name in output_tables:
        try:
            history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
            version = history.select("version").collect()[0][0]
            baseline_versions[table_name] = version
        except Exception as e:
            baseline_versions[table_name] = f"ERROR: {e}"
    
    manifest = {
        "job_id": str(job_id),
        "job_name": job_name,
        "batch_number": batch_number,
        "migration_path": migration_path,
        "source_dbr": "13.3 LTS",
        "target_compute": {
            "A": "16.4 LTS (classic)",
            "B": "serverless_env_v4",
            "C": "serverless_env_v4",
            "D": "dbsql_serverless"
        }.get(migration_path, "unknown"),
        "notebooks": [
            {
                "original_path": r["source"],
                "archive_path": r["original_archive"],
                "migrated_path": r["migrated_copy"],
                "archived_successfully": r["archived"]
            }
            for r in archive_results
        ],
        "output_tables": output_tables,
        "baseline_versions": baseline_versions,
        "migration_started": datetime.utcnow().isoformat() + "Z",
        "migration_completed": None,
        "validation_status": "pending"
    }
    
    # Save manifest
    archive_base = f"/Workspace/Archive/migration_2026/batch_{batch_number}/{job_name}"
    manifest_path = f"{archive_base}/manifest.json"
    
    # Write to workspace (via notebook or file API)
    dbutils.fs.put(
        manifest_path.replace("/Workspace/", "file:/Workspace/"),
        json.dumps(manifest, indent=2),
        overwrite=True
    )
    
    return manifest
```

### Step 5: Verify Archive Integrity

```python
def verify_archive(job_name, batch_number):
    """Verify that archived notebooks match originals."""
    archive_base = f"/Workspace/Archive/migration_2026/batch_{batch_number}/{job_name}"
    
    # Read manifest
    manifest_path = f"{archive_base}/manifest.json"
    # ... read and parse manifest ...
    
    for nb in manifest["notebooks"]:
        # Export both versions and compare
        orig_content = export_notebook(nb["original_path"])
        archive_content = export_notebook(nb["archive_path"])
        
        if orig_content == archive_content:
            print(f"  ✓ {nb['original_path']} — archive matches original")
        else:
            print(f"  ✗ {nb['original_path']} — MISMATCH! Archive may be corrupted")
```

---

## Rules

1. **NEVER modify files in the `original/` directory.** Only modify files in `migrated/`.
2. **Archive BEFORE any changes.** The archive must be created before Genie Code touches any notebook.
3. **One archive per job.** Each job gets its own directory, even if jobs share notebooks.
4. **Preserve folder structure.** If notebooks are in subdirectories, maintain that structure.
5. **Do NOT overwrite existing archives.** If an archive exists, it means a previous migration attempt was made. Create a new versioned archive or investigate.
6. **Record baseline versions immediately.** Delta table versions must be captured at archive time, not later.

---

## Git-Based Archiving (Azure DevOps)

For jobs managed via Azure DevOps repos, archiving is handled differently:

```
# Create a migration branch from the current production branch
git checkout main
git checkout -b migration/batch_{batch_number}/{job_name}

# The original code is preserved on main (or the current release branch)
# All migration changes happen on the migration branch
# After validation, the migration branch is merged via PR

# Branch naming convention:
migration/batch_01/daily_claims_pipeline
migration/batch_01/weekly_pharmacy_report
migration/batch_02/monthly_eligibility_load
```

### Azure DevOps PR Workflow

1. Create migration branch from production
2. Apply all code changes on the branch
3. Run validation against the branch notebooks
4. Create PR with the conversion report as the description
5. Require reviewer approval before merge
6. After merge, the original code is in git history (the archive)

---

## Batch Manifest

For batch processing (12-100 jobs), create a batch-level manifest:

```json
{
  "batch_number": 1,
  "batch_started": "2026-04-16T09:00:00Z",
  "total_jobs": 25,
  "jobs": [
    {
      "job_id": "123456789",
      "job_name": "daily_claims_pipeline",
      "migration_path": "C",
      "status": "archived",
      "manifest_path": "/Archive/migration_2026/batch_1/daily_claims_pipeline/manifest.json"
    },
    {
      "job_id": "987654321",
      "job_name": "weekly_pharmacy_report",
      "migration_path": "B",
      "status": "pending",
      "manifest_path": null
    }
  ],
  "status_summary": {
    "archived": 10,
    "migrating": 5,
    "validating": 3,
    "completed": 7,
    "failed": 0,
    "pending": 0
  }
}
```
