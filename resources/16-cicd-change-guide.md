# CI/CD Change Guide: Azure DevOps Pipeline Migration

This guide covers the **repo-side** changes needed when migrating Databricks jobs to serverless compute. This is the second execution track — separate from the Databricks-side notebook changes that Genie Code handles.

**Audience:** the customer's DevOps team and engineers who manage the Azure DevOps pipelines and PowerShell deployment scripts.

---

## Architecture Overview

The customer's deployment flow:

```
Azure DevOps Pipeline
  │
  ├── Variable Group (e.g., <Variable_Group_UAT>)
  │   └── Contains: env_name=uat, databricks_cluster, paths, etc.
  │
  ├── Artifact: Job JSON templates (e.g., <job_name>.json)
  │
  └── PowerShell Script (deploy_report_workflow_jobs.ps1)
      ├── Reads variables from pipeline variable group
      ├── Reads job JSON templates from artifact
      ├── Does string replacement: %placeholder% → actual value
      └── Deploys via Databricks Jobs API (create or reset)
```

---

## 1. Job JSON Template Changes

### Before (Classic Compute)

```json
{
  "name": "<job_name>",
  "email_notifications": {
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "<task_key>",
      "notebook_task": {
        "notebook_path": "%eim_wsp%/<state>/srccode/<job_folder>/<notebook_name>",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "Job_cluster",
      "timeout_seconds": 0,
      "email_notifications": {}
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "Job_cluster",
      "new_cluster": {
        "spark_version": "%spark_version_11%",
        "node_type_id": "%uc_reporting_node_type_id%",
        "driver_node_type_id": "%uc_reporting_driver_node_type_id%",
        "policy_id": "%uc_reporting_small_cluster_policy_id%"
      }
    }
  ],
  "format": "MULTI_TASK"
}
```

### After (Serverless Compute)

```json
{
  "name": "<job_name>",
  "email_notifications": {
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "parameters": [
    {
      "name": "PATH_LANDING",
      "default": "abfss://landingzone@dataingestion%env_name%adlsg2.dfs.core.windows.net/"
    },
    {
      "name": "PATH_DATALAKE",
      "default": "abfss://eim-datalake-%env_name%@scadlsg2datbks%env_name%.dfs.core.windows.net/"
    }
  ],
  "tasks": [
    {
      "task_key": "<task_key>",
      "notebook_task": {
        "notebook_path": "%eim_wsp%/<state>/srccode/<job_folder>/<notebook_name>",
        "source": "WORKSPACE"
      },
      "environment_key": "serverless_environment_v1",
      "timeout_seconds": 0,
      "email_notifications": {}
    }
  ],
  "environments": [
    {
      "environment_key": "serverless_environment_v1",
      "spec": {
        "client": "4",
        "dependencies": [
          "-r /Volumes/%env_name%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt"
        ]
      }
    }
  ],
  "queue": {
    "enabled": true
  },
  "performance_optimized": true,
  "format": "MULTI_TASK"
}
```

### Key Changes Summary

| Change | Before | After |
|--------|--------|-------|
| **Remove** `job_clusters` section | Present | Removed entirely |
| **Remove** `job_cluster_key` from tasks | `"job_cluster_key": "Job_cluster"` | Removed |
| **Add** `parameters` block | Not present | Job-level parameters for PATH_LANDING, PATH_DATALAKE with `%env_name%` substitution |
| **Add** `environment_key` to each task | Not present | `"environment_key": "serverless_environment_v1"` |
| **Add** `environments` block | Not present | Environment spec with client "4" and dependencies |
| **Add** `queue` | Not present | `"queue": {"enabled": true}` |
| **Add** `performance_optimized` | Not present | `"performance_optimized": true` |
| **Add** `%env_name%` placeholder | Not used | In Volume path, PATH_LANDING, and PATH_DATALAKE |
| **Remove** cluster-specific placeholders | `%spark_version_11%`, `%node_type_id%`, etc. | Not needed |

### Job-Level Parameters

The `parameters` block defines job-level parameters with environment-specific defaults. These are automatically available in notebooks via `dbutils.widgets.get()`:

```python
# In the notebook — replaces os.environ.get() calls:
path_landing = dbutils.widgets.get("PATH_LANDING")
path_datalake = dbutils.widgets.get("PATH_DATALAKE")
```

The `%env_name%` placeholder in the parameter defaults gets resolved by the PowerShell deployment script before the JSON is sent to the Databricks API. For UAT, `PATH_LANDING` resolves to:
```
abfss://landingzone@dataingestionuatadlsg2.dfs.core.windows.net/
```

### Where %env_name% Appears

The `%env_name%` placeholder now appears in three locations in the JSON:

1. **Requirements.txt path:** `/Volumes/%env_name%_catalog/default/serverless_dependencies/...`
2. **PATH_LANDING default:** `dataingestion%env_name%adlsg2`
3. **PATH_DATALAKE default:** `datalake-%env_name%` and `datbks%env_name%`

All three are handled by a single `.Replace("%env_name%","$env_name")` call in the PowerShell script.

---

## 2. PowerShell Deployment Script Changes

### The %env_name% Replacement Bug (Discovered 2026-04-16)

The existing deployment script (`deploy_report_workflow_jobs.ps1`) reads the `env_name` variable from the pipeline variable group but **does not perform string replacement** for the `%env_name%` placeholder in the job JSON.

**Root cause:** Line 5 reads the variable:
```powershell
$env_name="$($env:env_name)"  # Gets "uat" from variable group
```

But the replacement block (lines 96-108) has replacements for `%eim_wsp%`, `%node_type_id%`, etc. — but **no replacement for `%env_name%`**.

**Fix:** Add this line after the existing replacements (after line 108):

```powershell
$bodyJson = $bodyJson.Replace("%env_name%","$env_name")
```

### Complete Replacement Block (Updated)

```powershell
#replace variables from variable groups
$bodyJson = $bodyJson.Replace("%eim_wsp%","$databricksWorkspaceNotebookPath")
$bodyJson = $bodyJson.Replace("%eim_notification_email%","$eimNotificationEmail")
$bodyJson = $bodyJson.Replace("%node_type_id%","$node_type_id")
$bodyJson = $bodyJson.Replace("%driver_node_type_id%","$driver_node_type_id")
$bodyJson = $bodyJson.Replace("%uc_reporting_node_type_id%","$uc_reporting_node_type_id")
$bodyJson = $bodyJson.Replace("%uc_reporting_driver_node_type_id%","$uc_reporting_driver_node_type_id")
$bodyJson = $bodyJson.Replace("%reporting_medium_cluster_policy_id%","$reporting_medium_cluster_policy_id")
$bodyJson = $bodyJson.Replace("%reporting_small_cluster_policy_id%","$reporting_small_cluster_policy_id")
$bodyJson = $bodyJson.Replace("%uc_reporting_small_cluster_policy_id%","$uc_reporting_small_cluster_policy_id")
$bodyJson = $bodyJson.Replace("%spark_version_11%","$spark_version_11")

# NEW: Required for serverless jobs
$bodyJson = $bodyJson.Replace("%env_name%","$env_name")
```

### Variable Group Changes

The `env_name` variable already exists in the customer's variable groups:

| Variable Group | env_name Value |
|---------------|---------------|
| <Variable_Group_DEV> | `dev` |
| <Variable_Group_UAT> | `uat` |
| <Variable_Group_PROD> | `prod` |

This variable is used to construct the requirements.txt Volume path:
```
/Volumes/{env_name}_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt
```

Which resolves to:
- DEV: `/Volumes/dev_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`
- UAT: `/Volumes/uat_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`
- PROD: `/Volumes/prod_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt`

**No new variable group entries are needed** — `env_name` already exists. The only change is adding the `.Replace()` call in the PowerShell script.

---

## 3. Job ID Retention

**Issue from the customer's log (#9):** Job ID changes when updating workflow JSON through DevOps, breaking Autosys job references.

**Root cause:** The deployment script creates a new job (via `jobs/create`) instead of updating the existing one (via `jobs/reset`).

**The script already handles this correctly** (lines 118-141) — it checks if the job exists by name and uses `jobs/reset` for updates. But verify that:

1. The job name in the JSON matches the existing job name exactly (case-sensitive)
2. The service principal (`databricks_service_principal`) used for deployment is the same one that created the original jobs
3. The `jobs/reset` endpoint is used for updates, not `jobs/create`

```powershell
# Existing logic in the script (already correct):
if ($eimJobDict.Contains($jobName)) {
    # UPDATE existing job — preserves job_id
    $existingJobJson = "{`"job_id`":`"$value`",`"new_settings`":$bodyjson}"
    $job = Invoke-RestMethod -Method POST -Uri "https://$databricksCluster.azuredatabricks.net/api/$databricksJobsApiVersion/jobs/reset" -headers $headers -body $existingJobJson
} else {
    # CREATE new job — only for first deployment
    $job = Invoke-RestMethod -Method POST -Uri "https://$databricksCluster.azuredatabricks.net/api/$databricksJobsApiVersion/jobs/create" -headers $headers -body $bodyJson
}
```

---

## 4. Additive Changes Only

**Design principle:** Do NOT remove existing pipeline variables or modify the deployment script structure. Only add the new `%env_name%` replacement and update job JSON templates.

This means:
- Keep all existing variable group entries (even cluster-related ones like `node_type_id`)
- Keep all existing `.Replace()` calls in the PowerShell script (they're harmless if the placeholder doesn't exist in the JSON)
- Keep the existing create/reset logic
- Only ADD the `%env_name%` replacement
- Only CHANGE the job JSON files that are being migrated to serverless

---

## 5. Migration Checklist per Job

For each job being migrated to serverless:

- [ ] **Job JSON:** Remove `job_clusters` section
- [ ] **Job JSON:** Remove `job_cluster_key` from each task
- [ ] **Job JSON:** Add `environment_key` to each task
- [ ] **Job JSON:** Add `environments` block with client "4" and requirements.txt path
- [ ] **Job JSON:** Add `queue.enabled = true`
- [ ] **Job JSON:** Add `performance_optimized = true`
- [ ] **Job JSON:** Use `%env_name%` placeholder in Volume path
- [ ] **PowerShell:** Verify `%env_name%` replacement exists in deployment script
- [ ] **Variable Group:** Verify `env_name` variable exists (should already be there)
- [ ] **Test:** Deploy to UAT and verify the Volume path resolves correctly
- [ ] **Test:** Run the job and confirm it picks up requirements.txt
- [ ] **Test:** Verify job ID is preserved (not a new job ID)

---

## 6. Batch Migration for Azure DevOps

When migrating multiple jobs in a batch:

1. **Create a feature branch** in the Azure DevOps repo
2. **Update all job JSON files** in the batch (use the template above)
3. **Update the PowerShell script** once (add the `%env_name%` replacement)
4. **Commit and create a PR** for review
5. **Deploy to UAT first** via the UAT pipeline
6. **Validate all jobs in UAT** — run each job, verify it completes on serverless
7. **Promote to PROD** via the PROD pipeline after UAT validation

### Recommended PR Template

```markdown
## Serverless Migration — Batch X

### Jobs Migrated
- <job_name> (<job_id>)
- <job_name> (<job_id>)
- [list all jobs in the batch]

### Changes
- Updated job JSON templates: removed job_clusters, added serverless environments
- Added %env_name% replacement to deploy_report_workflow_jobs.ps1
- Requirements.txt path: /Volumes/%env_name%_catalog/default/serverless_dependencies/dependencies/engineering-requirements.txt

### Validation
- [ ] All jobs deployed successfully to UAT
- [ ] All jobs ran on serverless compute (verified via job run details)
- [ ] All job IDs preserved (not recreated)
- [ ] Output data validated against classic compute baseline
- [ ] Ready for PROD deployment
```
