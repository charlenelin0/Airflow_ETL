---
name: databricks-etl-integration
description: Reusable workflow for integrating Databricks ETL into an existing data pipeline, including raw landing, bronze/silver/gold notebooks, validation, deployment, and optional orchestration.
metadata:
  short-description: Reusable Databricks ETL integration workflow
---

# Objective
Use this skill to add or evolve a Databricks ETL path in an existing data project.

Use for:
* Sending raw files from an orchestrator to Databricks
* Building bronze, silver, or gold notebooks
* Designing Volume and Delta table layouts
* Validating notebook outputs before automation
* Planning Databricks Jobs or Airflow-triggered execution

# Core Pattern
Prefer incremental adoption:

```text
existing pipeline stays stable
  + new Databricks path runs in parallel
  -> validate outputs
  -> promote only after confidence is high
```

Typical flow:

```text
source/API/files -> raw landing -> bronze -> silver -> gold
```

# Step 1: Define Scope
Before coding, clarify:
* What source data is sent to Databricks?
* Is Databricks replacing a transform or running in parallel?
* Which layer is in scope: bronze, silver, gold, or all?
* Should failures block the existing pipeline?
* Will execution be manual, Databricks Jobs, or Airflow-triggered?

Default:
* Start parallel.
* Land raw data first.
* Validate notebooks manually.
* Automate after successful manual runs.

# Step 2: Landing Design
Use a stable landing layout with explicit partitions:
```text
/Volumes/<catalog>/<schema>/<volume>/<domain>/bronze/date=YYYY-MM-DD/<object_type>/source=<name>.json
```

Rules:
* Keep raw files minimally transformed.
* Preserve traceability with path metadata or columns.
* Do not commit credentials.
* Use Airflow connections, Databricks secrets, or cloud IAM.

# Step 3: Orchestrator Integration
If Airflow or another orchestrator sends files to Databricks:
* Keep upload helpers isolated from DAG business logic.
* Put connection IDs and base paths in shared config.
* Decide failure behavior explicitly:
  * parallel test phase: log warning and continue
  * production dependency: fail task and alert
* Avoid changing existing DAG dependencies unless requested.

# Step 4: Notebook Design
Databricks notebooks should accept parameters:
```text
batch_date
source_base_path
target_catalog_or_schema
```

Silver notebooks should:
* Read only the requested batch or partition.
* Flatten nested arrays or records into analytic rows.
* Add `batch_date`, `source_file`, and `processed_at`.
* Write Delta tables.
* Be idempotent for reruns.

For Unity Catalog, use:
```python
F.col("_metadata.file_path")
```

Do not use:
```python
F.input_file_name()
```

# Step 5: Idempotency
Avoid duplicate rows on rerun.

For batch tables, prefer:
```python
.mode("overwrite")
.option("replaceWhere", f"batch_date = '{batch_date}'")
```

Use append only when the table is event-based and deduplication is handled.

# Step 6: Validation
Before automation, run notebooks manually in Databricks.

Validate:
* Input files are found.
* Row counts are expected.
* Required columns are populated.
* Source file metadata is correct.
* Re-running the same batch does not duplicate rows.
* Output tables can be queried.

Useful SQL:
```sql
select batch_date, count(*) from <table> group by batch_date;
select * from <table> where batch_date = date('YYYY-MM-DD') limit 20;
```

# Step 7: Deployment
Recommended progression:
1. Commit and push notebook changes.
2. Sync code to Databricks using Git Folder / Repos.
3. Run notebook manually with parameters.
4. Validate tables.
5. Create a Databricks Job.
6. Decide whether Airflow should trigger the job.

# Step 8: Documentation
Document:
* Landing path
* Notebook path
* Required parameters
* Output tables
* Validation SQL
* Known platform constraints
* Failure behavior

# Guardrails
Do not automatically:
* Replace a working existing pipeline
* Add Airflow Databricks job triggers before manual validation
* Commit tokens, passwords, or workspace secrets
* Hide upload failures without an explicit policy
* Assume raw data contains metadata that only exists in file paths
