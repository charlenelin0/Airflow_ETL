# Databricks Airflow Helpers

This package contains Airflow-side helpers for landing raw data in Databricks.

Current scope:

- Upload raw JSON files to a Databricks Volume.
- Use Airflow connection `databricks_conn`.
- Keep Databricks-specific upload code separate from DAG business logic.

Out of scope:

- Triggering Databricks Workflows or Jobs from Airflow.
- Versioning Databricks Workflow definitions in this repository.
- Storing Databricks credentials in source code.

Databricks Workflows are configured directly in the Databricks UI.
