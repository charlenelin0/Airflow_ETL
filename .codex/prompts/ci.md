# Goal

Create a GitHub Actions CI workflow for this repository.

# Context

This project is a weather data platform built with:

- Apache Airflow
- dbt
- Python
- PostgreSQL
- MinIO
- BigQuery

The repository already contains a dbt project and Airflow DAGs.

# Materials

You may inspect:

- dags/
- dbt/
- requirements.txt
- existing GitHub workflow files

# Constraints

- Do not modify any files without approval.
- Keep the workflow simple.
- Avoid introducing additional services.
- Do not add tests that do not already exist in the repository.

# Definition of Done

Provide:

1. Recommended CI workflow design.
2. Explanation of each CI step.
3. Generated ci.yml draft.
4. Wait for approval before writing files.