---
name: data-governance-documentation-generator
description: Generate data governance documentation from PostgreSQL metadata, including data dictionary and schema overview.
metadata:
  short-description: Generate data governance documentation from PostgreSQL
-------------------------------------------------------------------------

# Objective

Generate data governance documentation from PostgreSQL metadata.

Inspect database metadata to produce:

* Database overview
* Schema overview
* Table inventory
* Column definitions

# Scope Definition

Document database objects actively used by this repository.

Included:

* Tables referenced by dbt models
* Views referenced by dbt models
* Tables referenced by Airflow DAGs
* Tables referenced by repository SQL files
* Source tables defined in dbt source configurations

Excluded:

* PostgreSQL system schemas
* Temporary tables
* Test tables
* Backup tables
* Sample tables
* Database objects not referenced by the repository

Do not scan or document the entire database unless explicitly requested.

# Prerequisites

Before execution, verify that the following MCP servers are available.

* weather_db
* notion

If the Notion MCP server is configured but unavailable:

* Run login_notion.sh
* Recheck MCP availability before continuing.
* Stop if the issue persists.

# Workflow

0. Check MCP availability. If unavailable, stop and ask the user to resolve it before continuing.

1. Identify database objects within the defined scope.

2. Retrieve metadata for each database object, including:

   * Table name
   * Column name
   * Data type
   * Nullable status
   * Default value
   * Comment

3. Generate a database overview and schema inventory.

4. Generate a data dictionary for in-scope database objects only.

5. Update the Notion documentation.

   1. Present a preview of the generated documentation to the user.
   2. Ask for user approval before creating or modifying any Notion content.
   3. Search for a page named "Airflow_docker Documentation".
   4. If the page cannot be found, ask the user before creating any new content.
   5. Append the approved documentation to the page.

# Principles

* Use the weather_db MCP server for database metadata retrieval.
* Read database metadata directly from the database.
* Treat database metadata as the source of truth.
* Limit documentation to database objects within the defined scope.
* Do not invent, infer, or speculate.