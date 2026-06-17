---
name: weekly-report-generator
description: Generate weekly project reports from repository activity and publish them to Notion.
metadata: 
  short-description: Generate project reports from git history
---

# Objective

Generate a management-level weekly report from repository activity and project documentation.

Focus on project outcomes, progress, and pending work rather than implementation details.

# Workflow

1. Define the reporting period as the current ISO week (YYYY-WW).

2. Retrieve Git commits within the reporting period.

3. Review modified files and relevant code changes.

4. Group related commits into meaningful work items.

5. Identify:

   * Completed work
   * In-progress work

6. Generate a weekly report following references/weekly_report_template.md.

7. Update the Notion report if a Notion MCP server is available.

   1. Present a preview of the generated report to the user.
   2. Ask for user approval before creating or modifying any Notion content.
   3. Search for a database named "Project Reports".
      If the database cannot be found, ask the user before creating any new content.
   4. Search for a report page matching the current ISO week (YYYY-WW).
      If no report page exists, create a new one.
   5. Append the approved report to the report page.

# Reporting Principles

* Focus on outcomes rather than individual commits.
* Combine related commits into a single accomplishment.
* Use concise management-friendly language.
* Avoid unnecessary technical details.
* Highlight only information relevant to stakeholders.

# Evidence-Based Reporting

All statements must be supported by available evidence, including:

* Git commits
* Source code changes
* Pull requests
* Project documentation
* Notion pages

Do not invent, infer, or speculate.