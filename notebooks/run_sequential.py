# Databricks notebook source
# MAGIC %md
# MAGIC # WAF Recommendations — Sequential
# MAGIC
# MAGIC Loads each `.sql` file from the repo by path (no SQL duplicated in this
# MAGIC notebook) and runs them in order: config → warehouse → query/table → jobs.
# MAGIC
# MAGIC **Compute:** Serverless notebook (or DBR 14.1+) — needs `spark` and Python.

# COMMAND ----------

import os

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))


def run_sql_file(rel_path: str):
    """Execute every statement in a .sql file. Returns the last DataFrame (or None)."""
    with open(os.path.join(REPO_ROOT, rel_path)) as f:
        sql = f.read()
    last = None
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt or all(
            line.strip().startswith("--") or not line.strip()
            for line in stmt.splitlines()
        ):
            continue
        last = spark.sql(stmt)
    return last


print(f"Repo root: {REPO_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Config

# COMMAND ----------

run_sql_file("queries/config.sql")
print("Config loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL warehouse recommendations

# COMMAND ----------

display(run_sql_file("queries/warehouse_recommendations.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query & table recommendations

# COMMAND ----------

display(run_sql_file("queries/query_table_recommendations.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Jobs → serverless candidacy

# COMMAND ----------

display(run_sql_file("queries/jobs_serverless_candidacy.sql"))
