# Databricks notebook source
# MAGIC %md
# MAGIC # WAF Recommendations — Parallel
# MAGIC
# MAGIC Same as `run_sequential` but the three query files run concurrently after
# MAGIC the config setup. Uses a `ThreadPoolExecutor` — each thread issues its
# MAGIC own `spark.sql()`, and Spark schedules them in parallel.
# MAGIC
# MAGIC **Compute:** Serverless notebook (or DBR 14.1+).

# COMMAND ----------

import os
import re
from concurrent.futures import ThreadPoolExecutor

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))


def run_sql_file(rel_path: str):
    """Strip comments first so ';' inside a comment doesn't split a statement."""
    with open(os.path.join(REPO_ROOT, rel_path)) as f:
        sql = f.read()
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    last = None
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            last = spark.sql(stmt)
    return last


print(f"Repo root: {REPO_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Config (sequential — must complete before the parallel block)

# COMMAND ----------

run_sql_file("queries/config.sql")
print("Config loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run three queries in parallel

# COMMAND ----------

QUERIES = [
    "queries/warehouse_recommendations.sql",
    "queries/query_table_recommendations.sql",
    "queries/jobs_serverless_candidacy.sql",
]

with ThreadPoolExecutor(max_workers=len(QUERIES)) as ex:
    futures = {q: ex.submit(run_sql_file, q) for q in QUERIES}
    results = {q: f.result() for q, f in futures.items()}

print("All three queries finished.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Results

# COMMAND ----------

display(results["queries/warehouse_recommendations.sql"])

# COMMAND ----------

display(results["queries/query_table_recommendations.sql"])

# COMMAND ----------

display(results["queries/jobs_serverless_candidacy.sql"])
