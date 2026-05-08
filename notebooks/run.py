# Databricks notebook source
# MAGIC %md
# MAGIC # WAF Recommendations — Runner
# MAGIC
# MAGIC Loads SQL files from `queries/` by path and runs them — no SQL
# MAGIC duplicated in this notebook. Use the widgets at the top to pick which
# MAGIC queries to run and whether to run them sequentially or in parallel.
# MAGIC The config (`queries/config.sql`) always runs first.
# MAGIC
# MAGIC **Compute:** Serverless notebook (or DBR 14.1+).

# COMMAND ----------

import os
import re
from concurrent.futures import ThreadPoolExecutor

# multiselect's `defaultValue` must be a single choice (Databricks looks it
# up as one literal string in `choices`). Default to one query — open the
# widget dropdown to check the others.
dbutils.widgets.multiselect(
    "queries",
    "warehouse",
    ["warehouse", "query_table", "jobs"],
    label="Queries to run",
)
dbutils.widgets.dropdown(
    "mode",
    "parallel",
    ["parallel", "sequential"],
    label="Execution mode",
)

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))

QUERY_FILES = {
    "warehouse": "queries/warehouse_recommendations.sql",
    "query_table": "queries/query_table_recommendations.sql",
    "jobs": "queries/jobs_serverless_candidacy.sql",
}


def _split_sql(sql: str):
    """Split on ';' while respecting single/double/backtick-quoted regions so
    we don't split inside a string literal or quoted identifier."""
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in ("'", '"', '`'):
            quote, region = ch, [ch]
            i += 1
            while i < n:
                c = sql[i]
                region.append(c)
                if c == "\\" and i + 1 < n:
                    region.append(sql[i + 1])
                    i += 2
                    continue
                if c == quote:
                    if i + 1 < n and sql[i + 1] == quote:
                        region.append(quote)
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            buf.append("".join(region))
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def run_sql_file(rel_path: str):
    """Execute every statement in a .sql file. Returns the last DataFrame
    (or None). Strips line + block comments first, then quote-aware splits on
    ';' so neither a comment nor a string literal can break a statement."""
    with open(os.path.join(REPO_ROOT, rel_path)) as f:
        sql = f.read()
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    last = None
    for stmt in _split_sql(sql):
        last = spark.sql(stmt)
    return last


print(f"Repo root: {REPO_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Config (always runs first)

# COMMAND ----------

run_sql_file("queries/config.sql")
print("Config loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run selected queries

# COMMAND ----------

selected = [s.strip() for s in dbutils.widgets.get("queries").split(",") if s.strip()]
mode = dbutils.widgets.get("mode")

unknown = [q for q in selected if q not in QUERY_FILES]
if unknown:
    raise ValueError(
        f"Unknown quer{'y' if len(unknown) == 1 else 'ies'} {unknown}. "
        f"Valid choices: {list(QUERY_FILES)}"
    )
print(f"Selected: {selected} | mode: {mode}")

if mode == "parallel" and len(selected) > 1:
    with ThreadPoolExecutor(max_workers=len(selected)) as ex:
        futures = {q: ex.submit(run_sql_file, QUERY_FILES[q]) for q in selected}
        results = {q: f.result() for q, f in futures.items()}
else:
    results = {q: run_sql_file(QUERY_FILES[q]) for q in selected}

print(f"Finished {len(results)} quer{'y' if len(results) == 1 else 'ies'}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Results
# MAGIC One cell per query, so each result gets its own table viewer.
# MAGIC Cells whose query is unselected display nothing.

# COMMAND ----------

# Warehouse recommendations
if "warehouse" in results:
    display(results["warehouse"])

# COMMAND ----------

# Query & table recommendations
if "query_table" in results:
    display(results["query_table"])

# COMMAND ----------

# Jobs → serverless candidacy
if "jobs" in results:
    display(results["jobs"])
