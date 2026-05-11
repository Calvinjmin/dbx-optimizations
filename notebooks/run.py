# Databricks notebook source
# MAGIC %md
# MAGIC # WAF Recommendations — Runner
# MAGIC
# MAGIC Loads SQL files from `queries/` by path and runs them — no SQL
# MAGIC duplicated in this notebook. Pick one or more queries with the widget;
# MAGIC **all selected queries run at once** on the cluster (materialized before
# MAGIC results render). With multiple selections, table previews are also
# MAGIC started together from one cell.
# MAGIC
# MAGIC The config (`queries/config.sql`) always runs first.
# MAGIC
# MAGIC **Compute:** Serverless notebook (or DBR 14.1+).

# COMMAND ----------

import os
import re
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait

# multiselect's `defaultValue` must be a single choice (Databricks looks it
# up as one literal string in `choices`). Default to one query — open the
# widget dropdown to check the others.
dbutils.widgets.multiselect(
    "queries",
    "warehouse",
    ["warehouse", "query_table", "jobs"],
    label="Queries to run",
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


def _run_and_materialize(rel_path: str):
    """`spark.sql` is lazy until an action. `.cache()` + `.count()` runs the
    query immediately (in the worker thread when using the pool) and keeps
    the result cached so `display()` only reads materialized data."""
    df = run_sql_file(rel_path)
    if df is not None:
        df.cache()
        df.count()
    return df


def _run_with_job_group(query_name: str, rel_path: str):
    """Run one SQL file with a dedicated Spark job group (per-thread).

    Separate job groups make concurrent jobs easier to spot in the Spark UI
    and can improve fairness vs an anonymous mix of actions from one thread.
    """
    sc = spark.sparkContext
    sc.setJobGroup(
        f"waf_runner_{query_name}",
        f"WAF runner — {query_name}",
        interruptOnCancel=False,
    )
    try:
        return _run_and_materialize(rel_path)
    finally:
        sc.clearJobGroup()


def _show_result(title: str, df):
    print(f"### {title}")
    display(df)


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

unknown = [q for q in selected if q not in QUERY_FILES]
if unknown:
    raise ValueError(
        f"Unknown quer{'y' if len(unknown) == 1 else 'ies'} {unknown}. "
        f"Valid choices: {list(QUERY_FILES)}"
    )
print(f"Selected: {selected}")

results = {}
if selected:
    max_workers = min(len(selected), 16)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_query = {
            ex.submit(_run_with_job_group, q, QUERY_FILES[q]): q for q in selected
        }
        wait(future_to_query.keys(), return_when=ALL_COMPLETED)
        results = {future_to_query[fut]: fut.result() for fut in future_to_query}

print(f"Finished {len(results)} quer{'y' if len(results) == 1 else 'ies'}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Results
# MAGIC Selected queries only. Multiple previews start together (same cell);
# MAGIC SQL already ran in §2 with overlap on the cluster.

# COMMAND ----------

to_show = [q for q in selected if q in results and results[q] is not None]
if to_show:
    if len(to_show) == 1:
        _show_result(to_show[0], results[to_show[0]])
    else:
        max_d = min(len(to_show), 16)
        with ThreadPoolExecutor(max_workers=max_d) as ex:
            futs = [ex.submit(_show_result, q, results[q]) for q in to_show]
            wait(futs, return_when=ALL_COMPLETED)
            for fut in futs:
                fut.result()
