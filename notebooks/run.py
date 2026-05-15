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

# Auto-create / update + publish a Lakeview dashboard at the end of the run.
# Turn off if you only want the in-notebook briefing.
dbutils.widgets.dropdown(
    "deploy_dashboard",
    "yes",
    ["yes", "no"],
    label="Deploy Lakeview dashboard",
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
    Serverless compute blocks `spark.sparkContext` access (JVM_ATTRIBUTE_
    NOT_SUPPORTED) — degrade gracefully there and just run the SQL without
    the UI grouping.
    """
    try:
        sc = spark.sparkContext
    except Exception:
        return _run_and_materialize(rel_path)
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
# MAGIC ## 3. Executive briefing
# MAGIC Headline cost-savings KPIs, breakdown tables ready for charting, and a
# MAGIC consolidated top-actions table. Each cell below renders a small
# MAGIC pre-aggregated DataFrame — click the chart icon at the bottom-left of
# MAGIC the result viewer to switch from table to bar/scatter/etc. Each cell
# MAGIC notes the recommended chart shape.

# COMMAND ----------

from decimal import Decimal

import pandas as pd


def _decimal_to_float(p):
    """Spark DECIMAL columns surface as Python `Decimal` objects in pandas, and
    `Decimal / float` (or `Decimal * float`) raises TypeError. Cast any
    object-dtype column whose first non-null value is a Decimal to float64 so
    downstream arithmetic Just Works."""
    if p is None:
        return None
    for col in p.columns:
        s = p[col]
        if s.dtype == object:
            non_null = s.dropna()
            if not non_null.empty and isinstance(non_null.iloc[0], Decimal):
                p[col] = pd.to_numeric(s, errors="coerce")
    return p


# Recommendation outputs are small (typically <1k rows each), so pandas is fine.
pdf = {
    q: _decimal_to_float(df.toPandas()) if df is not None else None
    for q, df in results.items()
}
wh = pdf.get("warehouse")
qt = pdf.get("query_table")
jb = pdf.get("jobs")


def _nonempty(df):
    return df is not None and not df.empty


# Warehouse DECOMMISSION_CANDIDATE rows have cost_multiplier ≈ 1.0 (no
# resize/type change), so projected_cost_delta_30d = 0 even though the
# realized savings is the full current_cost_30d once deleted. Special-case it
# so the headline KPI reflects the actual opportunity.
if _nonempty(wh):
    wh = wh.copy()
    wh["effective_savings_30d"] = wh.apply(
        lambda r: float(r["current_cost_30d"] or 0)
        if r["category"] == "DECOMMISSION_CANDIDATE"
        else max(0.0, float(r["projected_cost_delta_30d"] or 0)),
        axis=1,
    )

wh_savings_30d = float(wh["effective_savings_30d"].sum()) if _nonempty(wh) else 0.0
wh_current_spend_30d = (
    float(wh["current_cost_30d"].fillna(0).sum()) if _nonempty(wh) else 0.0
)
wh_count = int(wh["warehouse_id"].nunique()) if _nonempty(wh) else 0

qt_compute_hours = (
    float(qt["total_compute_s"].fillna(0).sum() / 3600.0) if _nonempty(qt) else 0.0
)
qt_count = int(len(qt)) if _nonempty(qt) else 0

jb_addressable_spend = (
    float(jb["classic_dbu_cost_30d"].fillna(0).sum()) if _nonempty(jb) else 0.0
)
jb_high_fit = int((jb["fit_score"] >= 70).sum()) if _nonempty(jb) else 0
jb_count = int(len(jb)) if _nonempty(jb) else 0

# Annualized projection: direct warehouse deltas plus a conservative 30%
# efficiency assumption on addressable classic-job spend. Treat as a
# directional headline; the per-row tables remain the source of truth.
JOB_SAVINGS_ASSUMPTION = 0.30
annualized = (wh_savings_30d + JOB_SAVINGS_ASSUMPTION * jb_addressable_spend) * 12


def _kpi_card(label, value, sub=None, color="#0EA47F"):
    sub_html = (
        f'<div style="color:#6b7280;font-size:12px;margin-top:4px">{sub}</div>'
        if sub
        else ""
    )
    return f"""
    <div style="flex:1;min-width:200px;padding:16px;border:1px solid #e5e7eb;
                border-radius:8px;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.04)">
      <div style="color:#6b7280;font-size:11px;text-transform:uppercase;
                  letter-spacing:.06em;font-weight:600">{label}</div>
      <div style="color:{color};font-size:26px;font-weight:600;margin-top:6px">{value}</div>
      {sub_html}
    </div>
    """


cards = [
    _kpi_card(
        "Identified savings (30d)",
        f"${wh_savings_30d:,.0f}",
        "warehouse rec deltas + decommission spend",
    ),
    _kpi_card(
        "Annualized projection",
        f"${annualized:,.0f}",
        f"wh deltas + {int(JOB_SAVINGS_ASSUMPTION * 100)}% on classic-job spend, ×12",
        color="#1F6FEB",
    ),
    _kpi_card(
        "Warehouses to act on",
        f"{wh_count:,}",
        f"${wh_current_spend_30d:,.0f} current 30d spend flagged",
        color="#1F2937",
    ),
    _kpi_card(
        "Query/table optimizations",
        f"{qt_count:,}",
        f"{qt_compute_hours:,.0f} compute hours flagged",
        color="#1F2937",
    ),
    _kpi_card(
        "Jobs → serverless",
        f"{jb_count:,}",
        f"{jb_high_fit} at ≥70 fit · ${jb_addressable_spend:,.0f} addressable",
        color="#1F2937",
    ),
]

displayHTML(
    f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',
                Roboto,sans-serif;padding:4px">
      <div style="display:flex;gap:12px;flex-wrap:wrap">{"".join(cards)}</div>
    </div>
    """
)

# COMMAND ----------

# Warehouse savings by category — recommended chart: BAR
#   Keys: category    Values: savings_30d    (secondary: warehouses)
if _nonempty(wh):
    by_cat = (
        wh.groupby("category", as_index=False)
        .agg(
            savings_30d=("effective_savings_30d", "sum"),
            warehouses=("warehouse_id", "nunique"),
        )
        .sort_values("savings_30d", ascending=False)
    )
    display(by_cat)
else:
    print("Warehouse query not selected — savings-by-category chart skipped.")

# COMMAND ----------

# Top 10 warehouses by 30-day savings — recommended chart: BAR (horizontal)
#   Keys: warehouse_name    Values: savings_30d    Group by: category
if _nonempty(wh):
    top_wh = (
        wh.sort_values("effective_savings_30d", ascending=False)
        .head(10)[
            [
                "warehouse_name",
                "category",
                "effective_savings_30d",
                "current_cost_30d",
                "utilization_pct",
                "recommended_action",
            ]
        ]
        .rename(columns={"effective_savings_30d": "savings_30d"})
    )
    display(top_wh)
else:
    print("Warehouse query not selected — top-warehouses chart skipped.")

# COMMAND ----------

# Query/table candidates: compute hours grouped by category — recommended
# chart: BAR. No direct $ projection here, so compute time is the size-of-prize.
#   Keys: category    Values: compute_hours    (secondary: candidates)
if _nonempty(qt):
    by_cat = (
        qt.assign(compute_hours=qt["total_compute_s"].fillna(0) / 3600.0)
        .groupby("category", as_index=False)
        .agg(compute_hours=("compute_hours", "sum"), candidates=("category", "size"))
        .sort_values("compute_hours", ascending=False)
    )
    display(by_cat)
else:
    print("Query/table query not selected — category chart skipped.")

# COMMAND ----------

# Jobs: serverless fit score vs 30-day classic spend — recommended chart: SCATTER
#   X: fit_score    Y: classic_dbu_cost_30d    Size: run_count_30d
#   Group by: has_init_script_risk   (fit >= 70 is the recommended-action zone)
if _nonempty(jb):
    plot_df = jb.assign(
        has_init_script_risk=jb["init_script_failure_count"].fillna(0).gt(0)
    )[
        [
            "job_name",
            "fit_score",
            "classic_dbu_cost_30d",
            "run_count_30d",
            "has_init_script_risk",
            "avg_run_minutes",
            "p95_p50_ratio",
            "top_reasons",
        ]
    ]
    display(plot_df)
else:
    print("Jobs query not selected — fit-vs-spend scatter skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top consolidated action items
# MAGIC Cross-query ranking by 30-day dollar impact. Warehouses use savings;
# MAGIC jobs use addressable classic spend; query/table rows have no direct $
# MAGIC projection and sort below the monetized items by default.

# COMMAND ----------

action_frames = []
if _nonempty(wh):
    action_frames.append(
        wh.assign(
            source="warehouse",
            target=wh["warehouse_name"],
            impact_usd_30d=wh["effective_savings_30d"],
            detail=wh["recommended_action"],
        )[["source", "category", "target", "impact_usd_30d", "detail"]]
    )
if _nonempty(jb):
    action_frames.append(
        jb.assign(
            source="jobs",
            category="JOB_TO_SERVERLESS",
            target=jb["job_name"],
            impact_usd_30d=jb["classic_dbu_cost_30d"].fillna(0),
            detail=(
                "fit "
                + jb["fit_score"].astype(int).astype(str)
                + "/100 — "
                + jb["top_reasons"].fillna("see migration_checklist")
            ),
        )[["source", "category", "target", "impact_usd_30d", "detail"]]
    )
if _nonempty(qt):
    action_frames.append(
        qt.assign(
            source="query_table",
            target=qt["target"].fillna(qt["query_sample"].str.slice(0, 60) + "…"),
            impact_usd_30d=0.0,
            detail=qt["recommended_action"],
        )[["source", "category", "target", "impact_usd_30d", "detail"]]
    )

if action_frames:
    actions = (
        pd.concat(action_frames, ignore_index=True)
        .sort_values("impact_usd_30d", ascending=False)
        .reset_index(drop=True)
    )
    actions["impact_usd_30d"] = actions["impact_usd_30d"].round(2)
    display(spark.createDataFrame(actions.head(50)))
else:
    print("No results to consolidate.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detailed results
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deploy Lakeview dashboard
# MAGIC Idempotently creates / updates a published dashboard in this user's
# MAGIC workspace. The dashboard datasets query system tables directly (same
# MAGIC SQL as `queries/`, with `waf_config` inlined as a CTE), so no
# MAGIC intermediate tables are written.

# COMMAND ----------

if dbutils.widgets.get("deploy_dashboard") == "yes":
    import importlib.util
    import json as _json

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Dashboard

    # Import the dashboard generator from dashboards/build_dashboard.py without
    # requiring the dashboards/ folder to be a Python package.
    _spec = importlib.util.spec_from_file_location(
        "build_dashboard",
        os.path.join(REPO_ROOT, "dashboards", "build_dashboard.py"),
    )
    _build_mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_build_mod)
    serialized = _json.dumps(_build_mod.build_dashboard_dict())

    w = WorkspaceClient()

    # Pick a warehouse: prefer a running serverless one, then any serverless,
    # then anything. Lakeview needs a warehouse_id to execute dataset queries.
    _whs = list(w.warehouses.list())
    _wh = (
        next(
            (
                x
                for x in _whs
                if getattr(x, "enable_serverless_compute", False)
                and getattr(x.state, "value", None) == "RUNNING"
            ),
            None,
        )
        or next(
            (x for x in _whs if getattr(x, "enable_serverless_compute", False)),
            None,
        )
        or (_whs[0] if _whs else None)
    )
    if _wh is None:
        raise RuntimeError(
            "No SQL warehouse available — create one (Serverless recommended) "
            "and re-run."
        )
    warehouse_id = _wh.id

    display_name = "WAF Recommendations"
    parent_path = REPO_ROOT  # co-locate the dashboard with the repo

    # Look up an existing dashboard at the same path with the same name so we
    # update rather than duplicate on every run.
    existing_id = None
    for d in w.lakeview.list():
        if (
            d.display_name == display_name
            and (d.parent_path or "").rstrip("/") == parent_path.rstrip("/")
        ):
            existing_id = d.dashboard_id
            break

    if existing_id:
        result = w.lakeview.update(
            dashboard_id=existing_id,
            dashboard=Dashboard(
                display_name=display_name,
                warehouse_id=warehouse_id,
                serialized_dashboard=serialized,
            ),
        )
        action = "Updated"
    else:
        result = w.lakeview.create(
            dashboard=Dashboard(
                display_name=display_name,
                warehouse_id=warehouse_id,
                parent_path=parent_path,
                serialized_dashboard=serialized,
            )
        )
        action = "Created"

    # Publish so the dashboard is viewable without opening it in edit mode.
    try:
        w.lakeview.publish(dashboard_id=result.dashboard_id, warehouse_id=warehouse_id)
        published = True
    except Exception as e:
        # Non-fatal: the draft is still usable.
        print(f"  publish skipped: {e}")
        published = False

    _host = w.config.host.rstrip("/")
    print(f"{action} dashboard: {display_name}")
    print(f"  Draft:     {_host}/dashboardsv3/{result.dashboard_id}/edit")
    if published:
        print(f"  Published: {_host}/dashboardsv3/{result.dashboard_id}/published")
else:
    print("Dashboard deploy skipped (widget set to 'no').")
