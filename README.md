# dbx-optimizations

A small set of Databricks SQL queries for Well-Architected Framework (WAF)
reviews — surfaces recommendations for SQL warehouse right-sizing, query &
table optimization, and jobs serverless candidacy. All queries read from
Databricks system tables.

## What's in the repo

| File | Purpose |
| --- | --- |
| [`waf_config.sql`](waf_config.sql) | Shared configuration. Run **first** in your session — defines `waf_config` (temp view) and the `enable_ai_reason` / `ai_model` session variables that the three query files reference. Tune all thresholds here. |
| [`sql_warehouse_recommendations.sql`](sql_warehouse_recommendations.sql) | Per-warehouse actions: decommission, cost-reduce, performance, concurrency, right-size, housekeeping. Includes projected $ delta. |
| [`sql_query_table_recommendations.sql`](sql_query_table_recommendations.sql) | Per-query / per-table actions: expensive query review, materialized view candidates, liquid clustering candidates, high-spill queries, hot tables, sub-second BI engine candidates. |
| [`jobs_serverless_candidacy.sql`](jobs_serverless_candidacy.sql) | Scores classic-compute jobs (0–100) for serverless migration fit, with optional per-row `ai_query()` justification. |
| [`waf_recommendations_notebook.sql`](waf_recommendations_notebook.sql) | All-in-one Databricks notebook (source format) that bundles the config cell + three query cells. One-click variant of the four files above. |

## Requirements

- A **Serverless SQL warehouse** (or any compute with system tables enabled).
- Read access to:
  - `system.compute.warehouses`, `system.compute.warehouse_events`
  - `system.query.history`
  - `system.access.table_lineage`
  - `system.lakeflow.jobs`, `system.lakeflow.job_run_timeline`
  - `system.billing.usage`, `system.billing.list_prices`
- For the AI column in `jobs_serverless_candidacy.sql`: a Foundation Model
  endpoint reachable via `ai_query()` (default: `databricks-meta-llama-3-3-70b-instruct`).

## How to run

There are two equivalent ways. Use whichever fits the audience.

### Option A — Notebook (one click)

Use the bundled notebook when you want a single artifact to hand off.

1. In your Databricks workspace: **Workspace → ⋮ → Create → Git folder**.
2. Repo URL: `https://github.com/Calvinjmin/dbx-optimizations.git`, branch `main`.
3. Open `waf_recommendations_notebook.sql`. It appears as a notebook with
   four cells: Config, Warehouse Recs, Query/Table Recs, Jobs Candidacy.
4. Attach a Serverless SQL warehouse and click **Run all**.

### Option B — Individual files (better for iteration)

Use the standalone files when you want to tune thresholds and re-run a
single query in a loop.

1. Open the SQL Editor or a SQL notebook.
2. Run [`waf_config.sql`](waf_config.sql) once. This creates the
   session-scoped `waf_config` temp view + `enable_ai_reason` / `ai_model`
   variables.
3. Run any of the three recommendation queries in the **same session**.
   Re-run `waf_config.sql` whenever you start a new session.

## Tuning

All thresholds live in [`waf_config.sql`](waf_config.sql), grouped by query.
Edit values, re-run the config cell/file, then re-run the queries. Common
knobs:

- `lookback` — analysis window (default `INTERVAL 30 DAY`, shared by all queries).
- `dbu_rate` / `type_rate` — pricing maps for projected $ delta in warehouse recs. Tune for your region/contract.
- `min_fit_score` — output floor for jobs candidacy (default `40`).
- `jobs_exclude_name_pattern` / `warehouse_exclude_name_pattern` — regex filters to skip non-prod resources.
- `enable_ai_reason` — set to `FALSE` to skip the `ai_query()` column (no LLM cost, no endpoint round-trip).

## Output

Each recommendation query returns a flat table with at minimum:

- `category` — the type of recommendation (e.g., `COST_REDUCE`, `LIQUID_CLUSTERING_CANDIDATE`).
- `selection_criteria` — the rule that triggered the row.
- `recommended_action` — human-readable next step.
- Supporting metrics (counts, durations, costs, percentages).

Sort order is by category priority then by impact (cost delta or compute time).
