# Databricks Optimizations 

Databricks SQL queries for Well-Architected Framework (WAF) reviews. Reads system tables and surfaces optimization recommendations.

## Layout

```
queries/      tunable SQL — config + three recommendation queries
notebooks/    Python runner notebooks that load and execute the queries
dashboards/   Lakeview dashboard (eligible items + reasons)
```

### `queries/`

- **`config.sql`** — shared thresholds. Defines the `waf_config` temp view + `enable_ai_reason` / `ai_model` session variables. Run first.
- **`warehouse_recommendations.sql`** — per-warehouse: decommission, resize, migrate to serverless, raise/cap clusters, auto-stop housekeeping. Includes projected $ delta.
- **`query_table_recommendations.sql`** — per-query/table: expensive query review, MV / liquid clustering candidates, high-spill queries, hot tables, sub-second BI engine candidates.
- **`jobs_serverless_candidacy.sql`** — scores classic-compute jobs for serverless migration (0–100), with optional `ai_query()` justification per row.

### `notebooks/`

- **`run.py`** — single runner. Loads each `.sql` file by path (no SQL duplicated). Two widgets: `queries` (multiselect — pick any subset of warehouse / query_table / jobs) and `mode` (`parallel` or `sequential`). Config always runs first.

### `dashboards/`

- **`build_dashboard.py`** — generator. Defines the four pages (Overview, Warehouses, Queries & Tables, Jobs → Serverless), each showing the eligible items plus a `selection_criteria` column (WHY) and `recommended_action` column (WHAT). Datasets inline the same thresholds as `queries/config.sql`.
- **`waf_recommendations.lvdash.json`** — pre-built JSON, for asset-bundle deployment or manual import. Regenerate with `python3 dashboards/build_dashboard.py`.

**You don't need to touch this folder** — `notebooks/run.py` imports the generator and deploys + publishes the dashboard in your workspace at the end of every run. The URL is printed in the final cell.

## Requirements

- Read access to `system.compute.*`, `system.query.history`, `system.access.table_lineage`, `system.lakeflow.*`, `system.billing.*`.
- For the runner notebooks: a Serverless notebook or DBR 14.1+ cluster.
- For standalone SQL: any SQL warehouse (Serverless recommended).

## Run

Connect Databricks to this repo via Git folder, then:

1. Open `notebooks/run.py`
2. Leave the widgets at their defaults (or pick more queries / disable dashboard deploy)
3. **Run all**

The notebook runs the selected SQL, renders the in-line executive briefing, and at the end creates/updates a published Lakeview dashboard in your workspace — the URL is printed in the last cell.

For ad-hoc SQL: run `queries/config.sql` then any individual query in the SQL editor (same session).

Tune thresholds in `queries/config.sql` (notebook + SQL editor path) and in `dashboards/build_dashboard.py` (`WAF_CONFIG_CTE`, for the dashboard).
