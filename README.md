# dbx-optimizations

Databricks SQL queries for Well-Architected Framework (WAF) reviews. Reads
system tables and surfaces optimization recommendations.

## Layout

```
queries/      tunable SQL — config + three recommendation queries
notebooks/    Python runner notebooks that load and execute the queries
```

### `queries/`

- **`config.sql`** — shared thresholds. Defines the `waf_config` temp view + `enable_ai_reason` / `ai_model` session variables. Run first.
- **`warehouse_recommendations.sql`** — per-warehouse: decommission, resize, migrate to serverless, raise/cap clusters, auto-stop housekeeping. Includes projected $ delta.
- **`query_table_recommendations.sql`** — per-query/table: expensive query review, MV / liquid clustering candidates, high-spill queries, hot tables, sub-second BI engine candidates.
- **`jobs_serverless_candidacy.sql`** — scores classic-compute jobs for serverless migration (0–100), with optional `ai_query()` justification per row.

### `notebooks/`

- **`run.py`** — single runner. Loads each `.sql` file by path (no SQL duplicated). Two widgets: `queries` (multiselect — pick any subset of warehouse / query_table / jobs) and `mode` (`parallel` or `sequential`). Config always runs first.

## Requirements

- Read access to `system.compute.*`, `system.query.history`, `system.access.table_lineage`, `system.lakeflow.*`, `system.billing.*`.
- For the runner notebooks: a Serverless notebook or DBR 14.1+ cluster.
- For standalone SQL: any SQL warehouse (Serverless recommended).

## Run

Connect Databricks to this repo via Git folder, then either:

- Open `notebooks/run.py`, set the widgets, and **Run all**, or
- Run `queries/config.sql` followed by any individual query in the SQL editor (same session).

Tune thresholds in `queries/config.sql`.
