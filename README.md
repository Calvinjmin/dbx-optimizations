# Databricks WAF Recommendations

A Well-Architected Framework review kit. Reads Unity Catalog system tables and surfaces actionable cost / performance recommendations for SQL warehouses, queries / tables, and jobs — each row with the criteria that flagged it (the WHY) and the suggested action (the WHAT).

Three interchangeable surfaces over the same SQL:

- **Lakeview dashboard** — five-page executive view with tunable filters and thresholds
- **Runner notebook** — ad-hoc execution with an inline executive briefing
- **Standalone SQL** — composable into your own pipelines

The dashboard is the primary deliverable; the rest is for power users.

## Prerequisites

Run each of these once before you start.

1. **A Databricks workspace** with Unity Catalog enabled and a SQL warehouse (Serverless recommended).
2. **System-table read access** for whoever will view the dashboard. A workspace admin runs (one-time, per schema):
   ```sql
   GRANT USE SCHEMA   ON SCHEMA system.compute TO `account users`;
   GRANT SELECT       ON SCHEMA system.compute TO `account users`;
   -- repeat for: system.query, system.access, system.lakeflow, system.billing
   ```
3. **Databricks CLI ≥ 0.218**, kept current:
   ```bash
   brew install databricks/tap/databricks      # or see the install docs link below
   brew upgrade databricks                      # IMPORTANT — older builds fail with
                                                # "openpgp: key expired" on deploy
   databricks --version                         # confirm 0.218 or higher
   ```
   Non-Homebrew install: https://docs.databricks.com/dev-tools/cli/install.html

## Deploy the dashboard (5 minutes)

Follow these steps in order. Each command is copy-pasteable.

### Step 1 — Clone the repo

```bash
git clone <repo_url>
cd waf-recommendations
```

### Step 2 — Set your workspace URL in `databricks.yml`

Open `databricks.yml`. Find this line under `targets.dev.workspace`:

```yaml
host: https://<your-workspace>.cloud.databricks.com
```

Replace it with your workspace URL. Example:

```yaml
host: https://acme-corp.cloud.databricks.com
```

If you also plan to deploy to `prod`, replace the placeholder under `targets.prod.workspace` the same way.

### Step 3 — Authenticate the CLI to your workspace

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

This opens a browser for OAuth and saves a profile in `~/.databrickscfg`. The `--host` value must match what you put in `databricks.yml`.

### Step 4 — Pick a SQL warehouse

```bash
databricks warehouses list
```

Copy the ID of a Serverless warehouse from the output (looks like `8baced1ff014912d`). The dashboard will run all of its datasets against this warehouse.

### Step 5 — Deploy

```bash
databricks bundle deploy --var sql_warehouse_id=<id_from_step_4>
```

The output ends with the workspace path where the dashboard was created, e.g. `/Workspace/Users/you@company.com/.bundle/waf-recommendations/dev/files`. Open the workspace in your browser, navigate there, and click the dashboard to view it.

### Step 6 — (Optional) Publish

By default the dashboard is in draft. To make it shareable, open it in the Databricks UI and click **Publish** in the top-right.

## Update or remove

To re-deploy after pulling new changes:

```bash
databricks bundle deploy --var sql_warehouse_id=<id>
```

To remove everything the bundle created:

```bash
databricks bundle destroy --var sql_warehouse_id=<id>
```

**Tip:** set `BUNDLE_VAR_sql_warehouse_id=<id>` in your shell to drop the `--var` flag.

## Tune the recommendations

| Where | What you can change | How |
|---|---|---|
| Dashboard **Filters** page | Lookback window, minimum query duration, minimum job fit score, minimum job 30-day cost; categorical filters on category / fit bucket / risk | Edit values in the widgets; the dashboard re-runs against the new thresholds. No redeploy. |
| `dashboards/build_dashboard.py` | Default values for the Filters-page inputs (`PARAM_DECLS`) and the dozen non-parameterized thresholds (`WAF_CONFIG_CTE`) | Edit the file, run `python3 dashboards/build_dashboard.py` to regenerate the `.lvdash.json`, then `databricks bundle deploy` again. |
| `queries/config.sql` | Same thresholds, for the notebook + SQL-editor paths | Edit the file. The notebook re-reads it on every run. |

## What gets recommended

Every row on every surface includes `selection_criteria` (the exact rule that flagged it) and `recommended_action` (concrete next step).

- **Warehouses** — decommission unused, resize up/down, migrate Classic/Pro → Serverless, raise / cap clusters, tighten auto-stop. Includes projected 30-day $ delta.
- **Queries & tables** — top expensive queries, materialized-view candidates, liquid-clustering candidates (low pruning), high-spill queries, hot tables, sub-second BI-engine workloads.
- **Jobs → Serverless** — scores classic-compute jobs 0–100 for serverless fit (duration + frequency + startup share + predictability + infra-failure history). Init-script users are flagged but not filtered.

## Other ways to run

For when you don't want the bundle deploy:

- **Manual dashboard import** — connect this repo as a Databricks Git folder, click `dashboards/waf_recommendations.lvdash.json`, attach a SQL warehouse, click Publish.
- **Runner notebook** — open `notebooks/run.py` in the Git folder, pick queries via the widget, **Run all**. Renders an executive briefing inline.
- **SQL editor** — run `queries/config.sql` first, then any of the three recommendation queries in the same session.

## Repository layout

```
databricks.yml              Asset bundle definition (edit hosts before deploy)
queries/
  config.sql                Shared thresholds (waf_config view + session vars)
  warehouse_recommendations.sql
  query_table_recommendations.sql
  jobs_serverless_candidacy.sql
notebooks/
  run.py                    Runner notebook + executive-briefing cells
dashboards/
  waf_recommendations.lvdash.json   Lakeview dashboard (5 pages incl. Filters)
  build_dashboard.py                Regenerates the .lvdash.json
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `error downloading Terraform: openpgp: key expired` | Your Databricks CLI is too old. `brew upgrade databricks` (or reinstall via [docs](https://docs.databricks.com/dev-tools/cli/install.html)). If you can't upgrade, set `export DATABRICKS_TF_EXEC_PATH=$(which terraform)` after `brew install terraform`. |
| `default auth: cannot configure default credentials` with `host=https://<your-workspace>...` | You didn't replace the placeholder in `databricks.yml`. Go back to Step 2. |
| `bundle deploy` says **warehouse not found** | The warehouse ID is from a different workspace, or your CLI is logged into a different host than `targets.dev.workspace.host`. Re-run `databricks auth login` against the same host. |
| Dashboard widgets show **Permission denied** on `system.*` | The viewer needs `SELECT` on the relevant system schemas. See Prerequisites step 2. The dashboard runs each query with the viewer's permissions, not the owner's. |
| Dashboard is **slow on first open** | Each dataset scans the full lookback window of system tables. Increase the SQL-warehouse size, or lower `lookback_days` on the Filters page. |
| **No rows** on the Jobs page | `min_fit_score` / `min_classic_dbu_cost_30d` on the Filters page are excluding all candidates. Lower the thresholds. |
