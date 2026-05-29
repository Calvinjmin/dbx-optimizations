# Databricks WAF Recommendations

A Well-Architected Framework review kit. Reads Unity Catalog system tables and surfaces actionable cost / performance recommendations for SQL warehouses, queries / tables, and jobs — each row with the criteria that flagged it (the WHY) and the suggested action (the WHAT).

## Start here: the dashboard

**The AI/BI (Lakeview) dashboard is the product.** [Deploy it in ~5 minutes](#deploy-the-dashboard-5-minutes) and you get a five-page executive view:

- **AI executive summary** — an `ai_query` (Claude Sonnet 4) narrative on the Overview page: total opportunity + a ranked, confidence-labelled list of priority actions across all three sources. Refresh it on demand with the **↻ Refresh AI summary** link (see [AI executive summary](#ai-executive-summary)).
- **Per-source pages** — Warehouses, Queries & Tables, and Jobs → Serverless, each a sortable table where every row carries its `selection_criteria`, `recommended_action`, and a **confidence** score (0–100).
- **KPIs & charts** — flagged counts, 30-day savings, and category breakdowns.
- **Filters page** — tune thresholds live (lookback, **Min savings 30d ($)**, **Min confidence**, fit score, …) and the per-source pages, counts, and charts re-run instantly.

Most users only ever need the dashboard. Two power-user surfaces run the same SQL:

- **Runner notebook** (`notebooks/run.py`) — ad-hoc execution with an inline HTML executive briefing (same confidence scores, filters, and AI summary as the dashboard).
- **Standalone SQL** (`queries/`) — composable into your own pipelines.

## Prerequisites

Run each of these once before you start.

1. **A Databricks workspace** with Unity Catalog enabled and a **Serverless** SQL warehouse. Serverless is required for the AI executive summary — `ai_query` (AI Functions) only runs on Serverless SQL.
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

Six steps, each copy-pasteable. Run them in order from the repo root.

### Step 1 — Clone the repo and copy the bundle config

```bash
git clone <repo_url>
cd waf-recommendations
cp databricks.example.yml databricks.yml
```

`databricks.yml` is gitignored — your edits in step 4 stay local. `databricks.example.yml` is the upstream template.

### Step 2 — Authenticate the CLI

Replace `<your-workspace>` with your actual workspace subdomain:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

This opens a browser for OAuth and saves a profile in `~/.databrickscfg`.

### Step 3 — Pick a SQL warehouse

```bash
databricks warehouses list
```

Copy the ID of a Serverless warehouse from the output (looks like `8baced1ff014912d`). The dashboard will run all of its datasets against this warehouse.

### Step 4 — Set your values in `databricks.yml`

Open `databricks.yml` and replace the two `default:` placeholders under `variables:`:

```yaml
variables:
  workspace_host:
    default: https://acme-corp.cloud.databricks.com    # ← your workspace URL
  sql_warehouse_id:
    default: 8baced1ff014912d                          # ← ID from step 3
```

Or, if you prefer not to edit the file, pass them as flags in step 5 instead.

### Step 5 — Deploy

```bash
databricks bundle deploy
```

If you skipped step 4, pass the values inline:

```bash
databricks bundle deploy \
  --var workspace_host=https://<your-workspace>.cloud.databricks.com \
  --var sql_warehouse_id=<warehouse_id_from_step_3>
```

The output ends with the workspace path where the dashboard was created, e.g. `/Workspace/Users/you@company.com/.bundle/waf-recommendations/dev/files`. Open the workspace in your browser, navigate there, and click the dashboard.

### Step 6 — (Optional) Publish

By default the dashboard is in draft. To make it shareable, open it in the Databricks UI and click **Publish** in the top-right.

> The deploy also creates a Databricks job, **WAF refresh exec summary** (`refresh_exec_summary`), used to refresh the AI summary — see below.

## AI executive summary

The Overview page opens with an AI-written executive summary: a short paragraph on the total opportunity followed by a ranked list of priority actions, each tagged with a confidence level. It's generated by `ai_query` (Claude Sonnet 4) over the same filtered recommendations the rest of the dashboard shows.

**It's a static snapshot, refreshed on demand.** AI/BI dashboards can't render a live LLM call as formatted text, so the summary is generated once (clean Markdown in a text box) and regenerated by a job when you ask for it:

- Click **↻ Refresh AI summary** on the Overview page → it opens the **WAF refresh exec summary** job → click **Run now**.
- ~3 minutes later the job has regenerated the summary and patched it back into the live dashboard. Reload to see it.

The job (`notebooks/refresh_summary.py`, serverless) reuses the exact summary logic from `dashboards/build_dashboard.py`, so the dashboard and notebook stay in sync. You can also run it from the Jobs UI or on a schedule (add a `schedule:` block to the job in `databricks.yml`).

To set the dashboard's refresh link, point it at the job's Run page when you build the dashboard:

```bash
# after the first deploy, grab the job id, then:
WAF_REFRESH_JOB_URL="https://<your-workspace>.cloud.databricks.com/jobs/<job_id>" \
  python3 dashboards/build_dashboard.py
databricks bundle deploy        # redeploy so the link appears
```

Without `WAF_REFRESH_JOB_URL` the dashboard simply omits the link (the summary still renders and the job is still runnable from the Jobs UI).

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
| Dashboard **Filters** page | Lookback window, minimum query duration, minimum job fit score, minimum job 30-day cost, **Min savings 30d ($)**, **Min confidence (0–100)**; categorical filters on category / fit bucket / risk | Edit values in the widgets; the per-source pages, counts, and charts re-run instantly. No redeploy. (The AI summary is a snapshot — refresh it separately, below.) |
| **↻ Refresh AI summary** link / `refresh_exec_summary` job | Regenerate the AI executive-summary text | Click the link on the Overview page → **Run now**, or run the job from the Jobs UI. See [AI executive summary](#ai-executive-summary). |
| `dashboards/build_dashboard.py` | Default values for the Filters-page inputs (`PARAM_DECLS`), the confidence formulas + summary prompt, and the dozen non-parameterized thresholds (`WAF_CONFIG_CTE`) | Edit the file, run `python3 dashboards/build_dashboard.py` to regenerate the `.lvdash.json`, then `databricks bundle deploy` again. |
| `queries/config.sql` | Same thresholds, for the notebook + SQL-editor paths | Edit the file. The notebook re-reads it on every run. |

## What gets recommended

Every row on every surface includes `selection_criteria` (the exact rule that flagged it), `recommended_action` (concrete next step), and a **confidence** score (0–100).

- **Warehouses** — decommission unused, resize up/down, migrate Classic/Pro → Serverless, raise / cap clusters, tighten auto-stop. Includes projected 30-day $ delta. Confidence is highest for unambiguous calls (e.g. an idle warehouse to decommission) and scales with query volume + the size of the cost delta.
- **Queries & tables** — top expensive queries, materialized-view candidates, liquid-clustering candidates (low pruning), high-spill queries, hot tables, sub-second BI-engine workloads. Confidence scales with execution count + total compute (more evidence = higher confidence).
- **Jobs → Serverless** — scores classic-compute jobs 0–100 for serverless fit (duration + frequency + startup share + predictability + infra-failure history); the fit score doubles as the confidence. Init-script users are flagged but not filtered.

The **AI executive summary** then synthesizes the highest-impact moves across all three into a short narrative with per-action confidence labels — see [AI executive summary](#ai-executive-summary).

## Other ways to run

For when you don't want the bundle deploy:

- **Manual dashboard import** — connect this repo as a Databricks Git folder, click `dashboards/waf_recommendations.lvdash.json`, attach a Serverless SQL warehouse, click Publish.
- **Runner notebook** — open `notebooks/run.py` in the Git folder, pick queries + set the **Min savings** / **Min confidence** widgets, **Run all**. Renders an inline HTML executive briefing with the same confidence scores and AI summary as the dashboard.
- **SQL editor** — run `queries/config.sql` first, then any of the three recommendation queries in the same session.

## Repository layout

```
databricks.example.yml      Bundle template — copy to databricks.yml and edit
databricks.yml              Your local bundle config (gitignored)
                            Deploys the dashboard + the refresh_exec_summary job
queries/
  config.sql                Shared thresholds (waf_config view + session vars)
  warehouse_recommendations.sql
  query_table_recommendations.sql
  jobs_serverless_candidacy.sql
notebooks/
  run.py                    Runner notebook + executive-briefing cells
  refresh_summary.py        Job notebook: regenerates the AI summary + patches
                            it into the live dashboard
dashboards/
  waf_recommendations.lvdash.json   Lakeview dashboard (5 pages incl. Filters)
  build_dashboard.py                Regenerates the .lvdash.json; --refresh-summary
                                    re-runs the AI summary (confidence + prompt live here)
  exec_summary.md                   Baked AI executive-summary snapshot (Markdown)
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
| **AI summary doesn't change** when I move the filters | Expected — the AI summary is a static snapshot, not a live query (AI/BI can't render a live LLM call as formatted text). Refresh it via the **↻ Refresh AI summary** link / `refresh_exec_summary` job. The Min savings / Min confidence filters do affect the live per-source pages, counts, and charts. |
| **AI summary** shows the "not generated yet" placeholder, or the refresh job fails with `ai_query` errors | The summary hasn't been generated, or the warehouse/compute isn't Serverless. Run the `refresh_exec_summary` job (Serverless), or build with `python3 dashboards/build_dashboard.py --refresh-summary --warehouse <id> --profile <name>`. `ai_query` requires Serverless. |
