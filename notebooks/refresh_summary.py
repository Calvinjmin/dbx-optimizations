# Databricks notebook source
# MAGIC %md
# MAGIC # WAF — Refresh AI executive summary
# MAGIC
# MAGIC Regenerates the AI executive summary (via `ai_query`) and patches it into
# MAGIC the live dashboard's `ov-ai-summary` text widget, then republishes.
# MAGIC
# MAGIC Run by the `refresh_exec_summary` job — on demand (the dashboard's
# MAGIC "↻ Refresh AI summary" link opens this job's Run page) or manually. The
# MAGIC summary SQL/prompt lives in `dashboards/build_dashboard.py` (single source
# MAGIC of truth) and is imported here, so there's no duplicated logic.
# MAGIC
# MAGIC **Compute:** Serverless (AI Functions + `ai_query` require it).

# COMMAND ----------

dbutils.widgets.text("dashboard_id", "", "Dashboard ID")
dbutils.widgets.text("warehouse_id", "", "Warehouse ID (for publish)")

dashboard_id = dbutils.widgets.get("dashboard_id").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
if not dashboard_id:
    raise ValueError("dashboard_id widget is required.")

# COMMAND ----------

import json
import os
import sys
import time

# Import build_dashboard.py from the deployed bundle files (sibling dir) so the
# generation SQL + prompt are shared with the dashboard build — no duplication.
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
REPO_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
sys.path.insert(0, os.path.join(REPO_ROOT, "dashboards"))
import build_dashboard as bd  # noqa: E402

# COMMAND ----------

# Generate the Markdown summary on this serverless notebook's compute (ai_query
# runs in Spark SQL on serverless). Default filters match the dashboard defaults.
summary_md = bd.summary_markdown_from_sql(
    lambda sql: spark.sql(sql).collect()[0][0]
)
print(summary_md)

# COMMAND ----------

# Patch the summary into the dashboard's text widget and republish.
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

w = WorkspaceClient()
dash = w.lakeview.get(dashboard_id)
spec = json.loads(dash.serialized_dashboard)

patched = False
for page in spec.get("pages", []):
    for widget in page.get("layout", []):
        if widget.get("widget", {}).get("name") == "ov-ai-summary":
            widget["widget"]["multilineTextboxSpec"]["lines"] = [summary_md]
            patched = True
if not patched:
    raise RuntimeError("ov-ai-summary widget not found in dashboard spec.")

# lakeview.update() takes a Dashboard object (not individual kwargs).
w.lakeview.update(
    dashboard_id,
    dashboard=Dashboard(
        display_name=dash.display_name,
        serialized_dashboard=json.dumps(spec),
    ),
)
if warehouse_id:
    w.lakeview.publish(
        dashboard_id, embed_credentials=True, warehouse_id=warehouse_id
    )
    print("Dashboard updated and republished.")
else:
    print("Dashboard draft updated (no warehouse_id provided → not republished).")
