"""Build the WAF Recommendations Lakeview dashboard.

Two entry points:
  * `build_dashboard_dict()` — returns the dashboard as a Python dict.
    Imported by notebooks/run.py to deploy in-memory via the SDK.
  * `python dashboards/build_dashboard.py` — also writes the dict to
    dashboards/waf_recommendations.lvdash.json for asset-bundle deployment
    or manual import.

Why a generator: keeping the SQL as raw Python strings is far more readable
than embedding it inside escaped JSON string literals.
"""

import json
import os

OUT = os.path.join(os.path.dirname(__file__), "waf_recommendations.lvdash.json")

# ─────────────────────────────────────────────────────────────────────────────
# Shared waf_config CTE — mirrors queries/config.sql. Edit thresholds here to
# retune the dashboard. Each dataset query embeds this CTE so dashboard
# refreshes are self-contained (no session-scoped temp view dependency).
# ─────────────────────────────────────────────────────────────────────────────
WAF_CONFIG_CTE = """waf_config AS (
  SELECT
    -- Values marked with `:` are dashboard parameters — exposed as input
    -- widgets on the Filters page. Edit the defaults in PARAM_DECLS below.
    make_dt_interval(:lookback_days) AS lookback,
    :min_query_duration_s              AS min_query_duration_s,
    20    AS top_expensive_queries_n,
    20    AS mv_min_executions,
    300.0 AS mv_min_total_compute_s,
    1.0   AS pruning_min_read_gb,
    20.0  AS pruning_max_pct,
    3     AS pruning_min_executions,
    1.0   AS spill_min_avg_gb,
    3     AS spill_min_executions,
    50    AS hot_table_min_queries,
    100.0 AS hot_table_min_compute_s,
    5000  AS subsecond_min_queries,
    5.0   AS subsecond_max_p50_s,
    3     AS subsecond_min_distinct_users,
    5.0   AS classic_startup_minutes,
    :min_classic_dbu_cost_30d AS min_classic_dbu_cost_30d,
    3     AS min_runs_30d,
    15.0  AS short_run_threshold_minutes,
    90.0  AS long_run_threshold_minutes,
    20    AS high_frequency_runs_30d,
    25.0  AS high_startup_share_pct,
    2.0   AS predictable_p95_p50_ratio,
    5.0   AS unpredictable_p95_p50_ratio,
    :min_fit_score AS min_fit_score,
    '^(test-|dev-|scratch-)' AS jobs_exclude_name_pattern,
    map(
      '2X_SMALL',  4.0,  'X_SMALL',   6.0,  'SMALL',    12.0,
      'MEDIUM',   24.0,  'LARGE',    40.0,  'X_LARGE',  80.0,
      '2X_LARGE',144.0,  '3X_LARGE',272.0,  '4X_LARGE',528.0
    ) AS dbu_rate,
    map('CLASSIC', 0.22, 'PRO', 0.55, 'SERVERLESS', 0.70) AS type_rate,
    25.0  AS migration_min_idle_pct,
    0.95  AS migration_max_cost_ratio,
    0.1   AS size_up_min_spill_gb_per_query,
    5.0   AS size_up_min_avg_queue_s,
    30.0  AS size_down_max_p50_s,
    60.0  AS size_down_max_p95_s,
    5.0   AS size_down_max_avg_queue_s,
    10    AS size_down_min_queries,
    10.0  AS queries_per_cluster_target,
    30    AS max_cluster_ceiling,
    10    AS auto_stop_threshold_minutes,
    10    AS auto_stop_target_minutes,
    5.0   AS decommission_min_cost_30d,
    0.05  AS cost_multiplier_floor,
    80.0  AS raise_warn_high_util_pct,
    30.0  AS raise_warn_modest_util_pct,
    5.0   AS min_delta_threshold,
    '^(lakebridge-warehouse-[0-9]+|cleanup-auto-created-warehouse)$'
          AS warehouse_exclude_name_pattern
)"""


# ─────────────────────────────────────────────────────────────────────────────
# Dataset SQL — each wraps the corresponding queries/*.sql with the config CTE
# inlined and a small projection of derived columns for the dashboard widgets.
# ─────────────────────────────────────────────────────────────────────────────

WAREHOUSE_SQL = f"""WITH
{WAF_CONFIG_CTE},

current_warehouses AS (
  SELECT warehouse_id, warehouse_name, warehouse_size, warehouse_type,
         auto_stop_minutes, max_clusters, min_clusters
  FROM system.compute.warehouses
  QUALIFY ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1
),

state_windows AS (
  SELECT warehouse_id, event_time AS w_start, cluster_count,
         LEAD(event_time) OVER (PARTITION BY warehouse_id ORDER BY event_time) AS w_end
  FROM system.compute.warehouse_events
  WHERE event_time >= NOW() - (SELECT lookback FROM waf_config)
),

on_time AS (
  SELECT warehouse_id,
         SUM(UNIX_TIMESTAMP(w_end) - UNIX_TIMESTAMP(w_start)) AS on_s,
         SUM((UNIX_TIMESTAMP(w_end) - UNIX_TIMESTAMP(w_start)) * cluster_count) AS cluster_on_s,
         MAX(cluster_count) AS max_clusters_observed
  FROM state_windows
  WHERE cluster_count > 0 AND w_end IS NOT NULL
  GROUP BY warehouse_id
),

queries AS (
  SELECT compute.warehouse_id AS warehouse_id, start_time, end_time,
         total_task_duration_ms, waiting_for_compute_duration_ms,
         waiting_at_capacity_duration_ms, spilled_local_bytes
  FROM system.query.history
  WHERE start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND compute.warehouse_id IS NOT NULL
),

utilized_seconds AS (
  SELECT s.warehouse_id,
         SUM(UNIX_TIMESTAMP(LEAST(q.end_time, s.w_end))
           - UNIX_TIMESTAMP(GREATEST(q.start_time, s.w_start))) AS util_s
  FROM state_windows s JOIN queries q
    ON q.warehouse_id = s.warehouse_id
   AND q.start_time < s.w_end AND q.end_time > s.w_start
  WHERE s.cluster_count > 0
  GROUP BY s.warehouse_id
),

query_stats AS (
  SELECT warehouse_id,
         COUNT(*) AS query_count,
         PERCENTILE_APPROX(total_task_duration_ms/1000.0, 0.5)  AS p50_task_s,
         PERCENTILE_APPROX(total_task_duration_ms/1000.0, 0.95) AS p95_task_s,
         AVG(COALESCE(waiting_for_compute_duration_ms,0)
           + COALESCE(waiting_at_capacity_duration_ms,0))/1000.0 AS avg_queue_s,
         SUM(spilled_local_bytes)/POW(1024,3) AS spill_gb
  FROM queries
  GROUP BY warehouse_id
),

concurrency AS (
  SELECT warehouse_id, MAX(c) AS peak_concurrent
  FROM (
    SELECT warehouse_id, DATE_TRUNC('minute', start_time) AS m, COUNT(*) AS c
    FROM queries GROUP BY 1, 2
  )
  GROUP BY warehouse_id
),

cost AS (
  SELECT u.usage_metadata.warehouse_id AS warehouse_id,
         SUM(u.usage_quantity) AS dbus_30d,
         SUM(u.usage_quantity * p.pricing.effective_list.default) AS dollars_30d
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
   AND u.usage_end_time BETWEEN p.price_start_time
                            AND COALESCE(p.price_end_time, NOW() + INTERVAL 1 DAY)
  WHERE u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND p.currency_code = 'USD'
  GROUP BY u.usage_metadata.warehouse_id
),

base AS (
  SELECT
    w.warehouse_id, w.warehouse_name, w.warehouse_size, w.warehouse_type,
    w.auto_stop_minutes, w.max_clusters, o.max_clusters_observed,
    o.on_s / 3600.0 AS hours_on,
    COALESCE(u.util_s, 0) / 3600.0 AS hours_utilized,
    CASE
      WHEN COALESCE(qs.query_count, 0) = 0 THEN NULL
      ELSE LEAST(100.0,
             ROUND(100.0 * COALESCE(u.util_s, 0) / NULLIF(o.cluster_on_s, 0), 1))
    END AS util_pct,
    GREATEST(0, o.on_s - COALESCE(u.util_s, 0)) / 3600.0 AS hours_idle,
    qs.query_count, qs.p50_task_s, qs.p95_task_s, qs.avg_queue_s, qs.spill_gb,
    c.peak_concurrent,
    cost.dollars_30d, cost.dbus_30d
  FROM current_warehouses w
  LEFT JOIN on_time          o    USING (warehouse_id)
  LEFT JOIN utilized_seconds u    USING (warehouse_id)
  LEFT JOIN query_stats      qs   USING (warehouse_id)
  LEFT JOIN concurrency      c    USING (warehouse_id)
  LEFT JOIN cost             cost USING (warehouse_id)
  WHERE o.on_s IS NOT NULL
),

recos AS (
  SELECT b.*,
    CASE
      WHEN warehouse_type = 'SERVERLESS' THEN warehouse_type
      WHEN warehouse_type IN ('CLASSIC','PRO')
       AND COALESCE((100.0 - util_pct), 100) > (SELECT migration_min_idle_pct FROM waf_config)
       AND ((SELECT type_rate FROM waf_config)['SERVERLESS']
            / NULLIF((SELECT type_rate FROM waf_config)[warehouse_type], 0))
           * COALESCE(hours_utilized / NULLIF(hours_on, 0), 0)
           < (SELECT migration_max_cost_ratio FROM waf_config)
        THEN 'SERVERLESS'
      ELSE warehouse_type
    END AS rec_type,
    CASE
      WHEN spill_gb / NULLIF(query_count, 0) > (SELECT size_up_min_spill_gb_per_query FROM waf_config)
        OR avg_queue_s > (SELECT size_up_min_avg_queue_s FROM waf_config) THEN
        CASE warehouse_size
          WHEN '2X_SMALL' THEN 'X_SMALL'  WHEN 'X_SMALL'  THEN 'SMALL'
          WHEN 'SMALL'    THEN 'MEDIUM'   WHEN 'MEDIUM'   THEN 'LARGE'
          WHEN 'LARGE'    THEN 'X_LARGE'  WHEN 'X_LARGE'  THEN '2X_LARGE'
          WHEN '2X_LARGE' THEN '3X_LARGE' WHEN '3X_LARGE' THEN '4X_LARGE'
          ELSE warehouse_size END
      WHEN warehouse_size IN ('LARGE','X_LARGE','2X_LARGE','3X_LARGE','4X_LARGE')
       AND p50_task_s   < (SELECT size_down_max_p50_s FROM waf_config)
       AND p95_task_s   < (SELECT size_down_max_p95_s FROM waf_config)
       AND avg_queue_s  < (SELECT size_down_max_avg_queue_s FROM waf_config)
       AND COALESCE(spill_gb, 0) = 0
       AND query_count >= (SELECT size_down_min_queries FROM waf_config)
      THEN
        CASE warehouse_size
          WHEN 'LARGE'    THEN 'MEDIUM'   WHEN 'X_LARGE'  THEN 'LARGE'
          WHEN '2X_LARGE' THEN 'X_LARGE'  WHEN '3X_LARGE' THEN '2X_LARGE'
          WHEN '4X_LARGE' THEN '3X_LARGE' END
      ELSE warehouse_size
    END AS rec_size,
    CASE
      WHEN COALESCE(peak_concurrent, 0) = 0 THEN 1
      WHEN spill_gb / NULLIF(query_count, 0) > (SELECT size_up_min_spill_gb_per_query FROM waf_config)
        OR avg_queue_s > (SELECT size_up_min_avg_queue_s FROM waf_config)
        THEN max_clusters
      ELSE LEAST(
        (SELECT max_cluster_ceiling FROM waf_config),
        GREATEST(1, CAST(CEIL(peak_concurrent
                              / (SELECT queries_per_cluster_target FROM waf_config)) AS INT))
      )
    END AS rec_max_clusters
  FROM base b
),

projected AS (
  SELECT r.*,
    GREATEST(
      (SELECT cost_multiplier_floor FROM waf_config),
      COALESCE(
        (SELECT dbu_rate FROM waf_config)[rec_size]
        / NULLIF((SELECT dbu_rate FROM waf_config)[warehouse_size], 0)
      , 1.0)
      *
      COALESCE(
        (SELECT type_rate FROM waf_config)[rec_type]
        / NULLIF((SELECT type_rate FROM waf_config)[warehouse_type], 0)
      , 1.0)
      *
      CASE
        WHEN rec_type = 'SERVERLESS' AND warehouse_type != 'SERVERLESS'
        THEN COALESCE(hours_utilized / NULLIF(hours_on, 0), 0)
        ELSE 1.0
      END
    ) AS cost_multiplier
  FROM recos r
),

final AS (
  SELECT
    CASE
      WHEN COALESCE(query_count, 0) = 0
       AND COALESCE(dollars_30d, 0) >= (SELECT decommission_min_cost_30d FROM waf_config)
        THEN 'DECOMMISSION_CANDIDATE'
      WHEN cost_multiplier < 1.0   THEN 'COST_REDUCE'
      WHEN cost_multiplier > 1.0   THEN 'PERFORMANCE'
      WHEN rec_max_clusters > max_clusters THEN 'CONCURRENCY'
      WHEN rec_max_clusters < max_clusters THEN 'RIGHTSIZE'
      WHEN auto_stop_minutes > (SELECT auto_stop_threshold_minutes FROM waf_config)
       AND warehouse_type IN ('CLASSIC','PRO')
        THEN 'HOUSEKEEPING'
      ELSE NULL
    END AS category,
    CASE
      WHEN COALESCE(query_count, 0) = 0
       AND COALESCE(dollars_30d, 0) >= (SELECT decommission_min_cost_30d FROM waf_config)
        THEN 'query_count = 0 AND dollars_30d >= decommission_min_cost_30d'
      WHEN cost_multiplier < 1.0
        THEN 'cost_multiplier < 1.0 (rec_size smaller OR migrate to serverless saves >= 5%) AND |delta| >= min_delta_threshold'
      WHEN cost_multiplier > 1.0
        THEN 'cost_multiplier > 1.0 (size-up: spill_per_query > size_up_min_spill_gb_per_query OR avg_queue > size_up_min_avg_queue_s) AND |delta| >= min_delta_threshold'
      WHEN rec_max_clusters > max_clusters
        THEN 'CEIL(peak_concurrent / queries_per_cluster_target) > current max_clusters (target capped at max_cluster_ceiling)'
      WHEN rec_max_clusters < max_clusters
        THEN 'CEIL(peak_concurrent / queries_per_cluster_target) < current max_clusters'
      WHEN auto_stop_minutes > (SELECT auto_stop_threshold_minutes FROM waf_config)
       AND warehouse_type IN ('CLASSIC','PRO')
        THEN 'auto_stop_minutes > auto_stop_threshold_minutes AND warehouse_type IN (CLASSIC, PRO) AND no other action triggered'
      ELSE NULL
    END AS selection_criteria,
    CASE
      WHEN COALESCE(query_count, 0) = 0
       AND COALESCE(dollars_30d, 0) >= (SELECT decommission_min_cost_30d FROM waf_config)
        THEN CONCAT('DELETE - 0 queries / 30d, $',
                    ROUND(COALESCE(dollars_30d, 0), 2), ' burned')
      ELSE NULLIF(
        CONCAT_WS(' + ',
          CASE
            WHEN rec_type != warehouse_type AND rec_size != warehouse_size THEN
              CONCAT('MIGRATE ', warehouse_type, ' -> SERVERLESS + RESIZE ',
                     warehouse_size, ' -> ', rec_size)
            WHEN rec_type != warehouse_type THEN
              CONCAT('MIGRATE ', warehouse_type, ' -> SERVERLESS')
            WHEN rec_size != warehouse_size THEN
              CONCAT('RESIZE ', warehouse_size, ' -> ', rec_size)
          END,
          CASE
            WHEN rec_max_clusters > max_clusters THEN
              CONCAT('RAISE max_clusters ', max_clusters, ' -> ', rec_max_clusters)
            WHEN rec_max_clusters < max_clusters THEN
              CONCAT('CAP max_clusters ', max_clusters, ' -> ', rec_max_clusters)
          END,
          CASE
            WHEN auto_stop_minutes > (SELECT auto_stop_threshold_minutes FROM waf_config)
             AND warehouse_type IN ('CLASSIC','PRO') THEN
              CONCAT('AUTO-STOP ', auto_stop_minutes, 'm -> ',
                     (SELECT auto_stop_target_minutes FROM waf_config), 'm')
          END
        ), '')
    END AS recommended_action,
    warehouse_id, warehouse_name,
    warehouse_size AS current_size, rec_size,
    warehouse_type AS current_type, rec_type,
    max_clusters   AS current_max_clusters, rec_max_clusters,
    auto_stop_minutes,
    util_pct AS utilization_pct,
    ROUND(hours_idle, 1) AS idle_hours_30d,
    query_count,
    ROUND(p50_task_s, 1) AS p50_task_s,
    ROUND(p95_task_s, 1) AS p95_task_s,
    ROUND(avg_queue_s, 1) AS avg_queue_s,
    ROUND(spill_gb, 2)    AS spill_gb,
    peak_concurrent,
    CAST(ROUND(dollars_30d, 2) AS DOUBLE)                          AS current_cost_30d,
    CAST(ROUND(dollars_30d * cost_multiplier, 2) AS DOUBLE)        AS projected_cost_30d,
    CAST(ROUND(dollars_30d * (1 - cost_multiplier), 2) AS DOUBLE)  AS projected_cost_delta_30d,
    CAST(ROUND(100.0 * (1 - cost_multiplier), 1) AS DOUBLE)        AS cost_delta_pct
  FROM projected
)

SELECT
  category,
  selection_criteria,
  recommended_action,
  warehouse_id, warehouse_name,
  current_size, rec_size, current_type, rec_type,
  current_max_clusters, rec_max_clusters,
  auto_stop_minutes,
  utilization_pct, idle_hours_30d,
  query_count, p50_task_s, p95_task_s, avg_queue_s, spill_gb, peak_concurrent,
  current_cost_30d, projected_cost_30d, projected_cost_delta_30d, cost_delta_pct,
  -- Headline savings field used by KPIs and charts.
  -- DECOMMISSION rows show no cost_multiplier delta (the warehouse stays the same
  -- size on paper) so we surface the full current_cost_30d as the realized
  -- savings opportunity once it's deleted.
  CASE
    WHEN category = 'DECOMMISSION_CANDIDATE' THEN current_cost_30d
    ELSE GREATEST(0, projected_cost_delta_30d)
  END AS effective_savings_30d
FROM final
WHERE category IS NOT NULL
  AND recommended_action IS NOT NULL
  AND warehouse_name NOT RLIKE '^(lakebridge-warehouse-[0-9]+|cleanup-auto-created-warehouse)$'
  AND (
    category IN ('DECOMMISSION_CANDIDATE','CONCURRENCY','RIGHTSIZE','HOUSEKEEPING')
    OR (category IN ('COST_REDUCE','PERFORMANCE')
        AND ABS(projected_cost_delta_30d) >= 5.0)
  )
"""


QUERY_TABLE_SQL = f"""WITH
{WAF_CONFIG_CTE},

warehouse_names AS (
  SELECT warehouse_id, warehouse_name
  FROM system.compute.warehouses
  QUALIFY ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1
),

all_queries AS (
  SELECT
    statement_id, executed_by,
    compute.warehouse_id AS warehouse_id,
    start_time, total_task_duration_ms, read_bytes, client_application
  FROM system.query.history
  WHERE start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND statement_type = 'SELECT'
    AND execution_status = 'FINISHED'
),

heavy_queries AS (
  SELECT
    aq.statement_id, aq.executed_by, aq.warehouse_id, aq.start_time,
    aq.total_task_duration_ms, aq.read_bytes,
    h.statement_text AS query_text,
    h.pruned_files_bytes AS pruned_bytes,
    h.spilled_local_bytes,
    md5(REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(h.statement_text), "'[^']*'", '?'),
            '\\\\b\\\\d+\\\\b', '?'),
          '\\\\s+', ' ')) AS query_signature
  FROM all_queries aq
  JOIN system.query.history h USING (statement_id)
  WHERE aq.total_task_duration_ms > (SELECT min_query_duration_s * 1000 FROM waf_config)
    AND h.statement_text IS NOT NULL
),

top_expensive_queries AS (
  SELECT *
  FROM (
    SELECT
      'EXPENSIVE_QUERY_REVIEW' AS category,
      'top N query signatures by SUM(total_task_duration_ms)' AS selection_criteria,
      CONCAT('Top query - ', COUNT(*), ' runs, ',
             ROUND(SUM(total_task_duration_ms) / 60000.0, 1), ' min total. ',
             'Open the query profile / review with AI for tuning.') AS recommended_action,
      CAST(NULL AS STRING) AS target,
      SUBSTR(MIN(query_text), 1, 300) AS query_sample,
      COUNT(*) AS exec_count,
      ROUND(SUM(total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
      ROUND(AVG(total_task_duration_ms) / 1000.0, 2) AS avg_compute_s,
      CAST(NULL AS DOUBLE) AS avg_spill_gb,
      CAST(NULL AS DOUBLE) AS pruning_pct,
      ROUND(SUM(read_bytes) / POW(1024, 3), 2) AS total_read_gb
    FROM heavy_queries
    GROUP BY query_signature
  )
  QUALIFY ROW_NUMBER() OVER (ORDER BY total_compute_s DESC)
    <= (SELECT top_expensive_queries_n FROM waf_config)
),

mv_candidates AS (
  SELECT
    'MATERIALIZED_VIEW_CANDIDATE' AS category,
    'same query signature: COUNT(*) >= mv_min_executions AND total compute >= mv_min_total_compute_s' AS selection_criteria,
    CONCAT('Run ', COUNT(*), 'x - total ',
           ROUND(SUM(total_task_duration_ms) / 60000.0, 1), ' min compute. ',
           'CREATE MATERIALIZED VIEW.') AS recommended_action,
    CAST(NULL AS STRING) AS target,
    SUBSTR(MIN(query_text), 1, 300) AS query_sample,
    COUNT(*) AS exec_count,
    ROUND(SUM(total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
    ROUND(AVG(total_task_duration_ms) / 1000.0, 2) AS avg_compute_s,
    CAST(NULL AS DOUBLE) AS avg_spill_gb,
    CAST(NULL AS DOUBLE) AS pruning_pct,
    ROUND(SUM(read_bytes) / POW(1024, 3), 2) AS total_read_gb
  FROM heavy_queries
  GROUP BY query_signature
  HAVING COUNT(*) >= (SELECT mv_min_executions FROM waf_config)
     AND SUM(total_task_duration_ms) / 1000.0 >= (SELECT mv_min_total_compute_s FROM waf_config)
),

low_pruning_queries AS (
  SELECT
    'LIQUID_CLUSTERING_CANDIDATE' AS category,
    'per-query read_bytes >= pruning_min_read_gb AND COUNT(*) >= pruning_min_executions AND aggregate pruning% < pruning_max_pct' AS selection_criteria,
    CONCAT('Reads ', ROUND(SUM(read_bytes) / POW(1024, 3), 1),
           ' GB with only ',
           ROUND(SUM(pruned_bytes) * 100.0
                 / NULLIF(SUM(read_bytes) + SUM(pruned_bytes), 0), 1),
           '% pruned over ', COUNT(*), ' runs. ',
           'ALTER TABLE ... CLUSTER BY (filter columns).') AS recommended_action,
    CAST(NULL AS STRING) AS target,
    SUBSTR(MIN(query_text), 1, 300) AS query_sample,
    COUNT(*) AS exec_count,
    ROUND(SUM(total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
    ROUND(AVG(total_task_duration_ms) / 1000.0, 2) AS avg_compute_s,
    CAST(NULL AS DOUBLE) AS avg_spill_gb,
    ROUND(SUM(pruned_bytes) * 100.0
          / NULLIF(SUM(read_bytes) + SUM(pruned_bytes), 0), 1) AS pruning_pct,
    ROUND(SUM(read_bytes) / POW(1024, 3), 2) AS total_read_gb
  FROM heavy_queries
  WHERE read_bytes >= (SELECT pruning_min_read_gb FROM waf_config) * POW(1024, 3)
  GROUP BY query_signature
  HAVING COUNT(*) >= (SELECT pruning_min_executions FROM waf_config)
     AND SUM(pruned_bytes) * 100.0
         / NULLIF(SUM(read_bytes) + SUM(pruned_bytes), 0)
         < (SELECT pruning_max_pct FROM waf_config)
),

spill_queries AS (
  SELECT
    'HIGH_SPILL_QUERY' AS category,
    'per-row spilled_local_bytes > 0 AND COUNT(*) >= spill_min_executions AND AVG(spill GB) >= spill_min_avg_gb' AS selection_criteria,
    CONCAT('Avg ', ROUND(AVG(spilled_local_bytes) / POW(1024, 3), 1),
           ' GB spill / query over ', COUNT(*), ' runs. ',
           'Size up warehouse or rewrite (broadcast joins, narrow projections).') AS recommended_action,
    CAST(NULL AS STRING) AS target,
    SUBSTR(MIN(query_text), 1, 300) AS query_sample,
    COUNT(*) AS exec_count,
    ROUND(SUM(total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
    ROUND(AVG(total_task_duration_ms) / 1000.0, 2) AS avg_compute_s,
    ROUND(AVG(spilled_local_bytes) / POW(1024, 3), 2) AS avg_spill_gb,
    CAST(NULL AS DOUBLE) AS pruning_pct,
    CAST(NULL AS DOUBLE) AS total_read_gb
  FROM heavy_queries
  WHERE spilled_local_bytes > 0
  GROUP BY query_signature
  HAVING COUNT(*) >= (SELECT spill_min_executions FROM waf_config)
     AND AVG(spilled_local_bytes) / POW(1024, 3) >= (SELECT spill_min_avg_gb FROM waf_config)
),

hot_tables AS (
  SELECT
    'HOT_TABLE_REVIEW' AS category,
    'per source_table_full_name: COUNT(DISTINCT entity_id) >= hot_table_min_queries AND total compute >= hot_table_min_compute_s' AS selection_criteria,
    CONCAT('Read by ', COUNT(DISTINCT tl.entity_id), ' queries / ',
           COUNT(DISTINCT tl.created_by), ' users. ',
           'High-leverage target - review LIQUID CLUSTERING + ENABLE PREDICTIVE OPTIMIZATION.') AS recommended_action,
    tl.source_table_full_name AS target,
    CAST(NULL AS STRING) AS query_sample,
    COUNT(DISTINCT tl.entity_id) AS exec_count,
    ROUND(SUM(qh.total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
    CAST(NULL AS DOUBLE) AS avg_compute_s,
    ROUND(SUM(qh.spilled_local_bytes) / POW(1024, 3), 2) AS avg_spill_gb,
    CAST(NULL AS DOUBLE) AS pruning_pct,
    ROUND(SUM(qh.read_bytes) / POW(1024, 3), 2) AS total_read_gb
  FROM system.access.table_lineage tl
  LEFT JOIN heavy_queries qh ON qh.statement_id = tl.entity_id
  WHERE tl.event_date >= DATE_SUB(CURRENT_DATE(), :lookback_days)
    AND tl.source_type = 'TABLE'
    AND tl.source_table_full_name IS NOT NULL
  GROUP BY tl.source_table_full_name
  HAVING COUNT(DISTINCT tl.entity_id) >= (SELECT hot_table_min_queries FROM waf_config)
     AND COALESCE(SUM(qh.total_task_duration_ms) / 1000.0, 0)
         >= (SELECT hot_table_min_compute_s FROM waf_config)
),

subsecond_bi_candidates AS (
  SELECT
    'SUB_SECOND_BI_ENGINE_CANDIDATE' AS category,
    'per warehouse: COUNT(*) >= subsecond_min_queries AND p50 < subsecond_max_p50_s AND COUNT(DISTINCT executed_by) >= subsecond_min_distinct_users' AS selection_criteria,
    CONCAT(COUNT(*), ' queries / ',
           COUNT(DISTINCT executed_by), ' users / p50 ',
           ROUND(PERCENTILE_APPROX(total_task_duration_ms / 1000.0, 0.5), 2), 's, p95 ',
           ROUND(PERCENTILE_APPROX(total_task_duration_ms / 1000.0, 0.95), 2), 's. ',
           'High-volume BI workload - qualifies for sub-second BI engine evaluation.') AS recommended_action,
    CONCAT('warehouse: ', COALESCE(wn.warehouse_name, '?'), ' (', aq.warehouse_id, ')') AS target,
    CAST(NULL AS STRING) AS query_sample,
    COUNT(*) AS exec_count,
    ROUND(SUM(total_task_duration_ms) / 1000.0, 1) AS total_compute_s,
    ROUND(AVG(total_task_duration_ms) / 1000.0, 2) AS avg_compute_s,
    CAST(NULL AS DOUBLE) AS avg_spill_gb,
    CAST(NULL AS DOUBLE) AS pruning_pct,
    ROUND(SUM(read_bytes) / POW(1024, 3), 2) AS total_read_gb
  FROM all_queries aq
  LEFT JOIN warehouse_names wn USING (warehouse_id)
  GROUP BY aq.warehouse_id, wn.warehouse_name
  HAVING COUNT(*) >= (SELECT subsecond_min_queries FROM waf_config)
     AND PERCENTILE_APPROX(total_task_duration_ms / 1000.0, 0.5)
         < (SELECT subsecond_max_p50_s FROM waf_config)
     AND COUNT(DISTINCT executed_by) >= (SELECT subsecond_min_distinct_users FROM waf_config)
),

combined AS (
  SELECT * FROM subsecond_bi_candidates
  UNION ALL SELECT * FROM top_expensive_queries
  UNION ALL SELECT * FROM mv_candidates
  UNION ALL SELECT * FROM low_pruning_queries
  UNION ALL SELECT * FROM spill_queries
  UNION ALL SELECT * FROM hot_tables
)

SELECT
  category,
  selection_criteria,
  recommended_action,
  COALESCE(target, SUBSTR(query_sample, 1, 80)) AS target_or_sample,
  target,
  query_sample,
  exec_count,
  total_compute_s,
  CAST(total_compute_s / 3600.0 AS DOUBLE) AS compute_hours,
  avg_compute_s,
  avg_spill_gb,
  pruning_pct,
  total_read_gb
FROM combined
"""


JOBS_SQL = f"""WITH
{WAF_CONFIG_CTE},

current_jobs AS (
  SELECT job_id, workspace_id, name AS job_name, creator_id
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id
                             ORDER BY change_time DESC) = 1
),

run_stats AS (
  SELECT
    workspace_id, job_id,
    COUNT(DISTINCT run_id) AS run_count_30d,
    AVG((UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0)
      AS avg_run_minutes,
    PERCENTILE_APPROX(
      (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0,
      0.5)  AS p50_run_minutes,
    PERCENTILE_APPROX(
      (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0,
      0.95) AS p95_run_minutes,
    100.0 * SUM(
      CASE WHEN (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0
                < (SELECT short_run_threshold_minutes FROM waf_config)
           THEN 1 ELSE 0 END
    ) / NULLIF(COUNT(*), 0) AS short_run_share_pct,
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    SUM(CASE WHEN termination_code IN (
      'CLUSTER_ERROR','DRIVER_ERROR','CLUSTER_REQUEST_LIMIT_EXCEEDED',
      'INSTANCE_UNREACHABLE','UNEXPECTED_LAUNCH_FAILURE'
    ) THEN 1 ELSE 0 END) AS infra_failure_count,
    SUM(CASE WHEN termination_code = 'INIT_SCRIPT_FAILURE' THEN 1 ELSE 0 END)
      AS init_script_failure_count
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND period_end_time IS NOT NULL
  GROUP BY workspace_id, job_id
),

job_billing AS (
  SELECT
    u.workspace_id,
    u.usage_metadata.job_id AS job_id,
    SUM(CASE WHEN u.sku_name ILIKE '%SERVERLESS%' THEN 0
             ELSE u.usage_quantity END) AS classic_dbus_30d,
    SUM(CASE WHEN u.sku_name ILIKE '%SERVERLESS%' THEN u.usage_quantity
             ELSE 0 END)                AS serverless_dbus_30d,
    SUM(CASE WHEN u.sku_name ILIKE '%SERVERLESS%' THEN 0
             ELSE u.usage_quantity * p.pricing.effective_list.default END)
      AS classic_dbu_cost_30d
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
   AND u.usage_end_time BETWEEN p.price_start_time
                            AND COALESCE(p.price_end_time, NOW() + INTERVAL 1 DAY)
  WHERE u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
    AND u.usage_start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND p.currency_code = 'USD'
  GROUP BY u.workspace_id, u.usage_metadata.job_id
),

base AS (
  SELECT
    j.workspace_id, j.job_id, j.job_name, j.creator_id,
    rs.run_count_30d, rs.avg_run_minutes, rs.p50_run_minutes, rs.p95_run_minutes,
    rs.short_run_share_pct, rs.failed_runs, rs.infra_failure_count,
    rs.init_script_failure_count,
    b.classic_dbus_30d, b.classic_dbu_cost_30d, b.serverless_dbus_30d,
    LEAST(0.80,
      (SELECT classic_startup_minutes FROM waf_config) / NULLIF(rs.avg_run_minutes, 0)
    ) AS startup_share,
    rs.p95_run_minutes / NULLIF(rs.p50_run_minutes, 0) AS p95_p50_ratio,
    100.0 * rs.failed_runs / NULLIF(rs.run_count_30d, 0) AS failure_rate_pct,
    rs.run_count_30d / 30.0 AS runs_per_day
  FROM current_jobs j
  JOIN run_stats   rs USING (workspace_id, job_id)
  JOIN job_billing b  USING (workspace_id, job_id)
  WHERE b.classic_dbus_30d > 0
),

scored AS (
  SELECT b.*,
    GREATEST(0, LEAST(30,
      30 * ((SELECT long_run_threshold_minutes FROM waf_config) - avg_run_minutes)
         / NULLIF((SELECT long_run_threshold_minutes FROM waf_config)
                  - (SELECT short_run_threshold_minutes FROM waf_config), 0)
    )) AS score_duration,
    LEAST(20,
      20.0 * run_count_30d / (SELECT high_frequency_runs_30d FROM waf_config)
    ) AS score_frequency,
    LEAST(20,
      20.0 * startup_share * 100 / (SELECT high_startup_share_pct FROM waf_config)
    ) AS score_startup,
    GREATEST(0, LEAST(10,
      10 * ((SELECT unpredictable_p95_p50_ratio FROM waf_config) - p95_p50_ratio)
         / NULLIF((SELECT unpredictable_p95_p50_ratio FROM waf_config)
                  - (SELECT predictable_p95_p50_ratio FROM waf_config), 0)
    )) AS score_predictability,
    CASE WHEN infra_failure_count > 0 THEN 20 ELSE 0 END
      AS score_infra_failures
  FROM base b
),

final AS (
  SELECT
    workspace_id, job_id, job_name, creator_id,
    run_count_30d,
    ROUND(runs_per_day, 1) AS runs_per_day,
    ROUND(avg_run_minutes, 1) AS avg_run_minutes,
    ROUND(p50_run_minutes, 1) AS p50_run_minutes,
    ROUND(p95_run_minutes, 1) AS p95_run_minutes,
    ROUND(p95_p50_ratio, 2)   AS p95_p50_ratio,
    ROUND(short_run_share_pct, 1) AS short_run_share_pct,
    ROUND(100.0 * startup_share, 1) AS startup_share_pct,
    ROUND(failure_rate_pct, 1) AS failure_rate_pct,
    infra_failure_count,
    init_script_failure_count,
    ROUND(classic_dbus_30d, 1)      AS classic_dbus_30d,
    CAST(ROUND(classic_dbu_cost_30d, 2) AS DOUBLE)  AS classic_dbu_cost_30d,
    ROUND(serverless_dbus_30d, 1)   AS serverless_dbus_30d,
    CAST(ROUND(score_duration + score_frequency + score_startup
        + score_predictability + score_infra_failures, 0) AS INT) AS fit_score,
    ROUND(score_duration, 0)       AS pts_duration,
    ROUND(score_frequency, 0)      AS pts_frequency,
    ROUND(score_startup, 0)        AS pts_startup,
    ROUND(score_predictability, 0) AS pts_predictability,
    ROUND(score_infra_failures, 0) AS pts_infra_failures,
    NULLIF(CONCAT_WS('; ',
      CASE WHEN avg_run_minutes <= (SELECT short_run_threshold_minutes FROM waf_config)
           THEN CONCAT('short avg run (', ROUND(avg_run_minutes, 1), ' min)') END,
      CASE WHEN run_count_30d >= (SELECT high_frequency_runs_30d FROM waf_config)
           THEN CONCAT('frequent (', run_count_30d, ' runs/30d)') END,
      CASE WHEN startup_share * 100 >= (SELECT high_startup_share_pct FROM waf_config)
           THEN CONCAT('startup is ', ROUND(startup_share * 100, 0),
                       '% of wall-clock') END,
      CASE WHEN p95_p50_ratio <= (SELECT predictable_p95_p50_ratio FROM waf_config)
           THEN 'consistent runtime (low p95/p50 spread)' END,
      CASE WHEN infra_failure_count > 0
           THEN CONCAT(infra_failure_count,
                       ' infra-flavored failure(s) serverless typically avoids') END
    ), '') AS top_reasons,
    CASE
      WHEN init_script_failure_count > 0
        THEN 'LIKELY BLOCKER: job uses init scripts (init script failures observed). Verify init script content can be replaced by serverless equivalents.'
      ELSE 'Verify before migrating: (1) no init scripts / custom Docker, (2) no DBFS mounts the workload depends on, (3) supported Spark version, (4) no R, no GPU, (5) network access to required private endpoints.'
    END AS migration_checklist
  FROM scored
  WHERE classic_dbu_cost_30d >= (SELECT min_classic_dbu_cost_30d FROM waf_config)
    AND run_count_30d        >= (SELECT min_runs_30d FROM waf_config)
)

SELECT
  job_id, job_name, workspace_id, creator_id,
  fit_score,
  CASE
    WHEN fit_score >= 80 THEN '80-100 (Strong)'
    WHEN fit_score >= 60 THEN '60-79 (Good)'
    WHEN fit_score >= 40 THEN '40-59 (Marginal)'
    ELSE '< 40 (Skip)'
  END AS fit_bucket,
  run_count_30d, runs_per_day,
  avg_run_minutes, p50_run_minutes, p95_run_minutes, p95_p50_ratio,
  short_run_share_pct, startup_share_pct, failure_rate_pct,
  infra_failure_count, init_script_failure_count,
  classic_dbus_30d, classic_dbu_cost_30d, serverless_dbus_30d,
  pts_duration, pts_frequency, pts_startup, pts_predictability, pts_infra_failures,
  top_reasons, migration_checklist,
  CASE WHEN init_script_failure_count > 0 THEN 'BLOCKER' ELSE 'OK' END AS migration_risk
FROM final
WHERE fit_score >= (SELECT min_fit_score FROM waf_config)
  AND job_name NOT RLIKE (SELECT jobs_exclude_name_pattern FROM waf_config)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard parameters — declared on every dataset that uses them (all three
# share WAF_CONFIG_CTE, so each dataset references all four). Defaults match
# queries/config.sql; viewers can override them via the Filters page widgets.
# ─────────────────────────────────────────────────────────────────────────────

def _param(display, keyword, dtype, default):
    return {
        "displayName": display,
        "keyword": keyword,
        "dataType": dtype,
        "defaultSelection": {
            "values": {
                "dataType": dtype,
                "values": [{"value": str(default)}],
            },
        },
    }


PARAM_DECLS = [
    _param("Lookback (days)",          "lookback_days",            "INTEGER", 30),
    _param("Min query duration (s)",   "min_query_duration_s",     "INTEGER", 5),
    _param("Min job fit score",        "min_fit_score",            "INTEGER", 40),
    _param("Min job cost 30d ($)",     "min_classic_dbu_cost_30d", "DECIMAL", "25.0"),
]

ALL_DATASETS = ["warehouse_recos", "query_table_recos", "jobs_recos"]


# ─────────────────────────────────────────────────────────────────────────────
# Widget builders — keep widget-level boilerplate compact.
# ─────────────────────────────────────────────────────────────────────────────

def text(name, lines, x, y, w, h):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def counter_agg(name, title, dataset, expr_name, expression, x, y, w=2, h=3,
                value_format=None):
    enc_value = {"fieldName": expr_name, "displayName": title}
    if value_format:
        enc_value["format"] = value_format
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [{"name": expr_name, "expression": expression}],
                    "disaggregated": False,
                },
            }],
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": enc_value},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def bar_grouped(name, title, dataset, dim_field, agg_field_name, agg_expression,
                x, y, w=3, h=6, dim_display="Category", val_display="Value"):
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [
                        {"name": dim_field, "expression": f"`{dim_field}`"},
                        {"name": agg_field_name, "expression": agg_expression},
                    ],
                    "disaggregated": False,
                },
            }],
            "spec": {
                "version": 3,
                "widgetType": "bar",
                "encodings": {
                    "x": {
                        "fieldName": dim_field,
                        "scale": {"type": "categorical"},
                        "displayName": dim_display,
                    },
                    "y": {
                        "fieldName": agg_field_name,
                        "scale": {"type": "quantitative"},
                        "displayName": val_display,
                    },
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def table(name, title, dataset, columns, x, y, w=6, h=8):
    fields = [{"name": c["field"], "expression": f"`{c['field']}`"} for c in columns]
    encoding_cols = [{"fieldName": c["field"], "displayName": c["display"]} for c in columns]
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": fields,
                    "disaggregated": True,
                },
            }],
            "spec": {
                "version": 2,
                "widgetType": "table",
                "encodings": {"columns": encoding_cols},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def param_widget(name, title, keyword, datasets, x, y, w=2, h=2):
    """Threshold-input widget that drives a dataset parameter across one or
    more datasets. Each dataset's parameter is bound via its own sub-query;
    changing the widget propagates to all bound datasets simultaneously."""
    queries, fields = [], []
    for ds in datasets:
        qname = f"q_{keyword}_{ds}"
        queries.append({
            "name": qname,
            "query": {
                "datasetName": ds,
                "parameters": [{"name": keyword, "keyword": keyword}],
                "disaggregated": False,
            },
        })
        fields.append({"parameterName": keyword, "queryName": qname})
    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": "filter-single-select",
                "encodings": {"fields": fields},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def column_filter(name, title, dataset, field, x, y, w=2, h=2):
    """Categorical multi-select filter bound to an actual column on a dataset.
    Affects every widget on every page that reads from `dataset`."""
    qname = f"q_{name}"
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": qname,
                "query": {
                    "datasetName": dataset,
                    "fields": [{"name": field, "expression": f"`{field}`"}],
                    "disaggregated": False,
                },
            }],
            "spec": {
                "version": 2,
                "widgetType": "filter-multi-select",
                "encodings": {
                    "fields": [{
                        "fieldName": field,
                        "displayName": title,
                        "queryName": qname,
                    }],
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pages
# ─────────────────────────────────────────────────────────────────────────────

overview_page = {
    "name": "overview",
    "displayName": "Overview",
    "pageType": "PAGE_TYPE_CANVAS",
    "layout": [
        text("ov-title", ["## WAF Recommendations - Eligibility Dashboard"], 0, 0, 6, 1),
        text("ov-subtitle",
             ["Surfaces every eligible warehouse, query/table, and job from the WAF queries, "
              "plus the criteria that flagged it. Tabs above drill into each source."],
             0, 1, 6, 1),

        text("ov-section-counts", ["### Eligible items by source"], 0, 2, 6, 1),

        counter_agg("ov-wh-count", "Warehouses flagged", "warehouse_recos",
                    "count(warehouse_id)", "COUNT(`warehouse_id`)", x=0, y=3),
        counter_agg("ov-qt-count", "Query / table optimizations", "query_table_recos",
                    "count(category)", "COUNT(`category`)", x=2, y=3),
        counter_agg("ov-jb-count", "Jobs -> serverless", "jobs_recos",
                    "count(job_id)", "COUNT(`job_id`)", x=4, y=3),

        text("ov-section-money", ["### 30-day cost opportunity"], 0, 6, 6, 1),

        counter_agg("ov-wh-savings", "Warehouse savings (30d)", "warehouse_recos",
                    "sum(effective_savings_30d)", "SUM(`effective_savings_30d`)", x=0, y=7),
        counter_agg("ov-jb-spend", "Addressable classic-job spend (30d)", "jobs_recos",
                    "sum(classic_dbu_cost_30d)", "SUM(`classic_dbu_cost_30d`)", x=2, y=7),
        counter_agg("ov-qt-hours", "Compute hours flagged", "query_table_recos",
                    "sum(compute_hours)", "SUM(`compute_hours`)", x=4, y=7),

        text("ov-section-breakdown", ["### Eligibility breakdown by reason"], 0, 10, 6, 1),

        bar_grouped("ov-wh-by-cat", "Warehouse recommendations by category",
                    "warehouse_recos", "category",
                    "count(warehouse_id)", "COUNT(`warehouse_id`)",
                    x=0, y=11, w=3, h=6, dim_display="Category", val_display="# warehouses"),
        bar_grouped("ov-qt-by-cat", "Query/table candidates by category",
                    "query_table_recos", "category",
                    "count(category)", "COUNT(`category`)",
                    x=3, y=11, w=3, h=6, dim_display="Category", val_display="# candidates"),

        bar_grouped("ov-jb-by-bucket", "Jobs by serverless fit bucket",
                    "jobs_recos", "fit_bucket",
                    "count(job_id)", "COUNT(`job_id`)",
                    x=0, y=17, w=3, h=6, dim_display="Fit bucket", val_display="# jobs"),
        bar_grouped("ov-jb-spend-bucket", "Addressable classic spend by fit bucket",
                    "jobs_recos", "fit_bucket",
                    "sum(classic_dbu_cost_30d)", "SUM(`classic_dbu_cost_30d`)",
                    x=3, y=17, w=3, h=6, dim_display="Fit bucket", val_display="$ classic 30d"),
    ],
}


warehouse_page = {
    "name": "warehouses",
    "displayName": "Warehouses",
    "pageType": "PAGE_TYPE_CANVAS",
    "layout": [
        text("wh-title", ["## Warehouse recommendations"], 0, 0, 6, 1),
        text("wh-subtitle",
             ["Each row is a flagged warehouse. `category` and `selection_criteria` "
              "explain WHY it qualified; `recommended_action` is the WHAT."],
             0, 1, 6, 1),

        counter_agg("wh-kpi-count", "Warehouses flagged", "warehouse_recos",
                    "count(warehouse_id)", "COUNT(`warehouse_id`)", x=0, y=2),
        counter_agg("wh-kpi-savings", "Total savings (30d)", "warehouse_recos",
                    "sum(effective_savings_30d)", "SUM(`effective_savings_30d`)", x=2, y=2),
        counter_agg("wh-kpi-current", "Current spend 30d (flagged whs)", "warehouse_recos",
                    "sum(current_cost_30d)", "SUM(`current_cost_30d`)", x=4, y=2),

        bar_grouped("wh-savings-by-cat", "30-day savings by recommendation category",
                    "warehouse_recos", "category",
                    "sum(effective_savings_30d)", "SUM(`effective_savings_30d`)",
                    x=0, y=5, w=3, h=6, dim_display="Category", val_display="$ savings 30d"),
        bar_grouped("wh-count-by-cat", "Warehouses by category",
                    "warehouse_recos", "category",
                    "count(warehouse_id)", "COUNT(`warehouse_id`)",
                    x=3, y=5, w=3, h=6, dim_display="Category", val_display="# warehouses"),

        table("wh-table", "All flagged warehouses (with eligibility reasons)",
              "warehouse_recos",
              columns=[
                  {"field": "warehouse_name",       "display": "Warehouse"},
                  {"field": "category",             "display": "Category"},
                  {"field": "selection_criteria",   "display": "Why flagged (criteria)"},
                  {"field": "recommended_action",   "display": "Recommended action"},
                  {"field": "current_size",         "display": "Current size"},
                  {"field": "rec_size",             "display": "Rec size"},
                  {"field": "current_type",         "display": "Current type"},
                  {"field": "rec_type",             "display": "Rec type"},
                  {"field": "utilization_pct",      "display": "Util %"},
                  {"field": "idle_hours_30d",       "display": "Idle hrs 30d"},
                  {"field": "current_cost_30d",     "display": "$ now 30d"},
                  {"field": "projected_cost_30d",   "display": "$ projected 30d"},
                  {"field": "effective_savings_30d","display": "$ savings 30d"},
              ],
              x=0, y=11, w=6, h=10),
    ],
}


query_table_page = {
    "name": "query_table",
    "displayName": "Queries & Tables",
    "pageType": "PAGE_TYPE_CANVAS",
    "layout": [
        text("qt-title", ["## Query and table optimizations"], 0, 0, 6, 1),
        text("qt-subtitle",
             ["Each row is a flagged query signature, table, or warehouse workload. "
              "`category` groups by reason type; `selection_criteria` shows the exact rule."],
             0, 1, 6, 1),

        counter_agg("qt-kpi-count", "Candidates flagged", "query_table_recos",
                    "count(category)", "COUNT(`category`)", x=0, y=2),
        counter_agg("qt-kpi-hours", "Total compute hours flagged", "query_table_recos",
                    "sum(compute_hours)", "SUM(`compute_hours`)", x=2, y=2),
        counter_agg("qt-kpi-exec", "Total executions", "query_table_recos",
                    "sum(exec_count)", "SUM(`exec_count`)", x=4, y=2),

        bar_grouped("qt-hours-by-cat", "Compute hours by category",
                    "query_table_recos", "category",
                    "sum(compute_hours)", "SUM(`compute_hours`)",
                    x=0, y=5, w=3, h=6, dim_display="Category", val_display="Compute hours"),
        bar_grouped("qt-count-by-cat", "Candidates by category",
                    "query_table_recos", "category",
                    "count(category)", "COUNT(`category`)",
                    x=3, y=5, w=3, h=6, dim_display="Category", val_display="# candidates"),

        table("qt-table", "All flagged queries/tables (with eligibility reasons)",
              "query_table_recos",
              columns=[
                  {"field": "category",            "display": "Category"},
                  {"field": "selection_criteria",  "display": "Why flagged (criteria)"},
                  {"field": "recommended_action",  "display": "Recommended action"},
                  {"field": "target_or_sample",    "display": "Target / query sample"},
                  {"field": "exec_count",          "display": "Executions"},
                  {"field": "total_compute_s",     "display": "Compute (s)"},
                  {"field": "avg_compute_s",       "display": "Avg compute (s)"},
                  {"field": "avg_spill_gb",        "display": "Avg spill (GB)"},
                  {"field": "pruning_pct",         "display": "Pruning %"},
                  {"field": "total_read_gb",       "display": "Read (GB)"},
              ],
              x=0, y=11, w=6, h=10),
    ],
}


jobs_page = {
    "name": "jobs",
    "displayName": "Jobs -> Serverless",
    "pageType": "PAGE_TYPE_CANVAS",
    "layout": [
        text("jb-title", ["## Job compute -> serverless candidates"], 0, 0, 6, 1),
        text("jb-subtitle",
             ["Each row is a classic-compute job scoring >= 40/100 for serverless migration. "
              "`top_reasons` explains the score; `migration_risk` flags init-script blockers."],
             0, 1, 6, 1),

        counter_agg("jb-kpi-count", "Jobs flagged", "jobs_recos",
                    "count(job_id)", "COUNT(`job_id`)", x=0, y=2),
        counter_agg("jb-kpi-spend", "Addressable spend 30d ($)", "jobs_recos",
                    "sum(classic_dbu_cost_30d)", "SUM(`classic_dbu_cost_30d`)", x=2, y=2),
        counter_agg("jb-kpi-runs", "Total runs 30d", "jobs_recos",
                    "sum(run_count_30d)", "SUM(`run_count_30d`)", x=4, y=2),

        bar_grouped("jb-count-by-bucket", "Jobs by fit bucket",
                    "jobs_recos", "fit_bucket",
                    "count(job_id)", "COUNT(`job_id`)",
                    x=0, y=5, w=3, h=6, dim_display="Fit bucket", val_display="# jobs"),
        bar_grouped("jb-spend-by-bucket", "Addressable spend by fit bucket",
                    "jobs_recos", "fit_bucket",
                    "sum(classic_dbu_cost_30d)", "SUM(`classic_dbu_cost_30d`)",
                    x=3, y=5, w=3, h=6, dim_display="Fit bucket", val_display="$ classic 30d"),

        bar_grouped("jb-risk", "Migration risk distribution",
                    "jobs_recos", "migration_risk",
                    "count(job_id)", "COUNT(`job_id`)",
                    x=0, y=11, w=3, h=6, dim_display="Risk", val_display="# jobs"),
        bar_grouped("jb-pts-breakdown", "Avg score component contribution",
                    "jobs_recos", "fit_bucket",
                    "avg(pts_duration)", "AVG(`pts_duration`)",
                    x=3, y=11, w=3, h=6, dim_display="Fit bucket", val_display="Avg duration pts"),

        table("jb-table", "All flagged jobs (with eligibility reasons)",
              "jobs_recos",
              columns=[
                  {"field": "job_name",                 "display": "Job"},
                  {"field": "fit_score",                "display": "Fit /100"},
                  {"field": "fit_bucket",               "display": "Fit bucket"},
                  {"field": "top_reasons",              "display": "Why flagged (top reasons)"},
                  {"field": "migration_risk",           "display": "Risk"},
                  {"field": "migration_checklist",      "display": "Migration checklist"},
                  {"field": "run_count_30d",            "display": "Runs 30d"},
                  {"field": "avg_run_minutes",          "display": "Avg run (m)"},
                  {"field": "p95_p50_ratio",            "display": "p95/p50"},
                  {"field": "startup_share_pct",        "display": "Startup %"},
                  {"field": "failure_rate_pct",         "display": "Fail %"},
                  {"field": "infra_failure_count",      "display": "Infra fails"},
                  {"field": "init_script_failure_count","display": "Init-script fails"},
                  {"field": "classic_dbu_cost_30d",     "display": "$ classic 30d"},
              ],
              x=0, y=17, w=6, h=10),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Assemble + write
# ─────────────────────────────────────────────────────────────────────────────

filters_page = {
    "name": "filters",
    "displayName": "Filters",
    "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
    "layout": [
        # Row 0: numeric threshold inputs (drive dataset parameters).
        param_widget("p-lookback",     "Lookback (days)",        "lookback_days",
                     ALL_DATASETS,            x=0, y=0),
        param_widget("p-min-qdur",     "Min query duration (s)", "min_query_duration_s",
                     ["query_table_recos"],   x=2, y=0),
        param_widget("p-min-fit",      "Min job fit score",      "min_fit_score",
                     ["jobs_recos"],          x=4, y=0),
        param_widget("p-min-jobcost",  "Min job cost 30d ($)",   "min_classic_dbu_cost_30d",
                     ["jobs_recos"],          x=0, y=2),

        # Row 2: categorical filters bound to dataset columns.
        column_filter("f-wh-cat",  "Warehouse category",   "warehouse_recos",   "category",
                      x=2, y=2),
        column_filter("f-qt-cat",  "Query/table category", "query_table_recos", "category",
                      x=4, y=2),
        column_filter("f-jb-bucket", "Job fit bucket",     "jobs_recos",        "fit_bucket",
                      x=0, y=4, w=3),
        column_filter("f-jb-risk",   "Job migration risk", "jobs_recos",        "migration_risk",
                      x=3, y=4, w=3),
    ],
}


def build_dashboard_dict() -> dict:
    """Return the dashboard as a Python dict, ready to serialize for the API."""
    return {
        "datasets": [
            {
                "name": "warehouse_recos",
                "displayName": "Warehouse recommendations",
                "queryLines": [WAREHOUSE_SQL],
                "parameters": PARAM_DECLS,
            },
            {
                "name": "query_table_recos",
                "displayName": "Query / table recommendations",
                "queryLines": [QUERY_TABLE_SQL],
                "parameters": PARAM_DECLS,
            },
            {
                "name": "jobs_recos",
                "displayName": "Job -> serverless candidates",
                "queryLines": [JOBS_SQL],
                "parameters": PARAM_DECLS,
            },
        ],
        "pages": [overview_page, warehouse_page, query_table_page, jobs_page, filters_page],
    }


def main():
    with open(OUT, "w") as f:
        json.dump(build_dashboard_dict(), f, indent=2)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
