-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks WAF Recommendations
-- MAGIC
-- MAGIC All-in-one notebook bundling the three WAF review queries with a shared
-- MAGIC configuration cell. Run cells top-to-bottom (or **Run all**) — the config
-- MAGIC cell creates session-scoped state (`waf_config` temp view +
-- MAGIC `enable_ai_reason` / `ai_model` variables) that the three query cells
-- MAGIC depend on.
-- MAGIC
-- MAGIC **Cells:**
-- MAGIC 1. **Config** — tune all thresholds here.
-- MAGIC 2. **SQL Warehouse Right-Sizing** — resize / migrate / decommission
-- MAGIC    warehouses based on 30d utilization, spill, queue, and cost.
-- MAGIC 3. **Query & Table Optimization** — surface expensive queries, MV /
-- MAGIC    Liquid Clustering candidates, hot tables, sub-second BI workloads.
-- MAGIC 4. **Jobs → Serverless Candidacy** — score classic-compute jobs by how
-- MAGIC    well their runtime profile fits serverless (with optional AI
-- MAGIC    justification per job).
-- MAGIC
-- MAGIC **Requirements:** access to `system.compute.*`, `system.query.history`,
-- MAGIC `system.access.table_lineage`, `system.lakeflow.*`, `system.billing.*`.
-- MAGIC Use a Serverless SQL warehouse (or any cluster with system tables enabled).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Shared Configuration
-- MAGIC
-- MAGIC Sets up `waf_config` (temp view) and the AI session variables. Edit
-- MAGIC values below to tune thresholds — all three query cells read from here.

-- COMMAND ----------

-- WAF Recommendations — Shared Configuration
-- Both the temp view and the DECLARE'd variables are session-scoped, so this
-- cell must run before the three query cells.

-- ─────────────────────────────────────────────────────────────────────────────
-- Session variables — used by the Jobs → Serverless cell for ai_query().
-- ─────────────────────────────────────────────────────────────────────────────
-- Set enable_ai_reason to FALSE to skip the AI column entirely (no LLM cost,
-- no endpoint round-trip — the CASE WHEN short-circuits).
DECLARE OR REPLACE VARIABLE enable_ai_reason BOOLEAN DEFAULT TRUE;
DECLARE OR REPLACE VARIABLE ai_model STRING DEFAULT 'databricks-meta-llama-3-3-70b-instruct';

CREATE OR REPLACE TEMPORARY VIEW waf_config AS
SELECT
  -- ───────────────────────────────────────────────────────────────────────────
  -- Shared
  -- ───────────────────────────────────────────────────────────────────────────
  INTERVAL 30 DAY AS lookback,            -- analysis window for all queries

  -- ───────────────────────────────────────────────────────────────────────────
  -- Query & Table Optimization
  -- ───────────────────────────────────────────────────────────────────────────

  -- Skip noise.
  5     AS min_query_duration_s,

  -- Top expensive queries to surface (review with AI / query profile).
  20    AS top_expensive_queries_n,

  -- Materialized view candidates (same query rerun many times).
  20    AS mv_min_executions,
  300.0 AS mv_min_total_compute_s,

  -- Liquid clustering candidates (low-pruning queries).
  1.0   AS pruning_min_read_gb,
  20.0  AS pruning_max_pct,
  3     AS pruning_min_executions,

  -- Spill (warehouse undersize OR query rewrite).
  1.0   AS spill_min_avg_gb,
  3     AS spill_min_executions,

  -- Hot tables (Liquid Clustering + Predictive Optimization review).
  50    AS hot_table_min_queries,
  100.0 AS hot_table_min_compute_s,

  -- Sub-second BI engine candidates (ClickHouse-class workload signature).
  -- Qualifying signature: high volume, short-median, multi-user BI traffic.
  5000  AS subsecond_min_queries,
  5.0   AS subsecond_max_p50_s,
  3     AS subsecond_min_distinct_users,

  -- ───────────────────────────────────────────────────────────────────────────
  -- Jobs → Serverless Candidacy
  -- ───────────────────────────────────────────────────────────────────────────

  -- Cluster-startup model. Estimated per-run cluster spin-up time on classic.
  -- Use 5.0 if no pools. Drop to 0.5–1.0 if customer uses instance/warm pools.
  5.0   AS classic_startup_minutes,

  -- Inclusion floors (exclude tiny / noisy jobs).
  25.0  AS min_classic_dbu_cost_30d,       -- skip jobs costing less than $X
  3     AS min_runs_30d,                   -- skip jobs that ran < N times

  -- Scoring thresholds. Each signal contributes points; full points at "good",
  -- 0 pts at "bad", linear in between. Total max score = 100.

  -- Duration: shorter = better fit (startup overhead is a bigger %).
  15.0  AS short_run_threshold_minutes,    -- full pts (30) at <= this
  90.0  AS long_run_threshold_minutes,     -- 0 pts at >= this

  -- Frequency: more runs = more startup overhead serverless eliminates.
  20    AS high_frequency_runs_30d,        -- full pts (20) at >= this

  -- Startup share: % of wall-clock spent on classic spin-up.
  25.0  AS high_startup_share_pct,         -- full pts (20) at >= this

  -- Predictability: p95/p50 duration ratio (lower = consistent).
  2.0   AS predictable_p95_p50_ratio,      -- full pts (10) at <= this
  5.0   AS unpredictable_p95_p50_ratio,    -- 0 pts at >= this

  -- Output filters.
  40    AS min_fit_score,                  -- only show jobs scoring >= this
  '^(test-|dev-|scratch-)' AS jobs_exclude_name_pattern,

  -- ───────────────────────────────────────────────────────────────────────────
  -- SQL Warehouse Right-Sizing
  -- ───────────────────────────────────────────────────────────────────────────

  -- Pricing — tune for your region/contract.
  map(
    '2X_SMALL',  4.0,  'X_SMALL',   6.0,  'SMALL',    12.0,
    'MEDIUM',   24.0,  'LARGE',    40.0,  'X_LARGE',  80.0,
    '2X_LARGE',144.0,  '3X_LARGE',272.0,  '4X_LARGE',528.0
  ) AS dbu_rate,
  map('CLASSIC', 0.22, 'PRO', 0.55, 'SERVERLESS', 0.70) AS type_rate,

  -- Migrate Classic/Pro → Serverless.
  25.0  AS migration_min_idle_pct,
  0.95  AS migration_max_cost_ratio,

  -- Size-up triggers.
  0.1   AS size_up_min_spill_gb_per_query,
  5.0   AS size_up_min_avg_queue_s,

  -- Size-down triggers.
  30.0  AS size_down_max_p50_s,
  60.0  AS size_down_max_p95_s,
  5.0   AS size_down_max_avg_queue_s,
  10    AS size_down_min_queries,

  -- Clusters.
  10.0  AS queries_per_cluster_target,
  30    AS max_cluster_ceiling,

  -- Auto-stop.
  10    AS auto_stop_threshold_minutes,
  10    AS auto_stop_target_minutes,

  -- Decommission unused warehouses.
  5.0   AS decommission_min_cost_30d,

  -- Output.
  0.05  AS cost_multiplier_floor,
  80.0  AS raise_warn_high_util_pct,
  30.0  AS raise_warn_modest_util_pct,
  5.0   AS min_delta_threshold,
  '^(lakebridge-warehouse-[0-9]+|cleanup-auto-created-warehouse)$'
        AS warehouse_exclude_name_pattern;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. SQL Warehouse Right-Sizing Recommendations
-- MAGIC
-- MAGIC Reads `system.compute.warehouses`, `system.compute.warehouse_events`,
-- MAGIC `system.query.history`, `system.billing.usage`. Surfaces decommission /
-- MAGIC cost-reduce / performance / concurrency / right-size / housekeeping
-- MAGIC actions per warehouse with projected $ delta.

-- COMMAND ----------

WITH current_warehouses AS (
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
        THEN CONCAT('DELETE — 0 queries / 30d, $',
                    ROUND(COALESCE(dollars_30d, 0), 2), ' burned')
      ELSE NULLIF(
        CONCAT_WS(' + ',
          CASE
            WHEN rec_type != warehouse_type AND rec_size != warehouse_size THEN
              CONCAT('MIGRATE ', warehouse_type, ' → SERVERLESS + RESIZE ',
                     warehouse_size, ' → ', rec_size)
            WHEN rec_type != warehouse_type THEN
              CONCAT('MIGRATE ', warehouse_type, ' → SERVERLESS')
            WHEN rec_size != warehouse_size THEN
              CONCAT('RESIZE ', warehouse_size, ' → ', rec_size)
          END,
          CASE
            WHEN rec_max_clusters > max_clusters THEN
              CONCAT('RAISE max_clusters ', max_clusters, ' → ', rec_max_clusters,
                     CASE
                       WHEN COALESCE(util_pct, 0)
                            >= (SELECT raise_warn_high_util_pct FROM waf_config)
                         THEN ' (peak-cost may grow significantly)'
                       WHEN COALESCE(util_pct, 0)
                            >= (SELECT raise_warn_modest_util_pct FROM waf_config)
                         THEN ' (peak-cost may grow modestly)'
                       ELSE ''
                     END)
            WHEN rec_max_clusters < max_clusters THEN
              CONCAT('CAP max_clusters ', max_clusters, ' → ', rec_max_clusters)
          END,
          CASE
            WHEN auto_stop_minutes > (SELECT auto_stop_threshold_minutes FROM waf_config)
             AND warehouse_type IN ('CLASSIC','PRO') THEN
              CONCAT('AUTO-STOP ', auto_stop_minutes, 'm → ',
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
    ROUND(dollars_30d, 2)                          AS current_cost_30d,
    ROUND(dollars_30d * cost_multiplier, 2)        AS projected_cost_30d,
    ROUND(dollars_30d * (1 - cost_multiplier), 2)  AS projected_cost_delta_30d,
    ROUND(100.0 * (1 - cost_multiplier), 1)        AS cost_delta_pct
  FROM projected
)

SELECT *
FROM final
WHERE category IS NOT NULL
  AND recommended_action IS NOT NULL
  AND ((SELECT warehouse_exclude_name_pattern FROM waf_config) IS NULL
       OR warehouse_name NOT RLIKE (SELECT warehouse_exclude_name_pattern FROM waf_config))
  AND (
    category IN ('DECOMMISSION_CANDIDATE','CONCURRENCY','RIGHTSIZE','HOUSEKEEPING')
    OR (category IN ('COST_REDUCE','PERFORMANCE')
        AND ABS(projected_cost_delta_30d) >= (SELECT min_delta_threshold FROM waf_config))
  )
ORDER BY
  CASE category
    WHEN 'DECOMMISSION_CANDIDATE' THEN 1
    WHEN 'COST_REDUCE'            THEN 2
    WHEN 'PERFORMANCE'            THEN 3
    WHEN 'CONCURRENCY'            THEN 4
    WHEN 'RIGHTSIZE'              THEN 5
    WHEN 'HOUSEKEEPING'           THEN 6
  END,
  CASE
    WHEN category = 'DECOMMISSION_CANDIDATE' THEN current_cost_30d
    ELSE ABS(projected_cost_delta_30d)
  END DESC NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Query & Table Optimization Recommendations
-- MAGIC
-- MAGIC Reads `system.query.history`, `system.access.table_lineage`. Categories:
-- MAGIC sub-second BI engine candidate, expensive query review, materialized
-- MAGIC view candidate, liquid clustering candidate, high spill, hot table.

-- COMMAND ----------

WITH warehouse_names AS (
  SELECT warehouse_id, warehouse_name
  FROM system.compute.warehouses
  QUALIFY ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1
),

-- All finished SELECTs (no duration floor) — used for sub-second BI engine candidates.
all_queries AS (
  SELECT
    statement_id,
    executed_by,
    compute.warehouse_id AS warehouse_id,
    start_time,
    total_task_duration_ms,
    read_bytes,
    client_application
  FROM system.query.history
  WHERE start_time >= NOW() - (SELECT lookback FROM waf_config)
    AND statement_type = 'SELECT'
    AND execution_status = 'FINISHED'
),

-- Non-trivial SELECTs (≥ min_query_duration_s) — used for query/table recs.
heavy_queries AS (
  SELECT
    aq.statement_id,
    aq.executed_by,
    aq.warehouse_id,
    aq.start_time,
    aq.total_task_duration_ms,
    aq.read_bytes,
    h.statement_text AS query_text,
    h.pruned_files_bytes AS pruned_bytes,
    h.spilled_local_bytes,
    md5(REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(h.statement_text), "'[^']*'", '?'),
            '\\b\\d+\\b', '?'),
          '\\s+', ' ')) AS query_signature
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
      CONCAT('Top query — ', COUNT(*), ' runs, ',
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
    CONCAT('Run ', COUNT(*), '× — total ',
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
           'ALTER TABLE … CLUSTER BY (filter columns).') AS recommended_action,
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
           'High-leverage target — review LIQUID CLUSTERING + ENABLE PREDICTIVE OPTIMIZATION.') AS recommended_action,
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
  WHERE tl.event_date >= DATE_SUB(CURRENT_DATE(), 30)
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
           'High-volume BI workload — qualifies for sub-second BI engine evaluation.') AS recommended_action,
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
)

SELECT * FROM subsecond_bi_candidates
UNION ALL SELECT * FROM top_expensive_queries
UNION ALL SELECT * FROM mv_candidates
UNION ALL SELECT * FROM low_pruning_queries
UNION ALL SELECT * FROM spill_queries
UNION ALL SELECT * FROM hot_tables
ORDER BY
  CASE category
    WHEN 'SUB_SECOND_BI_ENGINE_CANDIDATE'             THEN 1
    WHEN 'EXPENSIVE_QUERY_REVIEW'       THEN 2
    WHEN 'MATERIALIZED_VIEW_CANDIDATE'  THEN 3
    WHEN 'LIQUID_CLUSTERING_CANDIDATE'  THEN 4
    WHEN 'HIGH_SPILL_QUERY'             THEN 5
    WHEN 'HOT_TABLE_REVIEW'             THEN 6
  END,
  total_compute_s DESC NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Jobs → Serverless Candidacy Scoring
-- MAGIC
-- MAGIC Ranks classic-compute jobs by how well their runtime profile fits
-- MAGIC serverless. No cost projection — operational signals + classic DBU
-- MAGIC spend as the size-of-opportunity indicator. Optional per-row AI
-- MAGIC justification gated by `enable_ai_reason` from the config cell.

-- COMMAND ----------

WITH current_jobs AS (
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
    -- Infra-flavored terminations that serverless typically eliminates.
    -- INIT_SCRIPT_FAILURE intentionally excluded — it's a migration *blocker*.
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

-- Split usage classic vs serverless so we can (a) require classic spend exists,
-- (b) skip jobs already mostly on serverless.
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
    -- Component scores (sum to 100 max).

    -- (1) Duration: shorter wins. 30 pts at <= short_threshold,
    --     0 pts at >= long_threshold, linear in between.
    GREATEST(0, LEAST(30,
      30 * ((SELECT long_run_threshold_minutes FROM waf_config) - avg_run_minutes)
         / NULLIF((SELECT long_run_threshold_minutes FROM waf_config)
                  - (SELECT short_run_threshold_minutes FROM waf_config), 0)
    )) AS score_duration,

    -- (2) Frequency: more runs = more startup overhead to amortize away.
    --     Capped at 20 pts at high_frequency_runs_30d.
    LEAST(20,
      20.0 * run_count_30d / (SELECT high_frequency_runs_30d FROM waf_config)
    ) AS score_frequency,

    -- (3) Startup share: how much wall-clock serverless skips.
    --     20 pts at high_startup_share_pct, linear from 0.
    LEAST(20,
      20.0 * startup_share * 100 / (SELECT high_startup_share_pct FROM waf_config)
    ) AS score_startup,

    -- (4) Predictability: low p95/p50 = consistent, easier to migrate.
    --     10 pts at <= predictable, 0 pts at >= unpredictable, linear.
    GREATEST(0, LEAST(10,
      10 * ((SELECT unpredictable_p95_p50_ratio FROM waf_config) - p95_p50_ratio)
         / NULLIF((SELECT unpredictable_p95_p50_ratio FROM waf_config)
                  - (SELECT predictable_p95_p50_ratio FROM waf_config), 0)
    )) AS score_predictability,

    -- (5) Infra-flavored failures: 20 pts if any. These are the kinds of
    --     errors serverless typically eliminates (cluster spin-up, driver loss).
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
    init_script_failure_count,    -- > 0 = likely migration blocker

    ROUND(classic_dbus_30d, 1)      AS classic_dbus_30d,
    ROUND(classic_dbu_cost_30d, 2)  AS classic_dbu_cost_30d,
    ROUND(serverless_dbus_30d, 1)   AS serverless_dbus_30d,

    ROUND(score_duration + score_frequency + score_startup
        + score_predictability + score_infra_failures, 0) AS fit_score,

    -- Per-signal score breakdown so the DBA can audit the recommendation.
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
        THEN 'LIKELY BLOCKER: job uses init scripts (init script failures observed). '
             'Verify init script content can be replaced by serverless equivalents.'
      ELSE 'Verify before migrating: (1) no init scripts / custom Docker, '
           '(2) no DBFS mounts the workload depends on, '
           '(3) supported Spark version, (4) no R, no GPU, '
           '(5) network access to required private endpoints.'
    END AS migration_checklist
  FROM scored
  WHERE classic_dbu_cost_30d >= (SELECT min_classic_dbu_cost_30d FROM waf_config)
    AND run_count_30d        >= (SELECT min_runs_30d FROM waf_config)
)

SELECT
  f.*,
  -- Per-job migration justification synthesized from the signals.
  -- Gated by the `enable_ai_reason` variable from the config cell.
  -- The CASE WHEN short-circuits: when the variable is FALSE, ai_query is not
  -- called and there is no LLM cost.
  CASE
    WHEN enable_ai_reason THEN
      ai_query(
        ai_model,
        CONCAT(
          'You are a Databricks Solutions Architect advising a customer DBA on ',
          'migrating a job from classic to serverless job compute. Based on the ',
          '30-day runtime signals below, write a concise 2-3 sentence migration ',
          'justification for THIS specific job. Lead with the single strongest ',
          'signal. Be concrete; do not invent numbers; do not restate every ',
          'metric — pick the 1-2 that matter most. Mention serverless benefits ',
          '(no cluster spin-up, fewer infra failures, predictable performance) ',
          'only when the signals support them.\n\n',
          'Job name: ', f.job_name, '\n',
          'Avg run duration: ', f.avg_run_minutes, ' min ',
            '(p50 ', f.p50_run_minutes, ', p95 ', f.p95_run_minutes,
            ', p95/p50 ratio ', f.p95_p50_ratio, ')\n',
          'Run frequency: ', f.run_count_30d, ' runs in 30 days (',
            f.runs_per_day, '/day)\n',
          'Estimated startup overhead: ~', f.startup_share_pct,
            '% of wall-clock per run\n',
          'Failure rate: ', f.failure_rate_pct, '% ',
            '(', f.infra_failure_count, ' infra-flavored failures: cluster/driver errors)\n',
          'Init-script failures: ', f.init_script_failure_count,
            ' (any > 0 may indicate a migration blocker)\n',
          'Classic DBU cost (30d): USD ', f.classic_dbu_cost_30d, '\n',
          'Fit score: ', f.fit_score, '/100\n',
          'Detected signals: ', COALESCE(f.top_reasons, 'none')
        )
      )
    ELSE NULL
  END AS ai_migration_reason
FROM final f
WHERE fit_score >= (SELECT min_fit_score FROM waf_config)
  AND ((SELECT jobs_exclude_name_pattern FROM waf_config) IS NULL
       OR job_name NOT RLIKE (SELECT jobs_exclude_name_pattern FROM waf_config))
ORDER BY fit_score DESC, classic_dbu_cost_30d DESC NULLS LAST;
