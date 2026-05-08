-- SQL Warehouse Right-Sizing Recommendations
-- Reads system.compute.warehouses, system.compute.warehouse_events,
-- system.query.history, system.billing.usage.
-- Tune thresholds in config.sql (run that file FIRST in your session).

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
