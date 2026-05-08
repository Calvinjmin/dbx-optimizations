-- Query and Table Optimization Recommendations
-- Reads system.query.history, system.access.table_lineage.
-- Tune thresholds in config.sql (run that file FIRST in your session).
-- Pair with warehouse_recommendations.sql (warehouse-level).

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
