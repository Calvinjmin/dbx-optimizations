-- WAF Recommendations — Shared Configuration
-- Run this FIRST in your session to set up the shared `waf_config` temp view
-- and AI session variables. All three recommendation queries reference values
-- defined here so you only edit thresholds in one place:
--   • sql_query_table_recommendations.sql
--   • sql_warehouse_recommendations.sql
--   • jobs_serverless_candidacy.sql
--
-- Both the temp view and the DECLARE'd variables are session-scoped, so re-run
-- this file whenever you start a new SQL session / notebook.

-- ─────────────────────────────────────────────────────────────────────────────
-- Session variables — used by jobs_serverless_candidacy.sql for ai_query().
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
  -- sql_query_table_recommendations.sql
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
  -- jobs_serverless_candidacy.sql
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
  -- sql_warehouse_recommendations.sql
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
