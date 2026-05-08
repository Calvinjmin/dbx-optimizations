-- Job Compute → Serverless Candidacy Scoring
-- Ranks classic-compute jobs by how well their runtime profile fits serverless.
-- No cost projection — just operational signals + the classic DBU spend as the
-- size-of-opportunity indicator.
--
-- Tune thresholds + the AI on/off toggle in config.sql (run that file
-- FIRST in your session). The AI column is gated by the `enable_ai_reason`
-- session variable defined there — set to FALSE to skip ai_query() entirely.

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
  -- Gated by the `enable_ai_reason` variable at the top of the file.
  -- The CASE WHEN short-circuits: when the variable is FALSE, ai_query is not
  -- called and there is no LLM cost.
  CASE
    WHEN enable_ai_reason THEN
      ai_query(
        ai_model,  -- swap model here
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
