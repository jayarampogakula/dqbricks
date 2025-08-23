-- DQBricks — Databricks SQL Notebook (Ready‑made dashboards)
--
-- Use with the auto‑created views from config.yaml:
--   dq.v_dq_rule_results, dq.v_dq_table_health, dq.v_dq_column_nulls
-- Adjust schema qualifiers if you changed them in config.yaml.
--
-- 💡 Tip: After running the cells below once, save each query as a Databricks SQL
-- query and pin them into a dashboard. You can also parameterize with widgets below.

--──────────────────────────────────────────────────────────────────────────────
-- 0) Widgets (filters)
--──────────────────────────────────────────────────────────────────────────────
-- Use these to filter across all queries in the notebook
CREATE WIDGET TEXT w_table DEFAULT "%";
CREATE WIDGET TEXT w_rule DEFAULT "%";
CREATE WIDGET TEXT w_hours DEFAULT "24"; -- lookback window in hours

--──────────────────────────────────────────────────────────────────────────────
-- 1) KPI tiles: latest snapshot (use as individual tile visuals)
--──────────────────────────────────────────────────────────────────────────────
-- Overall pass ratio (latest per table+rule)
SELECT
  AVG(pass_ratio) AS overall_pass_ratio
FROM (
  SELECT table_name, rule_name, pass_ratio,
         ROW_NUMBER() OVER (PARTITION BY table_name, rule_name ORDER BY logged_at DESC) AS rn
  FROM dq.v_dq_rule_results
  WHERE table_name LIKE getArgument('w_table') AND rule_name LIKE getArgument('w_rule')
) t
WHERE rn = 1;

-- Total failed rows in the last X hours
SELECT
  SUM(failed) AS failed_rows_last_X_hours
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table')
  AND rule_name LIKE getArgument('w_rule')
  AND logged_at >= NOW() - INTERVAL getArgument('w_hours') HOURS;

-- Number of tables monitored (latest)
SELECT COUNT(DISTINCT table_name) AS tables_monitored
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table');

--──────────────────────────────────────────────────────────────────────────────
-- 2) Pass‑ratio trend (line chart)
--──────────────────────────────────────────────────────────────────────────────
SELECT
  date_trunc('hour', logged_at) AS ts,
  rule_name,
  AVG(pass_ratio) AS pass_ratio
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table')
  AND rule_name LIKE getArgument('w_rule')
  AND logged_at >= NOW() - INTERVAL getArgument('w_hours') HOURS
GROUP BY 1, 2
ORDER BY 1;

--──────────────────────────────────────────────────────────────────────────────
-- 3) Top failing rules (bar chart)
--──────────────────────────────────────────────────────────────────────────────
SELECT
  rule_name,
  SUM(failed) AS failed_rows,
  AVG(pass_ratio) AS avg_pass_ratio
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table')
  AND rule_name LIKE getArgument('w_rule')
  AND logged_at >= NOW() - INTERVAL getArgument('w_hours') HOURS
GROUP BY rule_name
ORDER BY failed_rows DESC
LIMIT 25;

--──────────────────────────────────────────────────────────────────────────────
-- 4) Table health snapshot (latest per table)
--──────────────────────────────────────────────────────────────────────────────
WITH latest AS (
  SELECT table_name, rule_name, pass_ratio, logged_at,
         ROW_NUMBER() OVER (PARTITION BY table_name, rule_name ORDER BY logged_at DESC) AS rn
  FROM dq.v_dq_rule_results
  WHERE table_name LIKE getArgument('w_table')
)
SELECT
  table_name,
  AVG(pass_ratio) AS avg_pass_ratio,
  SUM(CASE WHEN pass_ratio < 1 THEN 1 ELSE 0 END) AS rules_failing_count,
  MAX(logged_at) AS last_run
FROM latest
WHERE rn = 1
GROUP BY table_name
ORDER BY avg_pass_ratio ASC, table_name;

--──────────────────────────────────────────────────────────────────────────────
-- 5) Column nulls (heatmap‑friendly table)
--  • Convention: name not‑null rules as `nn_<column>` so the column is parsed.
--──────────────────────────────────────────────────────────────────────────────
SELECT
  table_name,
  column_name,
  AVG(null_ratio) AS avg_null_ratio,
  MAX(logged_at)  AS last_seen
FROM dq.v_dq_column_nulls
WHERE table_name LIKE getArgument('w_table')
GROUP BY table_name, column_name
ORDER BY avg_null_ratio DESC;

-- Optional pivoted heatmap (may be wide): one row per table, columns as fields
-- SELECT table_name,
--   MAX(CASE WHEN column_name='email' THEN avg_null_ratio END) AS email_null,
--   MAX(CASE WHEN column_name='phone' THEN avg_null_ratio END) AS phone_null
-- FROM (
--   SELECT table_name, column_name, null_ratio AS avg_null_ratio
--   FROM dq.v_dq_column_nulls
--   WHERE table_name LIKE getArgument('w_table')
-- ) s
-- GROUP BY table_name;

--──────────────────────────────────────────────────────────────────────────────
-- 6) Latest quarantine samples (table)
--──────────────────────────────────────────────────────────────────────────────
SELECT
  _dq_checked_at,
  table_name,
  rule_name,
  rule_type,
  * EXCEPT(_dq_checked_at, table_name, rule_name, rule_type)
FROM dq.quarantine
WHERE date(_dq_checked_at) >= current_date() - 7
  AND table_name LIKE getArgument('w_table')
ORDER BY _dq_checked_at DESC
LIMIT 500;

--──────────────────────────────────────────────────────────────────────────────
-- 7) SLA / threshold breaches (example: rules that averaged < 0.98 in lookback)
--──────────────────────────────────────────────────────────────────────────────
SELECT
  table_name,
  rule_name,
  AVG(pass_ratio) AS avg_pass_ratio
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table')
  AND rule_name LIKE getArgument('w_rule')
  AND logged_at >= NOW() - INTERVAL getArgument('w_hours') HOURS
GROUP BY table_name, rule_name
HAVING AVG(pass_ratio) < 0.98
ORDER BY avg_pass_ratio ASC;

--──────────────────────────────────────────────────────────────────────────────
-- 8) Rule inventory (what's being checked)
--──────────────────────────────────────────────────────────────────────────────
SELECT table_name, rule_name, rule_type, MAX(logged_at) AS last_seen
FROM dq.v_dq_rule_results
WHERE table_name LIKE getArgument('w_table')
  AND rule_name LIKE getArgument('w_rule')
GROUP BY table_name, rule_name, rule_type
ORDER BY table_name, rule_name;
