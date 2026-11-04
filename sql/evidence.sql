-- Evidence and report queries for Athena (workgroup "wistia").
-- Run all:  uv run python scripts/athena_query.py sql/evidence.sql
-- Each query starts with a "-- name:" line; the helper prints the name above the result.

-- name: 1. The simulated week: one row per pipeline run (FR8 evidence)
SELECT run_date, run_id, window_start, window_end, status, events_read, events_new,
       silver_events_total, dq_failed_critical, dq_failed_warning, duration_seconds
FROM wistia_ops.pipeline_runs
ORDER BY started_at;

-- name: 2. Data-quality results that did not pass
SELECT run_date, check_name, severity, failed_rows, details
FROM wistia_ops.dq_results
WHERE NOT passed
ORDER BY run_date, check_name;

-- name: 3. No duplicates after the rerun: number of natural keys that appear more than once (expect 0)
SELECT 'silver.events' AS table_name, COUNT(*) AS duplicated_keys
FROM (SELECT event_key, visitor_key, received_at FROM wistia_silver.events
      GROUP BY event_key, visitor_key, received_at HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'fact_media_engagement', COUNT(*)
FROM (SELECT media_id, visitor_id, date FROM wistia_gold.fact_media_engagement
      GROUP BY media_id, visitor_id, date HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'fact_media_daily', COUNT(*)
FROM (SELECT media_id, date FROM wistia_gold.fact_media_daily GROUP BY media_id, date HAVING COUNT(*) > 1) d;

-- name: 4. Events per media per day (should match Step 1: 8hunphufxp 11, 9k4tbcdfg0 28)
SELECT media_id, event_date, COUNT(*) AS events
FROM wistia_silver.events
GROUP BY media_id, event_date
ORDER BY media_id, event_date;

-- name: 5. Report: daily engagement per channel
SELECT m.channel, f.date, f.load_count, f.play_count, f.play_rate,
       ROUND(f.hours_watched, 2) AS hours_watched, f.unique_visitors
FROM wistia_gold.fact_media_daily f
JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
ORDER BY m.channel, f.date;

-- name: 6. Report: totals per video for the week
SELECT m.title, m.channel, SUM(f.play_count) AS plays,
       COUNT(DISTINCT f.visitor_id) AS visitors,
       ROUND(SUM(f.total_watch_time_seconds) / 3600, 2) AS hours_watched,
       ROUND(AVG(f.watched_percent), 3) AS avg_watched_percent
FROM wistia_gold.fact_media_engagement f
JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
GROUP BY m.title, m.channel
ORDER BY plays DESC;

-- name: 7. Report: plays by traffic source
SELECT f.traffic_source, m.channel, SUM(f.play_count) AS plays, COUNT(DISTINCT f.visitor_id) AS visitors
FROM wistia_gold.fact_media_engagement f
JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
GROUP BY f.traffic_source, m.channel
ORDER BY plays DESC;

-- name: 8. Report: top countries
SELECT v.country, SUM(f.play_count) AS plays, COUNT(DISTINCT f.visitor_id) AS visitors
FROM wistia_gold.fact_media_engagement f
JOIN wistia_gold.dim_visitor v ON v.visitor_id = f.visitor_id
GROUP BY v.country
ORDER BY plays DESC
LIMIT 10;
