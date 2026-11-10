# Steps 4–5: Deployment and the simulated 7-day production run (FR8)

Date: 2026-11-10. AWS account region `us-east-1`. All runs below were executed by the **deployed** pipeline
(Step Functions → Lambda → S3 → Glue 5.1 PySpark → Iceberg → DynamoDB), not locally.

## Step 4: deployment

| Step | Command (run by the user) | Result |
|---|---|---|
| 4.1 | `cdk bootstrap aws://<account>/us-east-1` | `CDKToolkit` stack `CREATE_COMPLETE` |
| 4.2 | `aws secretsmanager create-secret --name wistia/api-token ...` (value from `.env`, never printed) | Secret created (64-character value) |
| 4.3 | `cd infra && cdk deploy WistiaPipeline -c alert_email=...` | `WistiaPipeline` `CREATE_COMPLETE` in about 3 minutes |
| 4.4 | `scripts/simulate_week.py --start 2026-03-17 --days 1` (smoke test = day 1 of the week) | `SUCCEEDED` in 3 min 2 s |

Deployed resources (stack outputs): S3 data bucket, DynamoDB watermark table, state machine `wistia-pipeline`,
Glue job `wistia-transform` (Glue 5.1, 2 × G.1X), Athena workgroup `wistia`, daily schedule `wistia-daily` (**DISABLED**).

The first AWS run confirmed what could not be tested locally: Glue 5.1 with Iceberg tables in the Glue Data Catalog,
the write-once S3 upload (`IfNoneMatch`), `run_id` handed from Ingest to Transform by Step Functions, and Athena
reading the silver/gold/ops tables.

## Step 5: the simulated week

FR8 asks for 7 consecutive days in production. As decided on 2026-11-03 (see `docs/decisions.md`), the deployed
pipeline was run once per `run_date`, in order, for 2026-03-17 → 2026-03-23, then 2026-03-20 was run again to prove
that a rerun creates no duplicates:

```bash
uv run python scripts/simulate_week.py --start 2026-03-17 --days 1                     # smoke test (day 1)
uv run python scripts/simulate_week.py --start 2026-03-18 --days 6 --rerun 2026-03-20  # days 2-7 + rerun
uv run python scripts/athena_query.py sql/evidence.sql                                 # evidence below
```

Step Functions history: 8 executions `sim-2026-03-17-…` … `sim-2026-03-23-…` + `sim-2026-03-20-…` (rerun),
all `SUCCEEDED`. Watermarks after the week: both media at `2026-03-23` (the rerun of the 20th did not move them back).

### Evidence 1: one row per run (`wistia_ops.pipeline_runs`)

| run_date | window | status | events_read | events_new | silver_events_total | DQ critical | DQ warning | Glue seconds |
|---|---|---|---|---|---|---|---|---|
| 2026-03-17 | 03-17 → 03-17 | succeeded | 9 | 9 | 9 | 0 | 1 | 84.2 |
| 2026-03-18 | 03-17 → 03-18 | succeeded | 14 | 5 | 14 | 0 | 1 | 93.4 |
| 2026-03-19 | 03-18 → 03-19 | succeeded | 11 | 6 | 20 | 0 | 1 | 143.7 |
| 2026-03-20 | 03-19 → 03-20 | succeeded | 11 | 5 | 25 | 0 | 1 | 83.1 |
| 2026-03-21 | 03-20 → 03-21 | succeeded | 8 | 3 | 28 | 0 | 1 | 78.2 |
| 2026-03-22 | 03-21 → 03-22 | succeeded | 6 | 3 | 31 | 0 | 1 | 159.5 |
| 2026-03-23 | 03-22 → 03-23 | succeeded | 11 | 8 | 39 | 0 | 1 | 104.3 |
| 2026-03-20 (rerun) | 03-20 → 03-20 | succeeded | 5 | **0** | 39 | 0 | 0 | 122.6 |

How to read it:
- **Incremental:** each window starts one day before `run_date` (the 1-day lookback for late events), so
  `events_read` > `events_new`: the re-read events are matched by `MERGE` and updated, not inserted again.
- **Idempotent:** the rerun of 2026-03-20 read 5 events and added 0; `silver_events_total` stays at 39.
- The rerun's window is `03-20 → 03-20` because the window never starts after `run_date`.

### Evidence 2: data-quality warnings

No critical check failed in any run. The only failed check is the warning `reconcile:plays_vs_events`: Wistia's
daily `play_count` (from `by_date`) differs slightly from the number of events the Events API returns for the same
day, e.g. `9k4tbcdfg0 2026-03-17: plays=8 events=9`, `8hunphufxp 2026-03-21: plays=3 events=1`. This is a known
difference in Wistia's own data (decision 2026-11-04), so the check is a warning, recorded in `wistia_ops.dq_results`.

### Evidence 3: no duplicates after the rerun

| table | natural key | duplicated keys |
|---|---|---|
| silver.events | event_key, visitor_key, received_at | 0 |
| fact_media_engagement | media_id, visitor_id, date | 0 |
| fact_media_daily | media_id, date | 0 |

### Evidence 4: totals match Step 1

Events loaded for the week: `8hunphufxp` 11 (1, 1, 3, 1, 3, 2 on 18–23; 0 on 17) and `9k4tbcdfg0` 28
(9, 4, 5, 2, 2, 6; 0 on 22), total **39**, exactly the counts found in the Step 1 API exploration.

## Report results (gold tables)

**Totals per video for the week**

| title | channel | plays | visitors | hours watched | avg watched % |
|---|---|---|---|---|---|
| The Gap Method | Facebook | 28 | 24 | 4.74 | 30.4% |
| rivas_-_de_testimonial (1080p) (1) | YouTube | 11 | 10 | 0.69 | 22.7% |

**Daily engagement per channel** (`fact_media_daily` + `dim_media`)

| date | Facebook loads | Facebook plays | Facebook play rate | YouTube loads | YouTube plays | YouTube play rate |
|---|---|---|---|---|---|---|
| 03-17 | 19 | 8 | 42.1% | 248 | 0 | 0.0% |
| 03-18 | 11 | 4 | 36.4% | 317 | 1 | 0.3% |
| 03-19 | 7 | 4 | 57.1% | 289 | 1 | 0.4% |
| 03-20 | 2 | 2 | 100.0% | 222 | 3 | 1.4% |
| 03-21 | 6 | 2 | 33.3% | 150 | 3 | 2.0% |
| 03-22 | 1 | 0 | 0.0% | 146 | 2 | 1.4% |
| 03-23 | 7 | 6 | 85.7% | 248 | 1 | 0.4% |

**Plays by traffic source:** Facebook video: direct 15, website 13; YouTube video: website 6, direct 5.
No `facebook_ad` traffic in this week. **Countries:** all 39 plays from the US (34 visitors).

**Observations for the marketing team:**
- The Facebook video converts loads into plays far better (26 plays / 53 loads, about 49% over the week) than the
  YouTube video (11 / 1,620, about 0.7%): the YouTube video is loaded 150–320 times a day but almost never played. That points at the
  page it is embedded on (loaded automatically, rarely clicked) rather than at the video itself.
- Viewers watch about a third (Facebook) and a quarter (YouTube) of the video on average.
