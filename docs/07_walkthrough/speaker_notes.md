# Speaker notes: final project walkthrough

Recording length: about **15 minutes**. Read the lines in quotes in your own words; the **[Show]** lines say what is on
screen. Record **before the teardown**: sections 6–8 show the live AWS pipeline.
Numbers come from `docs/05_production_run/README.md` and the README.

## Before you record

**Browser tabs, in this order** (GitHub on branch `main`, AWS console in region **N. Virginia / us-east-1**):

1. GitHub repo front page (README)
2. `docs/02_design/architecture.png` (zoom so the diagram fills the screen)
3. `docs/02_design/data_model.png`
4. GitHub → the repo's **code** view of `src/wistia/` (folder list)
5. GitHub → **Actions** tab (CI runs, all green) and **Pull requests → Closed** (PRs #1–#7)
6. AWS → **CloudFormation → Stacks → WistiaPipeline → Resources**
7. AWS → **Step Functions → State machines → wistia-pipeline** (Executions list)
8. AWS → **S3 → wistiapipeline-databucket… → raw/wistia/**
9. AWS → **DynamoDB → Tables → WistiaPipeline-Watermarks… → Explore table items**
10. AWS → **AWS Glue → ETL jobs → wistia-transform → Runs**
11. AWS → **CloudWatch → Logs Insights** (log group `WistiaPipeline-IngestFunctionLogs…`, query below pasted in)
12. AWS → **Athena → Query editor**, workgroup **wistia** selected (top right), the queries below ready in tabs
13. `docs/05_production_run/README.md`
14. `docs/decisions.md`

**Run every Athena query and the Logs Insights query once before recording**, so they are fast and you know the output.

**Never show:**
- `.env`, the original `.docx`, or the Secrets Manager secret's value (don't click "Retrieve secret value").
- **Personal data:** don't `SELECT *` from `wistia_silver.events` or `wistia_gold.dim_visitor`, and don't open
  raw files under `raw/wistia/events/` (they contain visitor IP addresses and cities). Open a `medias/` file instead.
- The AWS account number in the console's top-right menu: keep that menu closed.

Close notifications; zoom the browser so text is readable; practice once with a timer.

### Queries to have ready

CloudWatch Logs Insights (ingest log group, time range "Last 1 day" or a custom range covering the runs):

```
fields @timestamp, msg, media_id, window_start, window_end, events
| filter msg = "media done"
| sort @timestamp asc
```

Athena (workgroup `wistia`), one per editor tab:

```sql
-- A. The simulated week
SELECT run_date, window_start, window_end, status, events_read, events_new, silver_events_total, dq_failed_critical
FROM wistia_ops.pipeline_runs ORDER BY started_at;

-- B. No duplicates after the rerun
SELECT COUNT(*) AS duplicated_keys FROM (
  SELECT event_key, visitor_key, received_at FROM wistia_silver.events
  GROUP BY event_key, visitor_key, received_at HAVING COUNT(*) > 1);

-- C. Daily engagement per channel
SELECT m.channel, f.date, f.load_count, f.play_count, f.play_rate, f.unique_visitors
FROM wistia_gold.fact_media_daily f JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
ORDER BY m.channel, f.date;

-- D. Totals per video
SELECT m.title, m.channel, SUM(f.play_count) AS plays, COUNT(DISTINCT f.visitor_id) AS visitors,
       ROUND(AVG(f.watched_percent), 3) AS avg_watched_percent
FROM wistia_gold.fact_media_engagement f JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
GROUP BY m.title, m.channel ORDER BY plays DESC;
```

---

## 1. Introduction (0:00–0:45)

[Show] Tab 1, README.

"Hi, I'm Polat. This is the finished Wistia Video Analytics pipeline. The marketing team promotes two videos on
Facebook and YouTube and wants to know how they perform: plays, play rate, watch time, and who is watching.

The pipeline pulls that data daily from the Wistia Stats API with Python, transforms it with PySpark into a star
schema, and runs entirely on AWS. It's deployed, it ran a simulated production week, and I'll show it live.

I'll cover the architecture, the code, CI/CD, the live pipeline in AWS, the results, and the decisions I made."

## 2. Architecture (0:45–2:30)

[Show] Tab 2, architecture diagram.

"One daily run goes like this:

- An EventBridge schedule would start it at 2 AM UTC. It's deployed **disabled**, because the 7-day requirement was
  met by a simulated week, which I'll show.
- Step Functions is the orchestrator. It runs three steps in order and retries each one.
- Step one, a Python Lambda, reads the Wistia token from Secrets Manager, works out which dates to pull from a
  watermark in DynamoDB, calls the API with pagination and retries, and writes the responses unchanged to S3, in a
  folder per run.
- Step two, a Glue job running PySpark, reads exactly that run's files, builds clean silver tables and the gold star
  schema as Apache Iceberg tables, and runs data-quality checks.
- Step three moves the watermark forward, **only** if the transform and the checks passed. If anything fails, the
  watermark stays, and the next run picks up the missed days automatically. A failed run also triggers a CloudWatch
  alarm that emails me.
- Athena queries the tables with SQL.

Everything is serverless, defined in code with AWS CDK, and the simulated week cost well under a dollar."

## 3. Data model (2:30–3:45)

[Show] Tab 3, data model.

"Three layers. Raw is the API JSON as received, never changed, so any day can be reprocessed without calling Wistia
again. Silver is typed and deduplicated. Gold is the star schema.

- `dim_media`: one row per video, including the channel. Wistia has no channel field, so it comes from a config file;
  the SME confirmed Facebook for 'The Gap Method' and YouTube for the testimonial.
- `dim_visitor`: one row per visitor, built from the events, because Wistia's visitors endpoint can't be filtered by
  video and has no location.
- `fact_media_engagement`: per video, visitor and day: plays, watch time, percent watched, traffic source.
- `fact_media_daily`: per video and day: loads, plays and play rate. Play rate lives here because Wistia only
  reports loads per video per day, not per visitor.

One important finding from the API: the event ID is **not unique**, so events are identified by event key, visitor
key and timestamp together. Every table is loaded with a MERGE on its natural key: existing rows are updated, new rows
inserted. That makes reruns safe."

## 4. The code (3:45–5:30)

[Show] Tab 4, `src/wistia/` folder; click into `api.py`, then `ingestion.py` (`compute_window`), then `transform/`.

"I built the code the way it grows in real projects: one working script first, then I moved the reusable parts into a
library, then tests, then the cloud pieces. Each stage is one commit, so the history shows that evolution.

- `api.py` is the Wistia client: token auth, retries with exponential backoff on rate limits and server errors, and
  pagination until an empty page.
- `ingestion.py` has the incremental logic. Each run pulls from the last loaded day up to its `run_date`: re-reading
  the last loaded day is a one-day lookback that catches late events, and the MERGE makes it harmless.
- `transform/` is the PySpark code, split into silver, gold and quality as **pure functions**: DataFrames in,
  DataFrames out, no I/O, so they're unit-tested with a few hand-made rows.
- The same library runs locally and in AWS. Only small adapters change: a local folder versus S3, a JSON file versus
  DynamoDB. Locally, Spark runs in the official AWS Glue Docker image, so what I test is what runs in AWS."

## 5. CI/CD (5:30–6:30)

[Show] Tab 5, Actions tab (green runs), then Closed pull requests.

"Every change went through a pull request. CI runs on each one: linting, 50 unit tests including the PySpark ones
with the HTTP calls and AWS mocked, and the infrastructure: CDK template tests and a full synth.

There's also a manual Deploy workflow that logs in to AWS with OIDC, so no AWS keys are stored in GitHub. For this
project I deployed from the command line; wiring the GitHub deploy is the next step I'll do."

## 6. The live pipeline in AWS (6:30–10:00)

[Show] Tab 6, CloudFormation resources.

"This is the deployed stack: the bucket, the DynamoDB table, two Lambdas, the Glue job and databases, the state
machine, the disabled schedule, the alarm and the Athena workgroup. All of it was created by one `cdk deploy`, and
one `cdk destroy` removes it."

[Show] Tab 7, Step Functions executions; open the first one (`sim-2026-03-17-…`), show the green graph, then click
**Ingest** → Output.

"Here's the simulated production week: eight executions, all succeeded: March 17 to 23, then the 20th again. Each run
gets a `run_date` as input. The Ingest step returns a `run_id`, and Step Functions passes it to the Glue job, so the
transform processes exactly the files that run wrote."

[Show] Tab 8, S3 `raw/wistia/` → `medias/` → `media_id=…/` → `run_date=2026-03-17/` → `run_id=…/` → open the file.

"Raw data is partitioned by endpoint, media, run date and run ID. Each file wraps the API response with where and when
it came from. Files are write-once: S3 refuses to overwrite them."

[Show] Tab 9, DynamoDB items.

"The watermarks: both videos at March 23. The rerun of the 20th didn't move them back, because the write only allows
the date to go forward."

[Show] Tab 10, Glue job runs.

"The Glue job ran eight times, each a couple of minutes, two small workers."

[Show] Tab 11, Logs Insights, run the query.

"Every log line is JSON, so I can query the logs. Here each run's window per video: March 17 alone, then 17 to 18,
and so on. You can see the lookback and the event counts."

## 7. The evidence (10:00–11:30)

[Show] Tab 12, Athena, query A.

"This is the run log the Glue job writes. One row per run. `events_read` is always higher than `events_new`, because
of the lookback; the re-read rows are updated, not duplicated. 39 events in total, exactly what the API exploration
counted for this week. The rerun of the 20th read 5 events and added zero."

[Show] Query B.

"And the duplicate check after the rerun: zero. The same check on both fact tables is in the evidence document, also
zero. Data-quality checks run on every load; the only flag was a small known difference between Wistia's daily play
count and its event list, which is logged as a warning."

## 8. Results for the marketing team (11:30–13:00)

[Show] Query C, then query D.

"The Facebook video, 'The Gap Method': 28 plays from 24 visitors, and about half of its loads turn into plays.
Viewers watch about 30 percent of it.

The YouTube video is different: it's loaded 150 to 300 times a day, but played only 11 times all week: a play rate
under 1 percent. That says the page where it's embedded loads the player automatically but people don't click it. So
the first thing I'd look at is the placement, not the video.

All viewers this week were in the US."

## 9. Decisions and trade-offs (13:00–14:30)

[Show] Tab 14, `docs/decisions.md`, then the README's requirements coverage table.

"Every decision is logged with its reason. The main ones:

- **Step Functions plus Lambda and Glue instead of Airflow**: at one run a day, managed Airflow would cost about 50
  cents an hour; this costs cents per run.
- **Iceberg with MERGE** for idempotent reruns.
- **Bearer auth**: the requirement mentions Basic; both work, and Bearer is Wistia's documented method.
- **Incremental by date window**: events have no updated timestamp, so the window uses the event date plus a lookback.
- **The 7 days were simulated**: the real deployed pipeline ran seven consecutive dates in order instead of waiting a
  calendar week. It's the same code and infrastructure as the daily schedule, and it also proved reruns are safe.
- **No dashboard yet**: the report queries answer the questions; a dashboard is optional and planned next.

The coverage table in the README maps each requirement to where it's met, including these deviations."

## 10. Wrap-up (14:30–15:00)

[Show] Tab 1, README.

"To sum up: a deployed, tested, incremental and idempotent pipeline from the Wistia API to a star schema on AWS, with
CI on every change and evidence of a full production week. Setup, run, deploy and teardown instructions are in the
README. After this recording I tear the stack down with `cdk destroy`. Thanks for watching."
