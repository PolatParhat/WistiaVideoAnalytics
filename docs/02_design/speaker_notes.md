# Speaker notes: design walkthrough for SME approval

Recording length: about **10 minutes**. Read the lines in quotes in your own words; the **[Show]** lines say what is on screen.
Numbers below come from `docs/01_api_exploration/README.md` and `docs/02_design/README.md`.

## Before you record

- Open these in browser tabs (GitHub, branch `main`), in this order:
  1. `docs/00_requirements/requirements.md`
  2. `docs/01_api_exploration/README.md`
  3. `docs/02_design/architecture.png` (open the image, then zoom so the whole diagram fills the screen)
  4. `docs/02_design/data_model.png`
  5. `docs/02_design/README.md` (sections 2 and 5)
  6. `docs/decisions.md`
- **Never show** `.env` or the original `.docx` (both contain the API token). The GitHub copy of the requirements is redacted.
- Close Slack/email notifications; set browser zoom so text is readable in the recording.
- Practice once with a timer; it is fine to read from these notes.

---

## 1. Introduction (0:00–0:30)

[Show] Repo front page on GitHub.

"Hi, I'm Polat. This is the design for the Wistia Video Analytics pipeline. I'll cover the business goal, what I learned
from the Wistia API, the architecture, the data model, and the decisions and assumptions I need your approval on.
Nothing is built yet: I'm asking for approval before I start."

## 2. The problem (0:30–1:30)

[Show] Tab 1, requirements.

"The marketing team hosts videos on Wistia and promotes them on Facebook and YouTube. They want to know how each video
performs: plays, play rate, watch time, and who is watching, by visitor and location.

The requirements are: pull media-level and visitor-level data from the Wistia Stats API; handle pagination and
incremental loads; store it in a structured model with `dim_media`, `dim_visitor` and `fact_media_engagement`; Python for
ingestion, PySpark for transformation; AWS only; no dbt; CI/CD with GitHub; and run it in production for 7 days.

Scope is two media IDs: `8hunphufxp` and `9k4tbcdfg0`."

## 3. What the API exploration showed (1:30–3:00)

[Show] Tab 2, API exploration, section 3 then section 4.

"Before designing, I called the API with read-only requests to see the real data.

- Authentication: both Bearer and Basic work with the token. I'll use Bearer, which is what Wistia documents.
- The two videos: `8hunphufxp` is a testimonial, about 16 minutes, with 916 viewing events since November 2024.
  `9k4tbcdfg0` is 'The Gap Method', about 37 minutes, with 326 events. So the data is small: about 1–3 events a day.
- Pagination: the API returns up to 100 rows per page and signals the end with an empty page. There's no total count,
  so the code must keep asking until it gets an empty page.
- The date filters work and include both end dates, which is what makes incremental loads possible.

Four findings shaped the design:
1. The event ID, `event_key`, is **not unique**. In each video, two different viewings share a key. If I deduplicated on
   that ID alone, I would silently lose real data. So the key is event key plus visitor key plus timestamp.
2. The visitors endpoint is account-wide, can't be filtered by video, and has no IP or country. All the visitor fields the
   requirements ask for are on the events, so the visitor dimension is built from events.
3. Wistia has **no channel field**. I'll come back to that, because it needs your decision.
4. The events contain IP addresses and city, which is personal data. The repo is public, so only masked samples are in it."

## 4. Architecture (3:00–6:00)

[Show] Tab 3, `architecture.png`. Point at each number as you say it.

"This is one daily run. Everything is serverless on AWS, in us-east-1.

- **①** A run starts either from EventBridge Scheduler at 6 a.m. UTC, or from me running a script with a specific date.
  Either way it starts a Step Functions state machine, which runs the steps in order, retries them, and records every run.
- **②** The Lambda function gets the Wistia token from Secrets Manager. The token is never in code, Git or logs.
- **③** It reads a watermark from DynamoDB: the last date it loaded successfully for each video. The window it asks
  for is from that date plus one day, minus a one-day lookback, up to the run date. The lookback catches late-arriving
  events.
- **④** It calls Wistia: video metadata, daily stats, and events, page by page until the empty page, with retries and
  backoff when Wistia returns 429 or 5xx errors.
- **⑤** It saves every response to S3 exactly as received, and those files are never changed. If I find a transform bug
  later, I can reprocess without calling Wistia again.
- **⑥ ⑦** Then a Glue job, which is PySpark, reads that run's files, cleans and deduplicates them, and merges them into
  Apache Iceberg tables. Merge means a re-read row updates instead of duplicating, so running the same day twice gives
  the same result. It also runs data-quality checks: keys not null and unique, percent viewed between 0 and 1, every
  fact row has a matching dimension row.
- **⑧** Only if all of that succeeds, the last step moves the watermark forward. If a run fails, the watermark stays,
  and the next run automatically picks up the missed days.
- **⑨** The tables are registered in the Glue Data Catalog and queried with Athena using SQL.
- On the right: all steps log to CloudWatch, and a failed run sends an email through SNS.
- At the bottom: the whole stack is defined in code with AWS CDK. GitHub Actions runs tests on every pull request, and
  deploys through an OIDC role, so no AWS keys are stored in GitHub."

## 5. Why these tools (6:00–7:00)

[Show] Stay on the architecture diagram.

"The rule I used was: what would a real team choose for small daily API data, AWS only, near-zero cost, nothing to
patch.

- Step Functions instead of Airflow on MWAA: for three steps, MWAA would cost about 49 cents an hour, around 85 dollars
  for the week. Step Functions is effectively free at one run a day.
- Lambda for ingestion: one run is about 30 API calls in under a minute, well inside Lambda's limits.
- Glue for PySpark: PySpark is required, and Glue runs it without managing a cluster.
- Iceberg because it supports merge, which gives me safe reruns and incremental loads without duplicates.
- Athena instead of Redshift: the data is about twelve hundred rows; a warehouse running by the hour is oversized.

Estimated cost: under one dollar for the 7-day run, and about 40 cents a month while idle, for the secret."

## 6. Data model (7:00–8:30)

[Show] Tab 4, `data_model.png`.

"Data moves through three layers: raw, exactly as received; silver, cleaned and deduplicated; and gold, this star schema.

- `dim_media`: one row per video, with title, URL, channel, duration and dates.
- `dim_visitor`: one row per visitor, with IP, country, region, city, browser and device, plus first and last seen.
- `fact_media_engagement`: one row per video, per visitor, per day. Play count is the number of viewings; total watch
  time is percent viewed times the video length; watched percent is the highest that day; and traffic source.
- `fact_media_daily`: one row per video per day, with loads, plays, play rate and hours watched.

Two operations tables, `pipeline_runs` and `dq_results`, record every run and every check. They are the evidence for
the 7-day run."

## 7. Decisions I need you to approve (8:30–9:30)

[Show] Tab 6, `decisions.md`, scroll to the 2026-11-03 rows.

"There are four places where I deviate from the requirements or make an assumption:

1. **Play rate moved.** The requirements put play rate in the engagement fact, per visitor. Wistia only reports loads per
   video per day, not per visitor, so play rate can't be computed per visitor. I put it in a new table,
   `fact_media_daily`.
2. **Channel is an assumption.** Wistia has no channel field. I map channel per video in a config file:
   `9k4tbcdfg0` as Facebook and `8hunphufxp` as YouTube. Please confirm or correct this mapping; it's a config change,
   not a code change. Separately, I add a traffic source per viewing from the page URL, because Facebook ad clicks carry
   `fbclid` or `utm_source=meta`. That one is real data.
3. **Bearer instead of Basic authentication.** Both work; Bearer is Wistia's documented method.
4. **The 7-day production run is simulated.** Instead of waiting 7 calendar days, I run the deployed AWS pipeline 7 times,
   once for each day from 17 to 23 March 2026. I picked that week because it has viewings every day, at least 3 a day,
   39 in total: 11 for one video and 28 for the other. Each video also has one day with no viewings, which is useful,
   because it tests that an empty day is handled correctly. It's the same code and infrastructure as a daily schedule, each run is incremental from the previous
   one, and I'll also rerun one day to show it creates no duplicates. The daily schedule exists in the code, just
   disabled."

## 8. Next steps and close (9:30–10:00)

[Show] Tab 5, design doc section 5 (build order).

"If you approve, I'll build in this order: a single ingestion script first, then refactor the reusable parts into a
library, then tests, the PySpark transform, the AWS infrastructure, CI/CD, and finally the 7-day simulation, with the
evidence in the `pipeline_runs` table.

What I need from you: approval of the architecture and data model, and confirmation of the four points above,
especially the channel mapping. Thank you."

---

## Likely SME questions (prepare, don't read)

| Question | Answer |
|---|---|
| Why not Airflow? | Cost and size: 3 steps, 1 run a day. MWAA ≈ $0.49/h (~$85 for the week) vs Step Functions ≈ $0. The design keeps steps separate, so moving to Airflow later only changes the orchestrator. |
| Why not Redshift / RDS for the warehouse? | ~1,200 rows. Iceberg on S3 + Athena is SQL-queryable, pay-per-query, and supports MERGE. Redshift would bill by the hour for nothing. |
| How do you avoid duplicates? | Natural keys (event_key + visitor_key + received_at; media + date; media + visitor + date) and Iceberg MERGE. Rerunning a day updates rows instead of adding them. |
| What if Wistia is down or rate-limits you? | Retries with exponential backoff on 429/5xx/timeouts (max 5 tries), Step Functions retries the step, and a failed run keeps the watermark, so the next run catches up. Plus an email alert. |
| What about late-arriving events? | The 1-day lookback re-reads the previous day on every run; MERGE makes that safe. |
| How is the token protected? | Secrets Manager, read at runtime by the Lambda role only. Not in code, Git, environment variables or logs. The repo is public and the history was checked. |
| How is personal data (IP) handled? | Raw and tables live in a private, encrypted S3 bucket with public access blocked; least-privilege IAM. Only masked samples are in the repo. |
| Why is 9k4tbcdfg0's play rate so different (42%) from 8hunphufxp (~1%)? | 8hunphufxp is loaded far more often (128,043 loads vs 939), probably autoplay or a high-traffic page embed, so most loads never become plays. Worth confirming with marketing. |
| Why simulate the 7 days? | Time. The simulation exercises the same scheduling, incremental and failure-recovery logic; the schedule exists and can be enabled for a real 7-day run if required. |
| How will you test it? | Unit tests for API pagination/retries (HTTP mocked) and for each PySpark transform; DQ checks on every run; CI runs the tests and `cdk synth` on every PR. |
