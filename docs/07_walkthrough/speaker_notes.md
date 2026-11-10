# Speaker notes: final project walkthrough

About 20 minutes. Read it like you're talking to a colleague; say it in your own words. The lines in *italics* tell
you what to click; everything else is what you say. Record this **before** tearing the stack down.

## Before you record

Open these tabs in order (AWS console in **N. Virginia / us-east-1**):

1. GitHub: the README
2. GitHub: `docs/02_design/architecture.png`
3. GitHub: `docs/02_design/data_model.png`
4. GitHub: the `src/wistia/` folder
5. GitHub: the Actions tab
6. AWS: CloudFormation → Stacks (shows `WistiaPipeline` and `CDKToolkit`) → WistiaPipeline → Resources
7. AWS: EventBridge → Scheduler → Schedules → `wistia-daily`
8. AWS: Step Functions → State machines → `wistia-pipeline`
9. AWS: Lambda → Functions (filter on `WistiaPipeline`: three functions)
10. AWS: Secrets Manager → `wistia/api-token` (the overview page only)
11. AWS: DynamoDB → Tables → `WistiaPipeline-Watermarks…` → Explore table items
12. AWS: S3 → the `wistiapipeline-databucket…` bucket
13. AWS: AWS Glue → ETL jobs → `wistia-transform` → Runs
14. AWS: AWS Glue → Data Catalog → Databases → `wistia_gold` → Tables
15. AWS: Athena → Query editor, workgroup **wistia**, with these three queries in three tabs:
    ```sql
    SELECT run_date, window_start, window_end, status, events_read, events_new, silver_events_total
    FROM wistia_ops.pipeline_runs ORDER BY started_at;
    ```
    ```sql
    SELECT COUNT(*) AS duplicated_keys FROM (
      SELECT event_key, visitor_key, received_at FROM wistia_silver.events
      GROUP BY event_key, visitor_key, received_at HAVING COUNT(*) > 1);
    ```
    ```sql
    SELECT m.title, m.channel, SUM(f.play_count) AS plays, COUNT(DISTINCT f.visitor_id) AS visitors,
           ROUND(AVG(f.watched_percent), 3) AS avg_watched
    FROM wistia_gold.fact_media_engagement f JOIN wistia_gold.dim_media m ON m.media_id = f.media_id
    GROUP BY m.title, m.channel ORDER BY plays DESC;
    ```
16. AWS: CloudWatch → Logs → Logs Insights, log group `WistiaPipeline-IngestFunctionLogs…`, with this query pasted in:
    ```
    fields @timestamp, msg, media_id, window_start, window_end, events
    | filter msg = "media done"
    | sort @timestamp asc
    ```
17. AWS: CloudWatch → Alarms → `WistiaPipeline-PipelineFailed…`
18. AWS: SNS → Topics → the `WistiaPipeline-Alerts…` topic
19. AWS: IAM → Roles, search `WistiaPipeline`
20. GitHub: `docs/decisions.md`

Run the queries once before you hit record so they're quick.

**Don't show:** the `.env` file, the secret's value (never click "Retrieve secret value"), your AWS account number
(top-right menu), the email address on the SNS subscription, or anything with visitor IP addresses. That means no
`SELECT *` on the events or visitor tables, and don't open files under `raw/wistia/events/`.

---

## The script

*README on screen.*

Hi, I'm Polat, and this is my Wistia video analytics pipeline. The idea is simple. A marketing team has two videos on
Wistia, one they push on Facebook and one on YouTube, and they want to know how those videos are doing: how many
people play them, how much they watch, and who's watching. So I built a pipeline that pulls that data from Wistia's
API every day, cleans it up, and puts it into tables they can query with SQL. It runs entirely on AWS, it's deployed
right now, and I already ran it through a full week of data, so I'll show you the real thing.

Let me start with the big picture, and I'll name every AWS service as we go.

*Switch to the architecture diagram.*

Every run starts with **Amazon EventBridge Scheduler**. That's AWS's cron: it would kick off the pipeline at 2 AM
UTC every night. I've deployed it switched off, and I'll explain why in a bit.

The schedule starts **AWS Step Functions**, which is the orchestrator. Think of it as the conductor: it runs three
steps in a fixed order, passes information from one step to the next, retries a step if it fails, and keeps a history
of every run. I picked it over Airflow because for one run a day, managed Airflow on AWS would cost around 50 cents an
hour, while Step Functions costs basically nothing.

The first step is an **AWS Lambda** function written in Python. Lambda just runs your code without you managing a
server, which fits here because each ingestion only takes a few seconds. The function first gets the Wistia API token
from **AWS Secrets Manager**, so the token is never in the code, in GitHub, or in the logs. Then it looks in
**Amazon DynamoDB**, a small key-value database, where I keep a bookmark for each video: the last day that was loaded
successfully. From that it works out which dates to ask for, calls the Wistia API, goes through every page of results,
and backs off and retries if Wistia is busy or has an error.

Everything Wistia sends back is saved to **Amazon S3** exactly as it arrived. S3 is the storage layer for the whole
project: the raw files, the final tables, and the Athena query results all live in one private, encrypted bucket. I
never change the raw files, so if anything goes wrong later, I can rebuild everything from them without calling Wistia
again.

The second step is an **AWS Glue** job. Glue runs PySpark without me managing a cluster: it starts two small workers,
runs my Spark code, and shuts down. The job reads just the files this run saved, cleans them, and builds the final
tables. Those tables are in the **Apache Iceberg** format, stored in S3 and registered in the **Glue Data Catalog**,
which is the list of databases and tables that both Spark and Athena look at. The Glue job also runs data quality
checks, so if something looks wrong, the run fails instead of loading bad data.

The third step is another small Lambda that moves the bookmark in DynamoDB forward, and it only runs if everything
before it worked. That's the important part. If a run fails, the bookmark stays where it was, and the next run picks
up the missed days on its own.

To query the data there's **Amazon Athena**, which runs SQL straight on the files in S3, so there's no database server
to keep running. And to keep an eye on things, all the logs go to **Amazon CloudWatch**, and a CloudWatch alarm watches
for failed runs. If one fails, it sends a message to an **Amazon SNS** topic, and SNS emails me.

Behind all of this, every service has its own **IAM** role with only the permissions it needs. And none of it was
clicked together in the console. It's all written in Python with the **AWS CDK**, which turns my code into an
**AWS CloudFormation** template, and CloudFormation creates everything. Running the whole test week cost well under a
dollar.

*Switch to the data model.*

Now, the data itself moves through three layers. Raw is the API responses, untouched. Silver is the same data,
cleaned, typed, and with duplicates removed. Gold is what people actually query, and it's a star schema. There's a
video table with the title and the channel, a visitor table with where people are watching from and on what device,
and two fact tables. One has engagement per video, per visitor, per day: plays, watch time, how much of the video they
watched, and where they came from. The other has daily totals per video, including play rate.

Two things I found while exploring the API shaped this. First, Wistia doesn't have a "channel" field anywhere, so
which video is Facebook and which is YouTube comes from a small config file, and the expert reviewing the design
confirmed that mapping. Second, Wistia's event ID isn't actually unique. Two different views can share the same ID.
So I identify each event by the ID, the visitor, and the time together. If I'd trusted the ID alone, I would've
silently lost real data.

Every table is loaded with a MERGE, which Iceberg supports. If a row is already there it gets updated, and if it's
new it gets added. So I can rerun any day as many times as I want and never get duplicates, and I'll prove that in a
minute.

*Switch to the `src/wistia/` folder.*

As for the code, I built it the way code usually grows on a real team. I started with one script that made a single
API call. Then I added pagination, retries, and logging. Then I pulled the reusable parts out into this library,
added tests, and only then added the AWS pieces. Every one of those stages is its own commit, so you can follow the
whole journey in the history.

*Open `api.py` briefly, then the `aws` folder, then the `transform` folder.*

This is the API client, with the retries and the paging. This `aws` folder is the only code that talks to AWS
services: S3, DynamoDB, Secrets Manager, and the two Lambda entry points. And this is the PySpark part, split into
silver, gold, and quality checks. These are pure functions: data goes in, data comes out, nothing else happens, which
makes them easy to test. The same code runs on my laptop and in AWS. Locally, files go to a folder instead of S3, the
bookmark is a JSON file instead of DynamoDB, and Spark runs in the official AWS Glue Docker image, so what I test
locally is what runs in Glue.

*Switch to the Actions tab.*

Every change went in through a pull request, and every pull request runs these checks automatically in GitHub
Actions: code style, 50 unit tests including the Spark ones, with Wistia and AWS faked so no real calls happen, and a
test build of the whole CDK stack. Nothing got merged unless it was green. There's also a deploy workflow that logs
into AWS through OIDC, which gives GitHub short-lived credentials instead of storing AWS keys. For this project I
deployed from my own terminal, and setting up the GitHub deploy is my next step.

Okay, so that's how it's built. Now let me show you each of those services actually running.

*Switch to CloudFormation and show the two stacks, then open WistiaPipeline → Resources.*

This is CloudFormation. There are two stacks. CDKToolkit is a one-time setup CDK needs, a bucket and some roles it
uses to upload my code. WistiaPipeline is the whole pipeline. If I open its resources, that's everything one
`cdk deploy` created: the bucket, the DynamoDB table, the Lambda functions, the Glue job and databases, the state
machine, the schedule, the alarm, the SNS topic, the Athena workgroup, and all the IAM roles. And one `cdk destroy`
removes all of it.

*Switch to EventBridge Scheduler.*

Here's the schedule: every day at 2 AM UTC, for the day before. You can see it's disabled. The requirement was to run
in production for seven days. Instead of waiting a real week, I ran the deployed pipeline once for each day from March
17th to March 23rd, in order, exactly the way the schedule would have. Same code, same infrastructure, just not
waiting for the calendar.

*Switch to Step Functions and show the list of executions.*

And here are those runs in Step Functions, all green. That eighth one at the top is March 20th again. I reran it on
purpose to prove that rerunning a day doesn't create duplicates.

*Open the first run, show the green graph, then click Ingest and show its output.*

Here's one run. You can see the three steps went through in order: Ingest, Transform, and AdvanceWatermarks. The input
was just the date. If I click the ingest step, it returns a run ID, and Step Functions hands that ID to the Glue job,
so the transform knows exactly which files to process.

*Switch to Lambda.*

These are the two Lambda functions: one for ingestion, one for moving the bookmark. Both run Python 3.12 on ARM
processors, which is the cheaper option, and they're the same code from my library, packaged up by CDK. The third one
here, the auto-delete one, isn't mine. CDK adds it so that when I destroy the stack, it empties the bucket first and
nothing is left behind.

*Switch to Secrets Manager. Stay on the overview; don't retrieve the value.*

This is where the Wistia token lives. I created it once from the command line, so the value never went through my
code or my CDK templates. Only the ingest Lambda's role is allowed to read it.

*Switch to DynamoDB and show the items.*

And here are the bookmarks. Both videos are at March 23rd, the last day of the week. Notice that rerunning March 20th
didn't drag them backwards. The code only ever lets that date move forward.

*Switch to S3, open the bucket, then click down: `raw` → `wistia` → `medias` → a media folder →
`run_date=2026-03-17` → the run folder → open the file.*

This is the bucket. There are three folders: raw for the API responses, warehouse for the Iceberg tables, and the
Athena results. Raw data is organized by endpoint, video, date, and run, and every file records where it came from
and when. These files can only be written once. S3 itself refuses to overwrite them.

*Switch to Glue → the job runs.*

This is the Glue job, `wistia-transform`. It ran eight times, each a couple of minutes on two small workers. That's
the PySpark code turning those raw files into the silver and gold tables.

*Switch to the Glue Data Catalog → `wistia_gold` → Tables.*

And this is the Glue Data Catalog. There are three databases, silver, gold, and an ops one for run logs, and in gold
you can see the four star-schema tables. This catalog is what lets Athena find the tables. The data itself sits in S3.

*Switch to Athena and run the first query.*

So now let's query it in Athena. This is the run log the Glue job writes: one row for every run. Look at these two
columns: "events read" is always bigger than "events new." That's because each run reaches back one day to catch any
views that showed up late, and those re-read rows get updated instead of added twice. By the end there are 39 events,
which is exactly what I counted when I first explored the API. And the rerun of the 20th at the bottom read 5 events
and added zero.

*Run the second query.*

Just to be sure, this counts duplicates: zero. The quality checks ran on every load too. The only thing they flagged
was a small known gap between Wistia's daily play count and its list of events, so that's logged as a warning rather
than treated as an error.

*Run the third query.*

So what does this tell the marketing team? The Facebook video, "The Gap Method," got 28 plays from 24 people, and
about half the times it loaded, someone pressed play. People watched about 30 percent of it on average. The YouTube
video is a very different story. It loads 150 to 300 times a day, but it was only played 11 times all week. That's a
play rate under one percent. So people are landing on the page where it's embedded, but they aren't clicking it. If I
were on that team, I'd look at where that video sits on the page before I touched the video itself.

*Switch to CloudWatch Logs Insights and run the query.*

Now, how do I know what's going on when it runs? Every log line is written as JSON and goes to CloudWatch, which means
I can query the logs like data. Here's each run and the dates it asked for: the 17th on its own, then the 17th to the
18th, then the 18th to the 19th, and so on. You can see the one-day look-back, and how many events each run found.

*Switch to CloudWatch Alarms, then to the SNS topic.*

And if a run fails, I don't have to go looking. This CloudWatch alarm watches Step Functions for failed runs. It's in
the OK state because nothing failed. If something did, it would publish to this SNS topic, and SNS emails me.

*Switch to IAM → Roles, filtered on WistiaPipeline.*

Last thing on the AWS side is security. Every piece has its own IAM role with only what it needs. The ingest Lambda
can read the secret and write to the raw folder, but it can't touch the tables. The Glue job can read raw and write
the warehouse, but it can't read the secret. The scheduler can only start the pipeline. CDK generated all of these
from a few lines of code.

*Switch to `decisions.md`.*

Finally, every decision I made is written down here with the reason behind it. A few worth mentioning. Step Functions
with Lambda and Glue instead of Airflow, for cost. Iceberg tables, so that MERGE and safe reruns work. The requirement
mentions Basic auth, but Wistia documents Bearer tokens, and both worked when I tested them, so I went with Bearer.
The seven days were simulated on the real pipeline instead of waited out. And I didn't build a dashboard, since that
part was optional and the SQL already answers the questions. The README has a table that maps every requirement to
where it's met, including these choices.

*Back to the README.*

So that's it. A pipeline that pulls from the Wistia API every day with Lambda, keeps the raw data safe in S3, transforms
it with PySpark on Glue into Iceberg tables, only loads what's new, can safely rerun any day, checks its own data,
alerts me when something breaks, and gives the marketing team numbers they can query in Athena. Everything is defined
in code with CDK and tested on every change, and the setup, run, and teardown steps are all in the README. Once I stop
recording, I'll tear the whole stack down with one command. Thanks for watching.
