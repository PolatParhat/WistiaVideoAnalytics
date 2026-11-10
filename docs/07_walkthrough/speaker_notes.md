# Speaker notes: final project walkthrough

About 15 minutes. Read it like you're talking to a colleague; say it in your own words. The lines in *italics* tell
you what to click; everything else is what you say. Record this **before** tearing the stack down.

## Before you record

Open these tabs in order (AWS console in **N. Virginia / us-east-1**):

1. GitHub: the README
2. GitHub: `docs/02_design/architecture.png`
3. GitHub: `docs/02_design/data_model.png`
4. GitHub: the `src/wistia/` folder
5. GitHub: the Actions tab
6. AWS: CloudFormation → WistiaPipeline → Resources
7. AWS: Step Functions → wistia-pipeline
8. AWS: S3 → the wistiapipeline-databucket… bucket → `raw/wistia/`
9. AWS: DynamoDB → the Watermarks table → Explore items
10. AWS: CloudWatch → Logs Insights, log group `WistiaPipeline-IngestFunctionLogs…`, with this query pasted in:
    ```
    fields @timestamp, msg, media_id, window_start, window_end, events
    | filter msg = "media done"
    | sort @timestamp asc
    ```
11. AWS: Athena → Query editor, workgroup **wistia**, with these three queries in three tabs:
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
12. GitHub: `docs/decisions.md`

Run the queries once before you hit record so they're quick.

**Don't show:** the `.env` file, the secret's value, your AWS account number (top-right menu), or anything with
visitor IP addresses. That means no `SELECT *` on the events or visitor tables, and don't open files under
`raw/wistia/events/`.

---

## The script

*README on screen.*

Hi, I'm Polat, and this is my Wistia video analytics pipeline. The idea is simple. A marketing team has two videos on
Wistia, one they push on Facebook and one on YouTube, and they want to know how those videos are doing: how many
people play them, how much they watch, and who's watching. So I built a pipeline that pulls that data from Wistia's
API every day, cleans it up, and puts it into tables they can query. It's all on AWS, it's deployed right now, and
I already ran it through a full week of data, so I'll show you the real thing.

Let me start with how it fits together.

*Switch to the architecture diagram.*

Every run starts here, with a schedule. It would kick things off at 2 AM every night, but I've left it switched off,
and I'll explain why in a minute. When a run starts, Step Functions takes over. Think of it as the conductor: it
runs three steps in order, and if one of them fails it retries it.

The first step is a small Python function in Lambda. It gets the Wistia token from Secrets Manager, so the token is
never in the code. Then it checks DynamoDB to see which day we loaded last, works out which dates to ask for, and
calls the Wistia API. It goes through every page of results, and it backs off and tries again if Wistia is busy.
Whatever comes back gets saved to S3 exactly as it arrived. I never change that raw data, so if something goes wrong
later I can always rebuild from it.

The second step is a Glue job running PySpark. It reads just the files that this run saved, cleans them up, and
builds the final tables. It also runs data quality checks, so if something looks wrong, the run fails instead of
loading bad data.

And only if all of that worked does the third step move the "last loaded day" forward in DynamoDB. That's the
important part. If a run fails, the bookmark stays where it was, so the next run just picks up the missed days
automatically. And if a run does fail, an alarm emails me.

At the end, the marketing team can query everything with plain SQL in Athena. The whole thing is defined in code
with AWS CDK, and running the full test week cost well under a dollar.

*Switch to the data model.*

The data moves through three layers. Raw is the API responses, untouched. Silver is the same data, cleaned and with
duplicates removed. Gold is what people actually query: a star schema. There's a video table with the title and the
channel, a visitor table with where people are watching from, and two fact tables. One has engagement per video, per
visitor, per day: how many plays, how much they watched. The other has daily totals per video, including play rate.

Two things I learned from exploring the API shaped this. First, Wistia doesn't have a "channel" field anywhere, so
which video is Facebook and which is YouTube comes from a small config file, and the expert reviewing this confirmed
that mapping. Second, Wistia's event ID isn't actually unique. Two different views can share the same ID. So I
identify each event by the ID, the visitor, and the time together. If I'd trusted the ID alone, I would've silently
lost real data.

Every table is loaded with a MERGE. If a row is already there it gets updated, and if it's new it gets added. That
means I can rerun any day as many times as I want and never get duplicates. I'll prove that in a minute.

*Switch to the `src/wistia/` folder.*

As for the code, I built it the way code usually grows on a real team. I started with one script that made a single
API call. Then I added pagination, retries, and logging. Then I pulled the reusable parts out into this library,
added tests, and only then added the cloud pieces. Every one of those stages is its own commit, so you can follow the
whole journey in the history.

*Open `api.py` briefly, then the `transform` folder.*

This is the API client, with the retries and the paging. And this is the PySpark part, split into silver, gold and
quality checks. These are pure functions: data goes in, data comes out, nothing else happens. That's what makes them
easy to test. The same code runs on my laptop and in AWS. The only thing that changes is where files go: a local
folder or S3, a JSON file or DynamoDB.

*Switch to the Actions tab.*

Every change went in through a pull request, and every pull request runs these checks automatically: code style, 50
unit tests including the Spark ones, and a test build of the whole AWS setup. Nothing got merged unless it was green.
There's also a deploy workflow that logs into AWS without storing any keys in GitHub. For this project I deployed from
my own terminal, and setting up the GitHub deploy is my next step.

Okay, so that's how it's built. Now let me show you it actually running.

*Switch to CloudFormation.*

This is everything that one deploy command created: the bucket, the DynamoDB table, the two Lambda functions, the
Glue job, the Step Functions pipeline, the alarm, and the Athena setup. And one destroy command removes all of it.

*Switch to Step Functions and show the list of executions.*

Now, about that schedule. The requirement was to run in production for seven days. Instead of waiting a real week, I
ran the deployed pipeline once for each day from March 17th to March 23rd, in order, exactly as the nightly schedule
would. Here are those runs, all green. That eighth one at the top is March 20th again. I reran it on purpose to prove
that rerunning a day doesn't create duplicates.

*Open the first run, show the green graph, then click Ingest and show its output.*

Here's one run. You can see the three steps went through in order. If I click on the ingest step, you can see it
returns a run ID, and that ID gets handed to the Spark job, so it knows exactly which files to process.

*Switch to S3 and click down: `medias` → a media folder → `run_date=2026-03-17` → the run folder → open the file.*

Here's where the raw data lands. It's organized by endpoint, video, date and run, and every file records where it
came from and when. These files can only be written once. S3 won't let anything overwrite them.

*Switch to DynamoDB.*

And here are the bookmarks. Both videos are at March 23rd, the last day of the week. Notice that rerunning March 20th
didn't drag them backwards. The code only ever lets that date move forward.

*Switch to Logs Insights and run the query.*

All the logs are written as JSON, which means I can query them. Here's each run and the dates it asked for: the 17th
on its own, then the 17th to the 18th, then the 18th to the 19th, and so on. Each run reaches back one day, to catch
any views that showed up late.

*Switch to Athena, run the first query.*

And this is the proof. The Spark job writes one row for every run. Look at these two columns: "events read" is
always bigger than "events new." That's the look-back day being read again and updated instead of added twice. By
the end there are 39 events, which is exactly what I counted when I first explored the API. And the rerun of the 20th
at the bottom read 5 events and added zero.

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

*Switch to `decisions.md`.*

Finally, every decision I made is written down here with the reason behind it. A few worth mentioning: I used Step
Functions with Lambda and Glue instead of Airflow, because for one run a day, managed Airflow would cost around 50
cents an hour while this costs a few cents a run. I used Iceberg tables so that MERGE and safe reruns work. The
requirement mentions Basic auth, but Wistia documents Bearer tokens, and both worked when I tested them, so I went with
Bearer. And as I said, the seven days were simulated on the real pipeline rather than waited out. I didn't build a
dashboard, since that part was optional and the SQL already answers the questions. The README has a table that maps
every requirement to where it's met, including these choices.

*Back to the README.*

So that's it: a pipeline that pulls from the Wistia API every day, only loads what's new, can safely rerun any day,
checks its own data, and turns it into numbers the marketing team can actually use. Everything's tested on every
change, and the setup, run and teardown steps are all in the README. Once I stop recording, I'll tear the whole stack
down with one command. Thanks for watching.
