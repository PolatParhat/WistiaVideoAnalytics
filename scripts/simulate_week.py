"""Simulated 7-day production run (FR8): run the pipeline once per day, in order, each with its own run_date.

AWS (after `cdk deploy`): starts the Step Functions pipeline for each day and waits for it to finish.
  uv run python scripts/simulate_week.py --start 2026-03-17 --days 7 --rerun 2026-03-20

Local (no AWS): runs jobs/ingest.py and the Spark transform (Glue Docker image) for each day.
  uv run python scripts/simulate_week.py --start 2026-03-17 --days 7 --rerun 2026-03-20 --local

--rerun runs one day again at the end, to show that a rerun adds no duplicates.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STACK_NAME = "WistiaPipeline"


def run_dates(start, days):
    return [start + timedelta(days=i) for i in range(days)]


# ------------------------------------------------------------------ AWS
def state_machine_arn(region):
    import boto3

    outputs = boto3.client("cloudformation", region_name=region).describe_stacks(StackName=STACK_NAME)["Stacks"][0]
    return next(o["OutputValue"] for o in outputs["Outputs"] if o["OutputKey"] == "StateMachineArn")


def run_aws(day, arn, region):
    import boto3

    sfn = boto3.client("stepfunctions", region_name=region)
    name = f"sim-{day.isoformat()}-{int(time.time())}"
    execution = sfn.start_execution(stateMachineArn=arn, name=name, input=json.dumps({"run_date": day.isoformat()}))
    print(f"{day}  started {name}", flush=True)
    while True:
        time.sleep(15)
        desc = sfn.describe_execution(executionArn=execution["executionArn"])
        if desc["status"] != "RUNNING":
            break
    output = json.loads(desc.get("output") or "{}")
    ingest = output.get("ingest", {})
    print(f"{day}  {desc['status']:9s} run_id={ingest.get('run_id')} events={ingest.get('events')}", flush=True)
    return desc["status"] == "SUCCEEDED"


# ------------------------------------------------------------------ local
def run_local(day):
    media_config = subprocess.run(
        ["uv", "run", "python", "-m", "wistia.config"], check=True, capture_output=True, text=True, cwd=REPO_ROOT
    ).stdout.strip()
    subprocess.run(
        ["uv", "run", "python", "jobs/ingest.py", "--run-date", day.isoformat()],
        check=True,
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
    )
    result = subprocess.run(
        ["scripts/spark_local.sh", "jobs/transform.py", "--run-date", day.isoformat(), "--media-config", media_config],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    summary = next((json.loads(line) for line in result.stdout.splitlines() if line.startswith('{"run_id"')), None)
    if result.returncode != 0 or summary is None:
        print(result.stdout[-3000:], result.stderr[-3000:], sep="\n")
        print(f"{day}  FAILED")
        return False
    print(
        f"{day}  {summary['status']:9s} window={summary['window_start']}..{summary['window_end']} "
        f"events_read={summary['events_read']} events_new={summary['events_new']} "
        f"silver_events_total={summary['silver_events_total']}",
        flush=True,
    )
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--rerun", type=date.fromisoformat, help="run this day once more at the end")
    parser.add_argument("--local", action="store_true", help="run locally instead of in AWS")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args(argv)

    days = run_dates(args.start, args.days) + ([args.rerun] if args.rerun else [])
    if args.local:
        run_one = run_local
    else:
        arn = state_machine_arn(args.region)
        print(f"state machine: {arn}")

        def run_one(day):
            return run_aws(day, arn, args.region)

    for day in days:
        if not run_one(day):
            print(f"stopped: the run for {day} failed (later days were not started)")
            return 1
    print("all runs succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
