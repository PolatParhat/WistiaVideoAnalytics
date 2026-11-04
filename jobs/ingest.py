"""Local ingestion job: pull Wistia stats for one run_date into lake/raw.

The logic lives in the library (src/wistia/); this script only reads settings and wires the parts together.

Run:  uv run python jobs/ingest.py --run-date 2026-03-17
"""

import argparse
import os
from datetime import UTC, date, datetime, timedelta

from dotenv import find_dotenv, load_dotenv

from wistia.api import WistiaClient
from wistia.config import load_media
from wistia.ingestion import run_ingestion
from wistia.state import LocalWatermarkStore
from wistia.storage import LocalStorage


def parse_args(argv=None):
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Pull Wistia stats into lake/raw")
    parser.add_argument(
        "--run-date",
        type=date.fromisoformat,
        default=yesterday,
        help="day this run is for, YYYY-MM-DD (default: yesterday UTC)",
    )
    parser.add_argument("--media", nargs="+", help="media ids (default: all in config/media.yaml)")
    parser.add_argument("--lake", default="lake", help="local lake folder (default: lake)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(find_dotenv(usecwd=True))  # finds .env in this folder or any parent

    client = WistiaClient(token=os.environ["WISTIA_API_TOKEN"])
    storage = LocalStorage(args.lake)
    watermarks = LocalWatermarkStore(f"{args.lake}/state/watermarks.json")
    media_ids = args.media or [m.media_id for m in load_media()]

    result = run_ingestion(client, storage, watermarks, media_ids, args.run_date)

    # Locally the watermark moves right after ingestion. In AWS it moves only after the
    # transform and data-quality checks succeed (a separate Step Functions task).
    for media_id in media_ids:
        watermarks.set(media_id, args.run_date)
    return result


if __name__ == "__main__":
    main()
