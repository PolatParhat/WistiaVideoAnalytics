"""Where raw files are written. Same interface locally (folder) and in AWS (S3, stage 3.8)."""

import json
from pathlib import Path


def raw_key(endpoint, media_id, run_date, run_id, page):
    """Path of one raw file, Hive-style partitions so Spark/Athena can filter on them."""
    return (f"raw/wistia/{endpoint}/media_id={media_id}/run_date={run_date}"
            f"/run_id={run_id}/page_{page:04d}.json")


class LocalStorage:
    """Writes files under a local folder (default lake/, git-ignored)."""

    def __init__(self, root="lake"):
        self.root = Path(root)

    def put_json(self, key, obj):
        path = self.root / key
        if path.exists():
            # raw data is immutable: a second write to the same key is a bug
            raise FileExistsError(f"refusing to overwrite {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2))
        return str(path)

    def uri(self, prefix=""):
        return str(self.root / prefix)
