"""Settings: which media to load and how the pipeline behaves."""

import json
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MEDIA_CONFIG = REPO_ROOT / "config" / "media.yaml"

BASE_URL = "https://api.wistia.com/v1"
LOOKBACK_DAYS = 1  # re-read the previous day to catch late events
PER_PAGE = 100  # Wistia's maximum page size
MAX_ATTEMPTS = 5  # per API call, including the first try


@dataclass(frozen=True)
class Media:
    media_id: str
    channel: str


def load_media(path=DEFAULT_MEDIA_CONFIG):
    """Read config/media.yaml into a list of Media."""
    import yaml  # imported here: Glue has no PyYAML, and Glue never calls this function

    data = yaml.safe_load(Path(path).read_text())
    return [Media(media_id=m["media_id"], channel=m["channel"]) for m in data["media"]]


def media_config_json(path=DEFAULT_MEDIA_CONFIG):
    """config/media.yaml as a JSON string, passed to the Spark job as an argument."""
    return json.dumps([{"media_id": m.media_id, "channel": m.channel} for m in load_media(path)])


if __name__ == "__main__":
    print(media_config_json())
