"""Watermarks: per media, the last day that was loaded successfully."""

import json
from datetime import date
from pathlib import Path


class LocalWatermarkStore:
    """Keeps watermarks in a JSON file (default lake/state/watermarks.json)."""

    def __init__(self, path="lake/state/watermarks.json"):
        self.path = Path(path)

    def _read_all(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, media_id):
        value = self._read_all().get(media_id)
        return date.fromisoformat(value) if value else None

    def set(self, media_id, loaded_date):
        """Advance the watermark. Never moves it backwards (re-running an old day must not undo later loads)."""
        marks = self._read_all()
        current = marks.get(media_id)
        if current and current >= loaded_date.isoformat():
            return False
        marks[media_id] = loaded_date.isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(marks, indent=2, sort_keys=True))
        return True
