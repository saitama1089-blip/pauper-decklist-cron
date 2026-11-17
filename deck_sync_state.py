"""Small, credential-free retry state shared by successive worker runs."""
import json
import math
import time
from pathlib import Path


class RetryState:
    def __init__(self, path=None):
        self.path = Path(path) if path else None
        self.data = {"version": 1, "decks": {}, "pending_since": {}, "global_retry_at": 0,
                     "global_error": "", "last_success_at": 0, "last_backlog": 0,
                     "no_progress_runs": 0, "growing_runs": 0}
        if self.path and self.path.exists():
            self.data.update(self.validate(json.loads(self.path.read_text(encoding="utf-8"))))

    @staticmethod
    def validate(data):
        if not isinstance(data, dict) or data.get("version") != 1:
            raise ValueError("Unsupported deck-sync retry state")
        if not isinstance(data.get("global_error", ""), str):
            raise ValueError("Invalid retry-state error category")
        for key in ("last_backlog", "no_progress_runs", "growing_runs"):
            if not isinstance(data.get(key, 0), int) or data.get(key, 0) < 0:
                raise ValueError("Invalid sync-health counter")
        for key in ("decks", "pending_since"):
            if not isinstance(data.get(key, {}), dict):
                raise ValueError("Invalid deck-sync retry state")
        for key in ("global_retry_at", "last_success_at"):
            value = data.get(key, 0)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid retry-state timestamp")
        for deck_id, record in data.get("decks", {}).items():
            if not str(deck_id).isdigit() or not isinstance(record, dict):
                raise ValueError("Invalid retry-state deck record")
            timestamp = record.get("next_retry_at", 0)
            attempts = record.get("attempts", 0)
            if (not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or timestamp < 0
                    or not isinstance(attempts, int) or attempts < 0):
                raise ValueError("Invalid per-deck retry state")
        for deck_id, timestamp in data.get("pending_since", {}).items():
            if (not str(deck_id).isdigit() or not isinstance(timestamp, (int, float))
                    or not math.isfinite(timestamp) or timestamp < 0):
                raise ValueError("Invalid backlog age state")
        return data

    def observe(self, deck_ids, now=None):
        now = time.time() if now is None else now
        pending = {str(deck_id) for deck_id in deck_ids}
        for key in ("decks", "pending_since"):
            self.data[key] = {key_id: value for key_id, value in self.data[key].items() if key_id in pending}
        for deck_id in pending:
            self.data["pending_since"].setdefault(deck_id, now)

    def ready(self, deck_id, now=None):
        now = time.time() if now is None else now
        return self.data["decks"].get(str(deck_id), {}).get("next_retry_at", 0) <= now

    def failed(self, deck_id, category, delay, now=None):
        now = time.time() if now is None else now
        previous = self.data["decks"].get(str(deck_id), {})
        self.data["decks"][str(deck_id)] = {"attempts": previous.get("attempts", 0) + 1,
                                          "last_error": category, "next_retry_at": now + delay}

    def saved(self, deck_id):
        self.data["decks"].pop(str(deck_id), None)
        self.data["pending_since"].pop(str(deck_id), None)

    def pause(self, message, seconds=1800):
        self.data["global_retry_at"] = time.time() + seconds
        self.data["global_error"] = str(message)[:500]

    def blocked(self, now=None):
        return self.data["global_retry_at"] > (time.time() if now is None else now)

    def save(self):
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
            temporary.replace(self.path)
