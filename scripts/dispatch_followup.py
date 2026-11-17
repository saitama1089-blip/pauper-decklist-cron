"""Queue one bounded continuation using this repository's Actions token."""
import json
import os
from pathlib import Path
import re
import time

import requests


def dispatch(repo, token, report, continuation, max_decks, session=None):
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repo) or not 1 <= max_decks <= 100:
        raise ValueError("Invalid continuation configuration")
    if not report.get("needs_followup") or not 0 <= continuation < 3:
        print("No continuation needed, or the continuation limit was reached.")
        return False
    session = session or requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    payload = {"ref": "main", "inputs": {"check_only": False, "max_decks": max_decks,
                                           "continuation": continuation + 1}}
    endpoint = f"https://api.github.com/repos/{repo}/actions/workflows/sync-decks.yml/dispatches"
    for attempt in range(3):
        try:
            response = session.post(endpoint, json=payload, timeout=30)
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 2:
                raise
            time.sleep(2 ** (attempt + 1))
            continue
        if response.status_code in (200, 201, 204):
            print(f"Queued continuation {continuation + 1}/3 for eligible missing decks.")
            return True
        if response.status_code not in (429, 500, 502, 503, 504) or attempt == 2:
            response.raise_for_status()
            raise RuntimeError(f"Workflow dispatch returned HTTP {response.status_code}")
        time.sleep(2 ** (attempt + 1))
    return False


if __name__ == "__main__":
    if os.environ.get("GITHUB_REF_NAME") != "main":
        raise SystemExit("Automatic continuations are only allowed from main")
    report = json.loads(Path(os.environ["DECK_SYNC_REPORT_FILE"]).read_text(encoding="utf-8"))
    dispatch(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"], report,
             int(os.environ.get("DECK_SYNC_CONTINUATION", "0")), int(os.environ["MAX_DECKS_PER_RUN"]))
