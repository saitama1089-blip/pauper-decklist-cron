"""Restore the newest completed main-branch worker's credential-free retry state."""
import io
import json
import os
from pathlib import Path
import re
import sys
import zipfile

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deck_sync_state import RetryState

MAX_BYTES = 2 * 1024 * 1024


def restore(repo, token, destination, current_run="", session=None):
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repo):
        raise ValueError("Invalid repository name")
    session = session or requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    base = f"https://api.github.com/repos/{repo}"
    response = session.get(base + "/actions/artifacts", params={"name": "deck-sync-state", "per_page": 100}, timeout=30)
    response.raise_for_status()
    artifacts = sorted(response.json()["artifacts"], key=lambda item: item["id"], reverse=True)
    for artifact in artifacts:
        run = artifact.get("workflow_run") or {}
        if artifact.get("expired") or run.get("head_branch") != "main" or str(run.get("id")) == str(current_run):
            continue
        metadata = session.get(base + f"/actions/runs/{int(run['id'])}", timeout=30)
        metadata.raise_for_status()
        data = metadata.json()
        if (data.get("status") != "completed" or data.get("head_branch") != "main"
                or data.get("path", "").split("@")[0] != ".github/workflows/sync-decks.yml"):
            continue
        archive = session.get(base + f"/actions/artifacts/{int(artifact['id'])}/zip", timeout=30, stream=True)
        archive.raise_for_status()
        chunks = bytearray()
        for chunk in archive.iter_content(65536):
            chunks.extend(chunk)
            if len(chunks) > MAX_BYTES:
                raise ValueError("Retry-state archive exceeds its size limit")
        with zipfile.ZipFile(io.BytesIO(chunks)) as zipped:
            entry = zipped.getinfo("deck-sync.json")
            if entry.file_size > MAX_BYTES:
                raise ValueError("Retry-state file exceeds its size limit")
            state = RetryState.validate(json.loads(zipped.read(entry)))
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state), encoding="utf-8")
        print(f"Restored retry state from completed worker run {run['id']}")
        return True
    print("No previous worker state; starting with an empty retry ledger.")
    return False


if __name__ == "__main__":
    restore(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"],
            os.environ["DECK_SYNC_STATE_FILE"], os.environ.get("GITHUB_RUN_ID", ""))
