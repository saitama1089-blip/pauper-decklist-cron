"""Show upstream freshness separately from deck-cache completeness."""
from datetime import datetime, timezone
import os

import requests


def source_age_minutes(session=None, now=None):
    session = session or requests.Session()
    token = os.environ.get("GH_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    response = session.get(
        "https://api.github.com/repos/saitama1089-blip/pauper-league-cron/actions/workflows/run-cron.yml/runs",
        params={"status": "success", "per_page": 1}, timeout=20,
    )
    response.raise_for_status()
    runs = response.json().get("workflow_runs", [])
    if not runs:
        return None
    completed = datetime.fromisoformat(runs[0]["updated_at"].replace("Z", "+00:00"))
    return max(0, round(((now or datetime.now(timezone.utc)) - completed).total_seconds() / 60))


if __name__ == "__main__":
    try:
        age = source_age_minutes()
        line = f"Last successful league scraper: {age} minutes ago." if age is not None else "No successful league scraper found."
        if age is None or age > 90:
            print("::warning::" + line + " Check upstream scheduling and result ingestion.")
        else:
            print(line)
    except requests.RequestException:
        line = "Upstream freshness could not be checked through the GitHub API."
        print("::warning::" + line)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as stream:
            stream.write("\n" + line + "\n")
