import requests
import os
import sys
import re
import unicodedata
import time
import random
import math
import argparse
import json
from pathlib import Path
from deck_sync_state import RetryState
from typing import List, Dict, Optional
from datetime import datetime, timezone, date
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from bs4 import BeautifulSoup

# ==========================
# Configuration from environment variables (NO HARDCODED SECRETS!)
# ==========================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

RESULTS_TABLE = "pauper_league_results"
DECK_CACHE_TABLE = "deck_cache_view"

# NOTE: On GitHub Actions you are more likely to be throttled by MTGGoldfish.
# Use jitter-based delays for MTGGoldfish and keep Supabase delay minimal.
RATE_LIMIT_DELAY = float(os.environ.get("RATE_LIMIT_DELAY", "0.0"))

# 0 means "no limit" (process everything)
MAX_DECKS_PER_RUN = int(os.environ.get("MAX_DECKS_PER_RUN", "100"))
MAX_RUN_SECONDS = float(os.environ.get("MAX_RUN_SECONDS", "1200"))
MAX_BACKLOG_QUERY = 1000000

# When processing "all", we still do it in batches for safety
BATCH_FETCH_LIMIT = int(os.environ.get("BATCH_FETCH_LIMIT", "200"))

# MTGGoldfish throttle/backoff tuning (recommended for GitHub Actions)
MTGGOLDFISH_DELAY_MIN = float(os.environ.get("MTGGOLDFISH_DELAY_MIN", "3.0"))
MTGGOLDFISH_DELAY_MAX = float(os.environ.get("MTGGOLDFISH_DELAY_MAX", "7.0"))
MTGGOLDFISH_MAX_RETRIES = max(1, int(os.environ.get("MTGGOLDFISH_MAX_RETRIES", "3")))
MTGGOLDFISH_TIMEOUT = int(os.environ.get("MTGGOLDFISH_TIMEOUT", "25"))
MTGGOLDFISH_MAX_BACKOFF = float(os.environ.get("MTGGOLDFISH_MAX_BACKOFF", "30"))

# Shared session (keep-alive + cookies)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": os.environ.get(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/javascript, application/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
})


# ------------------------------
# Logging helper
# ------------------------------
def log(message: str, level: str = "INFO"):
    """Log with timestamp and level."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")
    sys.stdout.flush()


# ------------------------------
# Sleep helpers (jitter)
# ------------------------------
def sleep_jitter(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))


# ------------------------------
# Card name normalization
# ------------------------------
def normalize_card_name(name: str) -> str:
    """Normalize card name by converting smart quotes to regular quotes."""
    if not name:
        return ""

    name = unicodedata.normalize("NFKC", name)

    # Curly apostrophes / quotes → plain
    name = name.replace("\u2019", "'").replace("\u2018", "'")
    name = name.replace("\u201C", '"').replace("\u201D", '"')

    return name.strip()


# ------------------------------
# Build Scryfall "named image" URL (no API call needed)
# ------------------------------
def build_scryfall_fuzzy_image_url(card_name: str, version: str = "normal") -> str:
    """
    Deterministic URL that Scryfall will redirect to the CDN image.
    Works directly in <img src="..."> because browsers follow redirects.
    """
    name = normalize_card_name(card_name)
    if not name:
        return ""
    return f"https://api.scryfall.com/cards/named?format=image&version={version}&fuzzy={quote_plus(name)}"


# ------------------------------
# Parse decklist into structured format
# ------------------------------
def parse_decklist(decklist: str) -> Dict[str, List[Dict[str, any]]]:
    """
    Parse a decklist string into mainboard and sideboard sections.
    Sections are separated by a blank line.
    """
    lines = decklist.strip().split("\n")

    mainboard = []
    sideboard = []
    current_section = mainboard
    blank_line_encountered = False

    for line in lines:
        line = line.strip()

        if not line:
            if not blank_line_encountered and mainboard:
                blank_line_encountered = True
                current_section = sideboard
            continue

        match = re.match(r"^(\d+)\s+(.+)$", line)
        if match:
            count = int(match.group(1))
            card_name = match.group(2).strip()
        else:
            count = 1
            card_name = line.strip()

        if card_name:
            current_section.append({"name": card_name, "count": count})

    return {"mainboard": mainboard, "sideboard": sideboard}


# ------------------------------
# Process decklist into JSON (no Scryfall fetching)
# ------------------------------
def process_decklist_to_json(decklist: str) -> Dict:
    """
    Return formatted JSON with Scryfall image URLs built locally.
    """
    parsed = parse_decklist(decklist)

    result = {"mainboard": [], "sideboard": []}

    for card in parsed["mainboard"]:
        result["mainboard"].append(
            {
                "name": card["name"],
                "count": card["count"],
                "scryfall_url": build_scryfall_fuzzy_image_url(card["name"]),
            }
        )

    for card in parsed["sideboard"]:
        result["sideboard"].append(
            {
                "name": card["name"],
                "count": card["count"],
                "scryfall_url": build_scryfall_fuzzy_image_url(card["name"]),
            }
        )

    return result


# ------------------------------
# Find missing IDs using RPC
# ------------------------------
def get_missing_ids(limit: int) -> List[int]:
    """
    Use RPC get_missing_deck_ids(max_results integer) to find missing deck IDs.
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_missing_deck_ids"

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
    }

    missing_ids: List[int] = []
    offset = 0
    while offset < limit:
        page_size = min(1000, limit - offset)
        page_headers = {**headers, "Prefer": "count=exact", "Range-Unit": "items",
                        "Range": f"{offset}-{offset + page_size - 1}"}
        r = requests.post(url, json={"max_results": limit}, headers=page_headers, timeout=30)
        if r.status_code not in (200, 206):
            raise RuntimeError(f"Missing-ID RPC failed with HTTP {r.status_code}")
        result = r.json()
        if not isinstance(result, list):
            raise ValueError("Missing-ID RPC returned an unexpected response")
        for row in result:
            value = (row.get("deck_id") or row.get("id")) if isinstance(row, dict) else row
            if value is not None:
                missing_ids.append(int(value))
        total = r.headers.get("Content-Range", "").rsplit("/", 1)[-1]
        offset += len(result)
        if total.isdigit():
            if int(total) >= MAX_BACKLOG_QUERY:
                raise RuntimeError("Backlog exceeds the metadata safety limit")
            if offset >= int(total):
                break
        elif len(result) < page_size:
            break
        if not result:
            raise RuntimeError("Missing-ID RPC pagination stopped before the advertised count")
    missing_ids = list(dict.fromkeys(missing_ids))
    if total.isdigit() and len(missing_ids) != min(limit, int(total)):
        raise RuntimeError("Missing-ID RPC returned duplicate or incomplete pages")
    log(f"Found {len(missing_ids)} missing deck IDs")
    return missing_ids


def classify_missing_decks(deck_ids):
    """Count league/challenge references and the oldest missing result date."""
    headers = {"apikey": SUPABASE_SERVICE_ROLE, "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}"}
    report = {"league_missing": 0, "challenge_missing": 0, "oldest_result_date": "none"}
    dates = []
    for label, table, column, date_column in (
        ("league", RESULTS_TABLE, "id", "event_date"),
        ("challenge", "challenge_deck_results", "deck_id", "date"),
    ):
        found = set()
        for offset in range(0, len(deck_ids), 200):
            group = deck_ids[offset:offset + 200]
            result = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, timeout=30,
                                  params={"select": f"{column},{date_column}",
                                          column: "in.(" + ",".join(map(str, group)) + ")"})
            if result.status_code != 200:
                raise RuntimeError(f"Backlog classification failed with HTTP {result.status_code}")
            for row in result.json():
                found.add(int(row[column]))
                if row.get(date_column):
                    dates.append(date.fromisoformat(str(row[date_column])[:10]).isoformat())
        report[f"{label}_missing"] = len(found)
    if dates:
        report["oldest_result_date"] = min(dates)
    return report


def audit_cached_decks():
    """Flag existing null/empty cache payloads without overwriting them blindly."""
    headers = {"apikey": SUPABASE_SERVICE_ROLE, "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
               "Prefer": "count=exact"}
    total = 0
    for condition in ("is.null", "eq.[]"):
        result = requests.head(f"{SUPABASE_URL}/rest/v1/{DECK_CACHE_TABLE}", headers=headers, timeout=30,
                               params={"select": "deck_id", "limit": 1, "json_decklist->>mainboard": condition})
        count = result.headers.get("Content-Range", "").rsplit("/", 1)[-1]
        if result.status_code not in (200, 206) or not count.isdigit():
            raise RuntimeError("Cache audit did not return an exact count")
        total += int(count)
    return total


# ----------------------------------------
# MTGGoldfish throttle detection + backoff
# ----------------------------------------
class MTGGoldfishUnavailable(RuntimeError):
    """Stop the entire sync when the upstream is blocked or unavailable."""


class InvalidDeckWidget(ValueError):
    """An individual deck did not pass validation."""


def _is_challenge(resp: requests.Response) -> bool:
    if resp.headers.get("cf-mitigated", "").lower() == "challenge":
        return True
    body = (resp.text or "").lower()
    # Cloudflare challenge pages
    throttle_markers = (
        "captcha",
        "attention required",
        "just a moment",
        "performing security verification",
        "please enable cookies",
    )
    if "<html" in body and any(marker in body for marker in throttle_markers):
        return True
    return False


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Accept both HTTP Retry-After formats; ignore malformed values."""
    if not value:
        return None
    try:
        wait = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            wait = (retry_at - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return max(0.0, wait) if math.isfinite(wait) else None


def _get_with_backoff(url: str) -> Optional[requests.Response]:
    """Retry temporary failures, aborting the run instead of every deck in turn."""
    for attempt in range(1, MTGGOLDFISH_MAX_RETRIES + 1):
        retry_after = None
        try:
            resp = SESSION.get(url, timeout=MTGGOLDFISH_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as exc:
            reason = f"network failure ({type(exc).__name__})"
        else:
            if _is_challenge(resp):
                raise MTGGoldfishUnavailable(
                    f"MTGGoldfish widget returned a Cloudflare/browser challenge (HTTP {resp.status_code}). "
                    "Stopping the sync; remaining decks will be retried on a later run."
                )
            if resp.status_code == 403:
                raise MTGGoldfishUnavailable(
                    "MTGGoldfish widget returned HTTP 403 (Forbidden). Stopping the sync; "
                    "remaining decks will be retried on a later run."
                )

            if resp.status_code in (404, 410):
                return None

            if resp.status_code == 200:
                return resp

            if resp.status_code != 429 and resp.status_code < 500:
                raise MTGGoldfishUnavailable(
                    f"MTGGoldfish widget returned unexpected HTTP {resp.status_code}; stopping the sync."
                )

            reason = f"widget HTTP {resp.status_code}"
            retry_after = _retry_after_seconds(resp.headers.get("Retry-After"))

        if attempt == MTGGOLDFISH_MAX_RETRIES:
            raise MTGGoldfishUnavailable(
                f"MTGGoldfish still unavailable after {attempt} attempts: {reason}. "
                "Stopping the sync; remaining decks will be retried on a later run."
            )

        wait = min(MTGGOLDFISH_MAX_BACKOFF, (2 ** (attempt - 1)) + random.uniform(1.0, 4.0))
        if retry_after is not None:
            if retry_after > MTGGOLDFISH_MAX_BACKOFF:
                raise MTGGoldfishUnavailable(
                    f"MTGGoldfish requested Retry-After {retry_after:.1f}s, exceeding "
                    f"the {MTGGOLDFISH_MAX_BACKOFF:.1f}s wait budget. Stopping the sync."
                )
            wait = max(wait, retry_after)

        log(
            f"MTGGoldfish {reason}. Retrying in {wait:.1f}s "
            f"(attempt {attempt}/{MTGGOLDFISH_MAX_RETRIES})",
            "WARNING",
        )
        time.sleep(wait)


def _decode_js_string(value: str) -> str:
    """Decode a JavaScript string literal's contents as data, without executing it."""
    escapes = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v", "0": "\0"}

    def replace(match):
        token = match.group(1)
        if token.startswith("u{") and token.endswith("}"):
            return chr(int(token[2:-1], 16))
        if token.startswith("u") and len(token) == 5:
            return chr(int(token[1:], 16))
        if token.startswith("x") and len(token) == 3:
            return chr(int(token[1:], 16))
        if token.startswith(("u", "x")):
            raise ValueError("Malformed JavaScript character escape")
        if token in ("\n", "\r", "\r\n"):
            return ""
        return escapes.get(token, token)

    decoded = re.sub(r"\\(u\{[0-9a-fA-F]+\}|u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|\r\n|[\s\S])", replace, value)
    # Join escaped UTF-16 surrogate pairs while preserving literal Unicode.
    return decoded.encode("utf-16", "surrogatepass").decode("utf-16")


def parse_widget_deck_text(script: str, deck_id: int) -> str:
    """Read the public Expand Decks widget and validate its machine-readable list."""
    widget_ids = set(re.findall(r"widget-deck-placeholder(?:-manual)?\[data-id=['\"](\d+)['\"]\]", script))
    if widget_ids != {str(deck_id)}:
        raise ValueError("Widget deck identity is missing or does not match the requested deck")

    match = re.search(
        r'''elem\.innerHTML\s*=\s*("(?:\\[\s\S]|[^"\\])*"|'(?:\\[\s\S]|[^'\\])*')\s*;''',
        script,
    )
    if not match:
        raise ValueError("Widget HTML string is missing or its format changed")
    soup = BeautifulSoup(_decode_js_string(match.group(1)[1:-1]), "html.parser")
    formats = {field.get("value", "").lower() for field in soup.find_all("input", attrs={"name": "deck_input[format]"})}
    if formats != {"pauper"}:
        raise ValueError("Widget does not identify a Pauper deck")
    sources = {field.get("value", "") for field in soup.find_all("input", attrs={"name": "deck_input[deck]"})}
    if len(sources) != 1:
        raise ValueError("Widget deck field is missing or inconsistent")

    main_lines, side_lines = [], []
    current = main_lines
    saw_sideboard = False
    for line in sources.pop().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower() == "sideboard":
            if saw_sideboard:
                raise ValueError("Widget contains repeated sideboard markers")
            saw_sideboard = True
            current = side_lines
            continue
        card = re.fullmatch(r"([1-9]\d*)\s+(.+)", line)
        if not card or not card.group(2).strip() or "\0" in line:
            raise ValueError("Widget contains a malformed card quantity or name")
        current.append(f"{int(card.group(1))} {normalize_card_name(card.group(2))}")

    main_count = sum(int(line.split(" ", 1)[0]) for line in main_lines)
    side_count = sum(int(line.split(" ", 1)[0]) for line in side_lines)
    if main_count < 60 or side_count > 15:
        raise ValueError(f"Invalid Pauper deck size: mainboard={main_count}, sideboard={side_count}")
    return "\n".join(main_lines) + ("\n\n" + "\n".join(side_lines) if side_lines else "")


# ----------------------------------------
# Fetch deck text through the public tournament-page widget
# ----------------------------------------
def fetch_deck_text(deck_id: int) -> Optional[str]:
    """Fetch the same widget used by tournament pages' Expand Decks control."""
    if deck_id <= 0:
        raise ValueError("Deck IDs must be positive integers")
    sleep_jitter(MTGGOLDFISH_DELAY_MIN, MTGGOLDFISH_DELAY_MAX)
    url = f"https://www.mtggoldfish.com/widgets/deck/js?deckId={deck_id}"
    resp = _get_with_backoff(url)
    if resp is None:
        return None
    content_type = resp.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if content_type not in ("text/javascript", "application/javascript"):
        raise MTGGoldfishUnavailable(
            f"Widget for deck {deck_id} returned unexpected content type {content_type!r}; stopping the sync."
        )
    try:
        return parse_widget_deck_text(resp.text, deck_id)
    except ValueError as exc:
        raise InvalidDeckWidget(f"Invalid widget for deck {deck_id}: {exc}") from exc


# --------------------------------------------------------
# Save a single deck into deck_cache_view (JSON only)
# --------------------------------------------------------
def save_deck_to_supabase(deck_id: int, json_decklist: Dict) -> bool:
    """Insert a deck into the cache table with json_decklist only."""
    url = f"{SUPABASE_URL}/rest/v1/{DECK_CACHE_TABLE}"

    payload = {
        "deck_id": int(deck_id),
        "json_decklist": json_decklist,
    }

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)

        if r.status_code in (200, 201, 204):
            return True
        elif r.status_code == 409:
            log(f"Deck {deck_id} already exists", "WARNING")
            return True
        else:
            log(f"Error saving deck {deck_id}: {r.status_code} {r.text}", "ERROR")
            return False
    except Exception as e:
        log(f"Exception saving deck {deck_id}: {e}", "ERROR")
        return False


# ----------------------------------------------------
# Import decks in batches
# ----------------------------------------------------
def import_decks_batch(deck_ids: List[int], state=None, deadline=None) -> Dict:
    """Import a batch of deck IDs and return statistics."""
    state = state or RetryState()
    stats = {"success": 0, "failed": 0, "skipped": 0, "error": "", "budget_exhausted": False,
             "saved_ids": []}
    invalid_streak = write_failure_streak = 0

    for i, deck_id in enumerate(deck_ids, 1):
        log(f"[{i}/{len(deck_ids)}] Processing deck {deck_id}...")

        if deadline is not None and time.monotonic() >= deadline:
            stats["budget_exhausted"] = True
            break
        try:
            text = fetch_deck_text(deck_id)
            if text is None:
                state.failed(deck_id, "missing_404_410", 86400)
                stats["failed"] += 1
                log(f"Deck {deck_id} is unavailable; retrying in 24 hours", "WARNING")
                continue
            json_decklist = process_decklist_to_json(text)
            invalid_streak = 0
            if save_deck_to_supabase(deck_id, json_decklist):
                log(f"Successfully saved deck {deck_id}")
                stats["success"] += 1
                stats["saved_ids"].append(deck_id)
                state.saved(deck_id)
                write_failure_streak = 0
            else:
                stats["failed"] += 1
                state.failed(deck_id, "database_write", 300)
                write_failure_streak += 1
                if write_failure_streak >= 3:
                    raise MTGGoldfishUnavailable("Three consecutive database writes failed")
        except InvalidDeckWidget as e:
            stats["failed"] += 1
            state.failed(deck_id, "invalid_widget", 21600)
            invalid_streak += 1
            log(str(e), "ERROR")
            if invalid_streak >= 3:
                stats["error"] = "Three consecutive invalid widgets; source markup may have changed"
                state.pause(stats["error"])
                break
        except MTGGoldfishUnavailable as e:
            stats["error"] = str(e)
            state.pause(stats["error"])
            log(stats["error"], "ERROR")
            break

        # Optional extra delay (generally keep this low; MTGGoldfish jitter does the heavy lifting)
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)

    return stats


# ----------------------------------------------------
# Main sync function (bounded batches)
# ----------------------------------------------------
def write_run_report(report):
    """Publish counts, continuation eligibility and health without credentials."""
    report_path = os.environ.get("DECK_SYNC_REPORT_FILE")
    if report_path:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as stream:
            stream.write(f"needs_followup={str(report['needs_followup']).lower()}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        rows = [(key.replace("_", " ").title(), report.get(key, "unknown")) for key in
                ("status", "missing_before", "success", "failed", "missing_after", "eligible_after",
                 "deferred", "runtime_seconds", "oldest_pending_hours", "trigger", "next_retry_utc")]
        rows.extend((key.replace("_", " ").title(), report.get(key, "unknown")) for key in
                    ("league_missing", "challenge_missing", "oldest_result_date", "invalid_cache_rows"))
        text = "## Deck synchronization\n\n| Metric | Value |\n| --- | --- |\n"
        text += "".join(f"| {key} | {value} |\n" for key, value in rows)
        if report.get("error"):
            text += "\nError: " + str(report["error"]).replace("\n", " ").replace("|", "\\|") + "\n"
        if report.get("oldest_pending_hours", 0) > 2:
            text += "\nWarning: missing decks have remained pending for more than two hours.\n"
        if report.get("no_progress_runs", 0) >= 3 or report.get("growing_runs", 0) >= 3:
            text += "\nWarning: the backlog is growing or not progressing across repeated runs.\n"
        if report.get("invalid_cache_rows", 0):
            text += "\nWarning: existing null/empty cache rows need repair.\n"
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(text)
    log("SYNC SUMMARY " + json.dumps(report))


def sync_missing_decks() -> int:
    started = time.monotonic()
    state = None
    report = {"status": "error", "success": 0, "failed": 0, "needs_followup": False,
              "trigger": os.environ.get("GITHUB_EVENT_NAME", "local")}
    try:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
            raise ValueError("Missing required SUPABASE_URL or SUPABASE_SERVICE_ROLE environment variable")
        if MAX_DECKS_PER_RUN < 0 or MAX_RUN_SECONDS <= 0:
            raise ValueError("Invalid deck count or runtime limit")
        continuation = int(os.environ.get("DECK_SYNC_CONTINUATION", "0"))
        if not 0 <= continuation <= 3:
            raise ValueError("Continuation index must be between 0 and 3")
        state = RetryState(os.environ.get("DECK_SYNC_STATE_FILE"))
        missing = get_missing_ids(MAX_BACKLOG_QUERY)
        state.observe(missing)
        report["missing_before"] = len(missing)
        eligible = [deck_id for deck_id in missing if state.ready(deck_id)]
        saved_ids = []
        if not state.blocked():
            selected = eligible[:MAX_DECKS_PER_RUN] if MAX_DECKS_PER_RUN else eligible
            stats = import_decks_batch(selected, state, started + MAX_RUN_SECONDS)
            saved_ids = stats.pop("saved_ids")
            report.update(stats)
        else:
            report["error"] = "Sync cooldown active: " + state.data["global_error"]
        after = get_missing_ids(MAX_BACKLOG_QUERY) if missing else []
        unconfirmed = set(saved_ids).intersection(after)
        for deck_id in unconfirmed:
            state.failed(deck_id, "unconfirmed_write", 300)
            log(f"Deck {deck_id} is still missing after an accepted write; deferring retry", "ERROR")
        report["success"] -= len(unconfirmed)
        report["failed"] += len(unconfirmed)
        progress = bool(set(missing).difference(after))
        state.observe(after)
        ready = [deck_id for deck_id in after if state.ready(deck_id)]
        report.update(missing_after=len(after), eligible_after=len(ready), deferred=len(after) - len(ready))
        if os.environ.get("DECK_SYNC_CLASSIFY_COUNTS") == "true":
            try:
                report.update(classify_missing_decks(after))
            except (requests.RequestException, RuntimeError, ValueError, KeyError) as exc:
                log(f"Backlog classification unavailable: {type(exc).__name__}", "WARNING")
                report["classification_error"] = type(exc).__name__
        report["oldest_pending_hours"] = round(
            (time.time() - min(state.data["pending_since"].values())) / 3600, 2
        ) if after else 0
        if report["success"] or not after:
            state.data["last_success_at"] = time.time()
        report["status"] = ("blocked" if state.blocked() else "complete" if not after else
                            "deferred" if not ready else "partial")
        state.data["no_progress_runs"] = state.data["no_progress_runs"] + 1 if ready and not progress else 0
        previous_backlog = state.data["last_backlog"]
        state.data["growing_runs"] = state.data["growing_runs"] + 1 if previous_backlog and len(after) > previous_backlog else 0
        state.data["last_backlog"] = len(after)
        report.update(no_progress_runs=state.data["no_progress_runs"], growing_runs=state.data["growing_runs"])
        retry_times = [record["next_retry_at"] for record in state.data["decks"].values()
                       if record["next_retry_at"] > time.time()]
        if state.blocked():
            retry_times.append(state.data["global_retry_at"])
        report["next_retry_utc"] = datetime.fromtimestamp(min(retry_times), timezone.utc).isoformat() if retry_times else "none"
        report["needs_followup"] = bool(progress and report["success"] and ready and not state.blocked() and continuation < 3)
        if os.environ.get("DECK_SYNC_AUDIT_CACHE") == "true":
            try:
                report["invalid_cache_rows"] = audit_cached_decks()
                if report["invalid_cache_rows"]:
                    report["status"] = "needs_cache_repair"
            except (requests.RequestException, RuntimeError) as exc:
                report["cache_audit_error"] = type(exc).__name__
                log("Cache integrity audit could not be completed", "WARNING")
        return 1 if report["failed"] or report.get("error") or report.get("invalid_cache_rows") else 0
    except Exception as exc:
        report["error"] = str(exc)
        if state is not None:
            state.pause(str(exc), 900)
        log(str(exc), "ERROR")
        return 1
    finally:
        report["runtime_seconds"] = round(time.monotonic() - started, 2)
        if state is not None:
            state.save()
        write_run_report(report)


# -------------------------------
# Entry point
# -------------------------------
def check_decks(deck_ids: List[int]) -> int:
    """Live, read-only smoke test; does not query or write Supabase."""
    try:
        for deck_id in dict.fromkeys(deck_ids):
            text = fetch_deck_text(deck_id)
            if text is None:
                raise MTGGoldfishUnavailable(f"Deck {deck_id} is missing at the widget endpoint")
            deck = process_decklist_to_json(text)
            main_count = sum(card["count"] for card in deck["mainboard"])
            side_count = sum(card["count"] for card in deck["sideboard"])
            log(f"Validated deck {deck_id} via widget: mainboard={main_count}, sideboard={side_count}")
        log("Live checks passed; no Supabase reads or writes performed.")
        return 0
    except (MTGGoldfishUnavailable, ValueError) as exc:
        log(str(exc), "ERROR")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync missing Pauper decks using MTGGoldfish's public widget")
    parser.add_argument("--check-decks", type=int, nargs="+", metavar="DECK_ID",
                        help="Fetch and validate these decks without any Supabase access")
    args = parser.parse_args()
    exit_code = check_decks(args.check_decks) if args.check_decks else sync_missing_decks()
    sys.exit(exit_code)
