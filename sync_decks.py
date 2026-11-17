import requests
import os
import sys
import re
import unicodedata
import time
import random
import math
import argparse
from typing import List, Dict, Optional
from datetime import datetime, timezone
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
MAX_DECKS_PER_RUN = int(os.environ.get("MAX_DECKS_PER_RUN", "50"))

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

    payload = {"max_results": limit}

    log(f"Fetching up to {limit} missing deck IDs using RPC get_missing_deck_ids...")
    r = requests.post(url, json=payload, headers=headers, timeout=30)

    if r.status_code != 200:
        log(f"RPC call failed with status {r.status_code}: {r.text}", "ERROR")
        raise RuntimeError(f"Failed to fetch missing IDs via RPC: {r.text}")

    result = r.json()
    missing_ids: List[int] = []

    # Handle both possible return shapes:
    # 1) [{"deck_id": 123}, ...]
    # 2) [123, 456, ...]
    if isinstance(result, list) and result:
        if isinstance(result[0], dict):
            for row in result:
                val = row.get("deck_id") or row.get("id")
                if val is not None:
                    missing_ids.append(int(val))
        else:
            missing_ids = [int(x) for x in result]

    log(f"Found {len(missing_ids)} missing deck IDs")
    return missing_ids


# ----------------------------------------
# MTGGoldfish throttle detection + backoff
# ----------------------------------------
class MTGGoldfishUnavailable(RuntimeError):
    """Stop the entire sync when the upstream is blocked or unavailable."""


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
        raise MTGGoldfishUnavailable(f"Invalid widget for deck {deck_id}: {exc}. Stopping the sync.") from exc


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
def import_decks_batch(deck_ids: List[int]) -> Dict[str, int]:
    """Import a batch of deck IDs and return statistics."""
    stats = {"success": 0, "failed": 0, "skipped": 0}

    for i, deck_id in enumerate(deck_ids, 1):
        log(f"[{i}/{len(deck_ids)}] Processing deck {deck_id}...")

        text = fetch_deck_text(deck_id)
        if not text:
            log(f"Deck {deck_id} is missing at the widget endpoint (HTTP 404/410)", "WARNING")
            stats["failed"] += 1
            continue

        try:
            json_decklist = process_decklist_to_json(text)

            if save_deck_to_supabase(deck_id, json_decklist):
                log(f"Successfully saved deck {deck_id}")
                stats["success"] += 1
            else:
                stats["failed"] += 1

        except Exception as e:
            log(f"Error processing deck {deck_id}: {e}", "ERROR")
            stats["failed"] += 1

        # Optional extra delay (generally keep this low; MTGGoldfish jitter does the heavy lifting)
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)

    return stats


# ----------------------------------------------------
# Main sync function (bounded batches)
# ----------------------------------------------------
def sync_missing_decks() -> int:
    """
    Sync missing decks from results table to cache table.
    Process batches up to the configured per-run limit.
    """
    try:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
            log("Missing required SUPABASE_URL or SUPABASE_SERVICE_ROLE environment variable", "ERROR")
            return 1
        log("=" * 60)
        log("DECK SYNC STARTING (JSON MODE)")
        log(f"Source: public deck widget; per-run limit: {MAX_DECKS_PER_RUN or 'unlimited'}")
        log("=" * 60)

        total_stats = {"success": 0, "failed": 0, "skipped": 0}
        batch_num = 0
        attempted_ids = set()

        saw_missing_any = False

        while True:
            batch_num += 1

            limit = BATCH_FETCH_LIMIT
            if MAX_DECKS_PER_RUN > 0:
                remaining = MAX_DECKS_PER_RUN - len(attempted_ids)
                if remaining <= 0:
                    log(f"Reached the per-run limit of {MAX_DECKS_PER_RUN} decks.")
                    break
                limit = min(limit, remaining)
            missing_ids = get_missing_ids(limit=limit)

            log("=" * 60)
            log(f"BATCH {batch_num} ANALYSIS")
            log("=" * 60)
            log(f"Missing in cache (this batch): {len(missing_ids)}")

            if not missing_ids:
                log("No missing decks to import. Cache is up to date!")
                break

            missing_ids = list(dict.fromkeys(
                deck_id for deck_id in missing_ids if deck_id not in attempted_ids
            ))
            if MAX_DECKS_PER_RUN > 0:
                missing_ids = missing_ids[:remaining]
            if not missing_ids:
                log(
                    "Only previously attempted decks remain in this batch; deferring them to a later run.",
                    "WARNING",
                )
                break

            saw_missing_any = True

            log("=" * 60)
            log(f"PROCESSING {len(missing_ids)} DECKS (BATCH {batch_num})")
            log("=" * 60)

            attempted_ids.update(missing_ids)
            stats = import_decks_batch(missing_ids)

            total_stats["success"] += stats["success"]
            total_stats["failed"] += stats["failed"]
            total_stats["skipped"] += stats["skipped"]

            log("=" * 60)
            log(f"BATCH {batch_num} SUMMARY")
            log("=" * 60)
            log(f"Imported: {stats['success']}")
            log(f"Failed: {stats['failed']}")
            log(f"Skipped: {stats['skipped']}")

            # If we failed all of them, continuing could loop forever (same IDs keep returning)
            if stats["success"] == 0 and stats["failed"] > 0:
                log(
                    "No successes in this batch; stopping to avoid repeating the same failed decks. "
                    "Retry on a later run.",
                    "WARNING",
                )
                break

            # Be polite between batches (small pause)
            time.sleep(random.uniform(2.0, 5.0))

        log("=" * 60)
        log("FINAL SUMMARY (ALL BATCHES)")
        log("=" * 60)
        log(f"Successfully imported: {total_stats['success']}")
        log(f"Failed to import: {total_stats['failed']}")
        log(f"Skipped: {total_stats['skipped']}")
        log("=" * 60)

        # Empty queues succeed; incomplete imports remain visible as failures.
        if not saw_missing_any:
            return 0
        return 0 if total_stats["failed"] == 0 and total_stats["success"] > 0 else 1

    except MTGGoldfishUnavailable as e:
        log(str(e), "ERROR")
        return 1
    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return 1


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
