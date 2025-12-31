import requests
import os
import sys
import re
import unicodedata
import time
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import quote_plus

# ==========================
# Configuration from environment variables (NO HARDCODED SECRETS!)
# ==========================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    print("ERROR: Missing required environment variables!")
    print("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE")
    sys.exit(1)

RESULTS_TABLE = "pauper_league_results"
DECK_CACHE_TABLE = "deck_cache_view"
BATCH_SIZE = 1000
RATE_LIMIT_DELAY = 0.5

# Process max 100 decks per run (adjustable)
MAX_DECKS_PER_RUN = int(os.environ.get("MAX_DECKS_PER_RUN", "100"))


# ------------------------------
# Logging helper
# ------------------------------
def log(message: str, level: str = "INFO"):
    """Log with timestamp and level."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")
    sys.stdout.flush()


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
    Returns a deterministic URL that Scryfall will redirect to the CDN image.
    Example:
      https://api.scryfall.com/cards/named?format=image&version=normal&fuzzy=lorien+revealed
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
    Process a decklist string and return formatted JSON with Scryfall image URLs
    built locally via the 'named?format=image&fuzzy=' endpoint.
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
def get_missing_ids(limit: int = MAX_DECKS_PER_RUN) -> List[int]:
    """
    Use the RPC function get_missing_deck_ids(max_results integer)
    to find deck IDs in pauper_league_results that are NOT in deck_cache_view.
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_missing_deck_ids"

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
    }

    payload = {"max_results": limit}

    try:
        log(f"Fetching up to {limit} missing deck IDs using RPC get_missing_deck_ids...")
        r = requests.post(url, json=payload, headers=headers, timeout=30)

        if r.status_code == 200:
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
            else:
                missing_ids = []

            log(f"Found {len(missing_ids)} missing deck IDs")
            return missing_ids

        else:
            log(f"RPC call failed with status {r.status_code}: {r.text}", "ERROR")
            raise RuntimeError(f"Failed to fetch missing IDs via RPC: {r.text}")

    except Exception as e:
        log(f"Error calling RPC function: {e}", "ERROR")
        raise


# ----------------------------------------
# Fetch raw decklist text from MTGGoldfish
# ----------------------------------------
def fetch_deck_text(deck_id: int) -> Optional[str]:
    """Try to fetch deck text from MTGGoldfish."""
    endpoints = [
        f"https://www.mtggoldfish.com/deck/download/{deck_id}",
        f"https://www.mtggoldfish.com/deck/arena_download/{deck_id}",
    ]

    for url in endpoints:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                text = r.text.strip()
                if text and "<html" not in text.lower():
                    return text
        except Exception as e:
            log(f"Error fetching from {url}: {e}", "WARNING")

    return None


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
        r = requests.post(url, json=payload, headers=headers, timeout=10)

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

        # Fetch raw decklist text
        text = fetch_deck_text(deck_id)
        if not text:
            log(f"Could not fetch deck {deck_id}", "WARNING")
            stats["failed"] += 1
            continue

        try:
            # Process decklist into JSON (no Scryfall API calls)
            json_decklist = process_decklist_to_json(text)

            # Save to database
            if save_deck_to_supabase(deck_id, json_decklist):
                log(f"Successfully saved deck {deck_id}")
                stats["success"] += 1
            else:
                stats["failed"] += 1

        except Exception as e:
            log(f"Error processing deck {deck_id}: {e}", "ERROR")
            stats["failed"] += 1

        time.sleep(RATE_LIMIT_DELAY)

    return stats


# ----------------------------------------------------
# Main sync function
# ----------------------------------------------------
def sync_missing_decks() -> int:
    """
    Sync missing decks from results table to cache table.
    Returns exit code (0 for success, 1 for failure).
    """
    try:
        log("=" * 60)
        log("DECK SYNC STARTING (JSON MODE)")
        log("=" * 60)

        # Get missing IDs using efficient RPC
        missing_ids = get_missing_ids(limit=MAX_DECKS_PER_RUN)

        log("=" * 60)
        log("SYNC ANALYSIS")
        log("=" * 60)
        log(f"Missing in cache: {len(missing_ids)}")
        log(f"Will process: {min(len(missing_ids), MAX_DECKS_PER_RUN)} decks this run")

        if not missing_ids:
            log("No missing decks to import. Cache is up to date!")
            return 0

        # Process decks
        log("=" * 60)
        log(f"PROCESSING {len(missing_ids)} DECKS")
        log("=" * 60)

        stats = import_decks_batch(missing_ids)

        # Final summary
        log("=" * 60)
        log("FINAL SUMMARY")
        log("=" * 60)
        log(f"Successfully imported: {stats['success']}")
        log(f"Failed to import: {stats['failed']}")
        log(f"Total processed: {len(missing_ids)}")
        if len(missing_ids) > 0:
            log(f"Success rate: {stats['success'] / len(missing_ids) * 100:.1f}%")
        log("=" * 60)

        return 0 if stats["success"] > 0 or len(missing_ids) == 0 else 1

    except Exception as e:
        log(f"Fatal error: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return 1


# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    exit_code = sync_missing_decks()
    sys.exit(exit_code)
