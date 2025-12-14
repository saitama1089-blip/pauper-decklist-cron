import requests
import os
import sys
import re
import unicodedata
import time
from typing import List, Dict, Optional
from datetime import datetime

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

    name = unicodedata.normalize('NFKC', name)
    name = name.replace(''', "'").replace(''', "'")
    name = name.replace('"', '"').replace('"', '"')

    return name


# ------------------------------
# Parse decklist into structured format
# ------------------------------
def parse_decklist(decklist: str) -> Dict[str, List[Dict[str, any]]]:
    """
    Parse a decklist string into mainboard and sideboard sections.
    Sections are separated by a blank line.
    """
    lines = decklist.strip().split('\n')

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

        match = re.match(r'^(\d+)\s+(.+)$', line)
        if match:
            count = int(match.group(1))
            card_name = match.group(2).strip()
        else:
            count = 1
            card_name = line.strip()

        if card_name:
            current_section.append({
                'name': card_name,
                'count': count
            })

    return {
        'mainboard': mainboard,
        'sideboard': sideboard
    }


# ------------------------------
# Fetch card data from Scryfall
# ------------------------------
def fetch_cards_from_scryfall(card_names: List[str], chunk_size: int = 75) -> Dict[str, Dict]:
    """Fetch card data from Scryfall's collection endpoint in batches."""
    card_data_by_name = {}
    unique_names = list(set(card_names))

    for i in range(0, len(unique_names), chunk_size):
        chunk = unique_names[i:i + chunk_size]
        identifiers = [{'name': name} for name in chunk]

        try:
            response = requests.post(
                'https://api.scryfall.com/cards/collection',
                json={'identifiers': identifiers},
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                cards = data.get('data', [])

                for card in cards:
                    if not card:
                        continue

                    if 'card_faces' in card and card['card_faces']:
                        for idx, face in enumerate(card['card_faces']):
                            if face and 'name' in face:
                                key = normalize_card_name(face['name']).lower()
                                card_data_by_name[key] = {
                                    'card': card,
                                    'face_index': idx
                                }
                    else:
                        if 'name' in card:
                            key = normalize_card_name(card['name']).lower()
                            card_data_by_name[key] = {
                                'card': card,
                                'face_index': None
                            }
            else:
                log(f"Scryfall API returned status {response.status_code}", "WARNING")

        except Exception as e:
            log(f"Error fetching chunk from Scryfall: {e}", "WARNING")

        # Rate limiting for Scryfall API
        if i + chunk_size < len(unique_names):
            time.sleep(0.1)

    return card_data_by_name


# ------------------------------
# Get card image URL
# ------------------------------
def get_card_image_url(card_data: Dict, face_index: Optional[int] = None) -> str:
    """Extract the normal/large image URL from card data."""
    card = card_data.get('card', {})
    face_idx = card_data.get('face_index', face_index)

    if face_idx is not None and 'card_faces' in card:
        faces = card['card_faces']
        if face_idx < len(faces) and 'image_uris' in faces[face_idx]:
            image_uris = faces[face_idx]['image_uris']
            return image_uris.get('normal') or image_uris.get('large') or image_uris.get('small', '')

    if 'image_uris' in card:
        image_uris = card['image_uris']
        return image_uris.get('normal') or image_uris.get('large') or image_uris.get('small', '')

    if 'card_faces' in card and card['card_faces']:
        first_face = card['card_faces'][0]
        if 'image_uris' in first_face:
            image_uris = first_face['image_uris']
            return image_uris.get('normal') or image_uris.get('large') or image_uris.get('small', '')

    return ''


# ------------------------------
# Process decklist into JSON with Scryfall data
# ------------------------------
def process_decklist_to_json(decklist: str) -> Dict:
    """
    Process a decklist string and return formatted JSON with Scryfall URLs.
    """
    parsed = parse_decklist(decklist)

    all_card_names = []
    for card in parsed['mainboard']:
        all_card_names.append(normalize_card_name(card['name']))
    for card in parsed['sideboard']:
        all_card_names.append(normalize_card_name(card['name']))

    log(f"Fetching {len(set(all_card_names))} unique cards from Scryfall...")
    card_data = fetch_cards_from_scryfall(all_card_names)

    result = {
        'mainboard': [],
        'sideboard': []
    }

    for card in parsed['mainboard']:
        normalized_name = normalize_card_name(card['name']).lower()
        card_info = card_data.get(normalized_name)

        result['mainboard'].append({
            'name': card['name'],
            'count': card['count'],
            'scryfall_url': get_card_image_url(card_info) if card_info else ''
        })

    for card in parsed['sideboard']:
        normalized_name = normalize_card_name(card['name']).lower()
        card_info = card_data.get(normalized_name)

        result['sideboard'].append({
            'name': card['name'],
            'count': card['count'],
            'scryfall_url': get_card_image_url(card_info) if card_info else ''
        })

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
            # Process decklist into JSON with Scryfall data
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

        # Return success if we processed at least some decks successfully
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
