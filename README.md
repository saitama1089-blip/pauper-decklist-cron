# Pauper deck cache sync

Fetch missing deck IDs through the existing Supabase RPC, retrieve each deck from
the public MTGGoldfish widget used by tournament pages' **Expand Decks** control,
and save the existing `deck_id` / `json_decklist` payload to `deck_cache_view`.

The importer reads the widget's HTML string as data. It does not execute the
returned JavaScript or require Chrome. It verifies deck identity, Pauper format,
positive quantities, at least 60 mainboard cards, and at most 15 sideboard cards
before saving. Empty and short sideboards are allowed.

## Local tests

Use Python 3.11 with the pinned dependencies:

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Check actual deck retrieval without any Supabase reads, writes, or credentials:

```sh
python sync_decks.py --check-decks 8027855 8044902
```

The fixtures were captured from public widgets on November 17, 2026. Unrelated
markup, prices and anonymous form tokens were removed. Expected JSON retains
the original cache format, including locally constructed Scryfall image URLs.

## GitHub Actions trial

Manually run **Sync Missing Decks**, select the branch to test, and enable
`check_only`. This checks two live widgets and skips the database sync step.
The check step receives no Supabase secrets.

**Test Deck Sync** runs offline regression tests on pull requests and main-branch
pushes. Its manual `live_check` option also checks two live widgets without
database access.

## Normal imports and limits

Normal sync runs require `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE` in the
environment. The database schema, missing-ID RPC, cache payload and save endpoint
are unchanged.

The sync workflow imports at most **25 decks per run**, stops after **30 minutes**,
and allows one active sync run at a time. Already saved decks remain cached;
later triggers retrieve the remaining IDs.

| Setting | Script default | Purpose |
| --- | --- | --- |
| `MAX_DECKS_PER_RUN` | 50 | Total distinct deck attempts across all batches; 0 disables this cap. The workflow sets 25. |
| `BATCH_FETCH_LIMIT` | 200 | Maximum IDs requested in one RPC, further limited by the remaining per-run allowance. |
| `MTGGOLDFISH_MAX_RETRIES` | 3 | Maximum HTTP attempts for a temporary failure. |
| `MTGGOLDFISH_TIMEOUT` | 25 seconds | Timeout for an HTTP request. |
| `MTGGOLDFISH_MAX_BACKOFF` | 30 seconds | Largest retry delay permitted before stopping. |
| `MTGGOLDFISH_DELAY_MIN/MAX` | 3 / 7 seconds | Random pacing delay before a deck request. |

Confirmed challenge responses and HTTP 403 stop the sync immediately. HTTP
429, server failures and network failures receive bounded retries; no sleep is
performed after the last attempt. `Retry-After` supports both seconds and HTTP
dates. A longer requested delay stops the run instead of retrying too soon.

HTTP 404/410 marks an unavailable deck as failed without trying blocked download
or visual endpoints. Failed decks are not attempted again in the same run.
Partial failures return a nonzero exit status, even when some decks were saved.

The widget is a website feature whose markup can change. Unexpected content or
invalid card data stops the run with an error before that deck is saved.
