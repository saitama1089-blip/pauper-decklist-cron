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

The sync workflow imports at most **100 decks per run**, has a **20-minute
processing budget** and a **30-minute hard timeout**, and allows one active sync
run at a time. Already saved decks remain cached.

It runs after the league/challenge scraper and independently **every hour at
minute 17**. A productive run with eligible work remaining can request up to
three automatic continuations. An empty queue exits without fetching widgets.
Each run reads the full missing-ID queue, including pages beyond Supabase's
1,000-row response limit, so missed notifications do not discard work.

The scraper's half-hour schedule uses minutes **7 and 37**. Its dispatch runs
after non-cancelled scrape attempts, including partial failures, and rejects
failed HTTP dispatches instead of silently accepting them.

| Setting | Script default | Purpose |
| --- | --- | --- |
| `MAX_DECKS_PER_RUN` | 100 | Maximum eligible deck attempts. The workflow permits 1–100. |
| `MAX_RUN_SECONDS` | 1200 | Processing budget checked before each deck. |
| `MTGGOLDFISH_MAX_RETRIES` | 3 | Maximum HTTP attempts for a temporary failure. |
| `MTGGOLDFISH_TIMEOUT` | 25 seconds | Timeout for an HTTP request. |
| `MTGGOLDFISH_MAX_BACKOFF` | 30 seconds | Largest retry delay permitted before stopping. |
| `MTGGOLDFISH_DELAY_MIN/MAX` | 3 / 7 seconds | Random pacing delay before a deck request. |

Confirmed challenge responses and HTTP 403 stop the sync immediately. HTTP
429, server failures and network failures receive bounded retries; no sleep is
performed after the last attempt. `Retry-After` supports both seconds and HTTP
dates. A longer requested delay stops the run instead of retrying too soon.

HTTP 404/410 defers an unavailable deck for **24 hours** while healthy decks keep
importing. Invalid deck data is deferred for **6 hours**; three consecutive invalid
widgets stop the run to flag a possible source-format change. Repeated database
write failures also stop the run. Confirmed provider problems get a **30-minute
cooldown** shared by subsequent runs. Other fatal worker errors get 15 minutes.
Partial failures return a nonzero exit status, even when some decks were saved.

Retry state is stored in a credential-free `deck-sync-state` Actions artifact,
retained for 30 days. Only state from a completed main-branch run of this worker
is restored. This requires no database migration. The durable source of pending
work remains Supabase; an expired state artifact only resets retry cooldowns.

Actions summaries show backlog before/after, saved/failed/deferred counts,
league/challenge counts, oldest missing result date, retry time and runtime.
They flag pending work older than two hours and repeated lack of progress.
Every normal run also checks upstream scraper freshness; more than 90 minutes
since a successful scrape produces a warning. Hourly sweeps audit existing
null/empty cache payloads and report records that need repair.

GitHub schedules may be delayed or dropped. If multi-hour delays persist, use
an external scheduler/health check to request this same workflow; this change
does not provision an external scheduling service.

The widget is a website feature whose markup can change. Validation prevents
invalid card data from being saved; repeated validation failures stop for review.
