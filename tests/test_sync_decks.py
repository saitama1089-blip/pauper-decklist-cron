import os
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import call, patch

import requests
from helpers import FIXTURES, response, widget

with patch.dict(os.environ, {
    "SUPABASE_URL": "https://example.invalid",
    "SUPABASE_SERVICE_ROLE": "test-only",
}):
    import sync_decks as sync


class DeckSyncTests(unittest.TestCase):
    def setUp(self):
        self.get = self.enterContext(patch.object(sync.SESSION, "get"))
        self.post = self.enterContext(patch.object(
            sync.requests, "post", side_effect=AssertionError("Unexpected live database request")
        ))
        self.sleep = self.enterContext(patch.object(sync.time, "sleep"))
        self.log = self.enterContext(patch.object(sync, "log"))
        self.enterContext(patch.object(sync.random, "uniform", return_value=1.0))
        self.enterContext(patch.object(sync, "MTGGOLDFISH_MAX_RETRIES", 3))
        self.enterContext(patch.object(sync, "MTGGOLDFISH_MAX_BACKOFF", 30.0))
        self.enterContext(patch.object(sync, "MAX_DECKS_PER_RUN", 0))
        self.enterContext(patch.object(sync, "BATCH_FETCH_LIMIT", 2))

    def test_403_aborts_entire_200_deck_run_on_first_request(self):
        self.get.return_value = response(403)
        with patch.object(sync, "get_missing_ids", return_value=list(range(1, 201))), \
                patch.object(sync, "save_deck_to_supabase") as save:
            self.assertEqual(sync.sync_missing_decks(), 1)
        self.get.assert_called_once()
        save.assert_not_called()
        self.sleep.assert_called_once()  # Initial pacing, no retry/fallback sleep.
        self.assertTrue(any("403" in args[0] for args, _ in self.log.call_args_list))

    def test_block_after_success_still_returns_failure_and_keeps_saved_deck(self):
        self.get.side_effect = [response(200, widget(1)), response(403)]
        with patch.object(sync, "get_missing_ids", return_value=[1, 2, 3]), \
                patch.object(sync, "save_deck_to_supabase", return_value=True) as save:
            self.assertEqual(sync.sync_missing_decks(), 1)
        self.assertEqual(self.get.call_count, 2)
        save.assert_called_once()
        self.assertEqual(save.call_args.args[0], 1)

    def test_persistent_temporary_failures_stop_after_three_attempts(self):
        for status in (429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.get.reset_mock()
                self.sleep.reset_mock()
                self.get.return_value = response(status)
                with self.assertRaises(sync.MTGGoldfishUnavailable):
                    sync._get_with_backoff("https://example.invalid")
                self.assertEqual(self.get.call_count, 3)
                self.assertEqual(self.sleep.call_count, 2)  # No final useless sleep.

    def test_network_failures_stop_after_three_attempts(self):
        for error in (requests.Timeout, requests.ConnectionError):
            with self.subTest(error=error):
                self.get.reset_mock()
                self.sleep.reset_mock()
                self.get.side_effect = error("offline")
                with self.assertRaises(sync.MTGGoldfishUnavailable):
                    sync._get_with_backoff("https://example.invalid")
                self.assertEqual(self.get.call_count, 3)
                self.assertEqual(self.sleep.call_count, 2)

    def test_challenge_html_stops_on_first_request(self):
        self.get.return_value = response(200, "<html>Just a moment</html>")
        with self.assertRaises(sync.MTGGoldfishUnavailable):
            sync.fetch_deck_text(1)
        self.get.assert_called_once()
        self.assertIn("/widgets/deck/js?deckId=1", self.get.call_args.args[0])

    def test_challenge_header_alone_stops_even_with_http_200(self):
        self.get.return_value = response(200, "interstitial", {"cf-mitigated": "challenge"})
        with self.assertRaisesRegex(sync.MTGGoldfishUnavailable, "challenge"):
            sync.fetch_deck_text(1)
        self.get.assert_called_once()

    def test_temporary_throttle_can_recover(self):
        expected = response(200, "4 Island")
        self.get.side_effect = [response(429), expected]
        self.assertIs(sync._get_with_backoff("https://example.invalid"), expected)
        self.assertEqual(self.get.call_count, 2)
        self.sleep.assert_called_once_with(2.0)

    def test_numeric_retry_after_is_respected(self):
        self.get.side_effect = [response(429, headers={"Retry-After": "10"}), response(200, "4 Island")]
        sync._get_with_backoff("https://example.invalid")
        self.sleep.assert_called_once_with(10.0)

    def test_http_date_retry_after_is_respected(self):
        self.get.side_effect = [response(503, headers={
            "Retry-After": "Tue, 17 Nov 2026 12:00:20 GMT"
        }), response(200, "4 Island")]
        with patch.object(sync, "datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
            sync._get_with_backoff("https://example.invalid")
        self.sleep.assert_called_once_with(20.0)

    def test_long_retry_after_stops_instead_of_sleeping_for_hours(self):
        self.get.return_value = response(429, headers={"Retry-After": "3600"})
        with self.assertRaisesRegex(sync.MTGGoldfishUnavailable, "wait budget"):
            sync._get_with_backoff("https://example.invalid")
        self.get.assert_called_once()
        self.sleep.assert_not_called()

    def test_malformed_retry_after_uses_normal_backoff(self):
        for value in ("garbage", "NaN", "Infinity", "Tue, 17 Nov 2026 12:00:20"):
            with self.subTest(value=value):
                self.sleep.reset_mock()
                self.get.side_effect = [response(429, headers={"Retry-After": value}), response(200, "4 Island")]
                sync._get_with_backoff("https://example.invalid")
                self.sleep.assert_called_once_with(2.0)

    def test_404_and_410_are_not_retried(self):
        for status in (404, 410):
            with self.subTest(status=status):
                self.get.reset_mock()
                self.get.return_value = response(status, "<html>Not found</html>")
                self.assertIsNone(sync._get_with_backoff("https://example.invalid"))
                self.get.assert_called_once()
        self.sleep.assert_not_called()

    def test_missing_widget_does_not_request_blocked_fallbacks(self):
        self.get.return_value = response(404)
        self.assertIsNone(sync.fetch_deck_text(1))
        self.get.assert_called_once()
        self.assertIn("/widgets/deck/js?deckId=1", self.get.call_args.args[0])

    def test_widget_fetch_parses_mainboard_and_sideboard(self):
        self.get.return_value = response(200, widget(1))
        self.assertEqual(sync.fetch_deck_text(1), "60 Island\n\n15 Dispel")
        self.get.assert_called_once()

    def test_first_widget_request_is_paced(self):
        events = []
        self.sleep.side_effect = lambda _: events.append("sleep")
        self.get.side_effect = lambda *args, **kwargs: events.append("request") or response(200, widget(1))
        sync.fetch_deck_text(1)
        self.assertEqual(events, ["sleep", "request"])

    def test_per_run_limit_applies_across_batches(self):
        with patch.object(sync, "MAX_DECKS_PER_RUN", 3), \
                patch.object(sync, "get_missing_ids", side_effect=[[1, 2], [3]]) as missing, \
                patch.object(sync, "fetch_deck_text", return_value="4 Island") as fetch, \
                patch.object(sync, "save_deck_to_supabase", return_value=True) as save:
            self.assertEqual(sync.sync_missing_decks(), 0)
        self.assertEqual(missing.call_args_list, [call(limit=2), call(limit=1)])
        self.assertEqual(fetch.call_args_list, [call(1), call(2), call(3)])
        self.assertEqual(save.call_count, 3)

    def test_positive_limit_also_stops_on_total_failure(self):
        with patch.object(sync, "MAX_DECKS_PER_RUN", 10), \
                patch.object(sync, "get_missing_ids", return_value=[1, 2]) as missing, \
                patch.object(sync, "fetch_deck_text", return_value=None):
            self.assertEqual(sync.sync_missing_decks(), 1)
        missing.assert_called_once()

    def test_failed_deck_is_not_retried_in_later_batch(self):
        with patch.object(sync, "get_missing_ids", side_effect=[[1, 2], [1, 3], [1]]) as missing, \
                patch.object(sync, "fetch_deck_text", side_effect=[None, "4 Island", "4 Island"]) as fetch, \
                patch.object(sync, "save_deck_to_supabase", return_value=True):
            self.assertEqual(sync.sync_missing_decks(), 1)  # Report partial failures honestly.
        self.assertEqual(missing.call_count, 3)
        self.assertEqual(fetch.call_args_list, [call(1), call(2), call(3)])

    def test_limit_is_enforced_even_if_rpc_returns_too_many_ids(self):
        with patch.object(sync, "MAX_DECKS_PER_RUN", 1), \
                patch.object(sync, "get_missing_ids", return_value=[1, 2, 3]), \
                patch.object(sync, "fetch_deck_text", return_value="60 Island") as fetch, \
                patch.object(sync, "save_deck_to_supabase", return_value=True):
            self.assertEqual(sync.sync_missing_decks(), 0)
        fetch.assert_called_once_with(1)

    def test_invalid_widget_never_reaches_database_write(self):
        self.get.return_value = response(200, widget(1, "1 Island"))
        with patch.object(sync, "get_missing_ids", return_value=[1, 2]), \
                patch.object(sync, "save_deck_to_supabase") as save:
            self.assertEqual(sync.sync_missing_decks(), 1)
        save.assert_not_called()
        self.get.assert_called_once()

    def test_unexpected_content_type_stops_without_fallback(self):
        self.get.return_value = response(200, widget(1), {"Content-Type": "text/html"})
        with self.assertRaisesRegex(sync.MTGGoldfishUnavailable, "content type"):
            sync.fetch_deck_text(1)
        self.get.assert_called_once()

    def test_full_sync_uses_existing_rpc_and_save_payload(self):
        ids = (8027855, 8044902)
        self.get.side_effect = [response(200, (FIXTURES / f"widget-{deck_id}.js").read_text(encoding="utf-8"))
                                for deck_id in ids]
        self.post.side_effect = [response(200, json.dumps([{"deck_id": deck_id} for deck_id in ids])),
                                 response(204), response(204), response(200, "[]")]
        self.assertEqual(sync.sync_missing_decks(), 0)
        self.assertEqual(self.post.call_count, 4)
        self.assertEqual(self.post.call_args_list[0].kwargs["json"], {"max_results": 2})
        for index, deck_id in enumerate(ids, 1):
            saved = self.post.call_args_list[index]
            self.assertTrue(saved.args[0].endswith("/rest/v1/deck_cache_view"))
            expected = json.loads((FIXTURES / f"deck-{deck_id}.json").read_text(encoding="utf-8"))
            self.assertEqual(saved.kwargs["json"], expected)

    def test_database_write_failure_is_reported(self):
        with patch.object(sync, "get_missing_ids", return_value=[1]), \
                patch.object(sync, "fetch_deck_text", return_value="60 Island"), \
                patch.object(sync, "save_deck_to_supabase", return_value=False):
            self.assertEqual(sync.sync_missing_decks(), 1)

    def test_read_only_check_needs_no_credentials_and_never_accesses_supabase(self):
        self.get.return_value = response(200, widget(1))
        with patch.object(sync, "SUPABASE_URL", None), patch.object(sync, "SUPABASE_SERVICE_ROLE", None), \
                patch.object(sync, "get_missing_ids", side_effect=AssertionError("Unexpected database read")):
            self.assertEqual(sync.check_decks([1, 1]), 0)
        self.get.assert_called_once()
        self.post.assert_not_called()

    def test_read_only_check_reports_missing_or_blocked_decks(self):
        for status in (403, 404):
            with self.subTest(status=status):
                self.get.return_value = response(status)
                self.assertEqual(sync.check_decks([1]), 1)
        self.post.assert_not_called()

    def test_sync_requires_database_credentials(self):
        with patch.object(sync, "SUPABASE_URL", None):
            self.assertEqual(sync.sync_missing_decks(), 1)
        self.get.assert_not_called()
        self.post.assert_not_called()

    def test_no_missing_decks_is_success(self):
        with patch.object(sync, "get_missing_ids", return_value=[]):
            self.assertEqual(sync.sync_missing_decks(), 0)
        self.get.assert_not_called()
        self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
