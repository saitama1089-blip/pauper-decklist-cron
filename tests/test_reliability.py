import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import zipfile
from datetime import datetime, timezone

import requests
import sync_decks as sync
from deck_sync_state import RetryState
from helpers import response
from scripts.restore_sync_state import restore
from scripts.dispatch_followup import dispatch
from scripts.check_sync_health import source_age_minutes


class WorkerReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.directory = self.enterContext(tempfile.TemporaryDirectory())
        self.path = str(Path(self.directory) / "deck-sync.json")
        self.enterContext(patch.dict(os.environ, {"DECK_SYNC_STATE_FILE": self.path,
                        "DECK_SYNC_REPORT_FILE": "", "GITHUB_OUTPUT": "", "GITHUB_STEP_SUMMARY": "",
                        "DECK_SYNC_CONTINUATION": "0"}))
        self.enterContext(patch.object(sync, "SUPABASE_URL", "https://example.invalid"))
        self.enterContext(patch.object(sync, "SUPABASE_SERVICE_ROLE", "test-only"))
        self.enterContext(patch.object(sync, "MAX_DECKS_PER_RUN", 100))
        self.enterContext(patch.object(sync, "MAX_RUN_SECONDS", 1200))
        self.report = self.enterContext(patch.object(sync, "write_run_report"))
        self.enterContext(patch.object(sync, "log"))
        self.missing = self.enterContext(patch.object(sync, "get_missing_ids"))
        self.fetch = self.enterContext(patch.object(sync, "fetch_deck_text", return_value="60 Island"))
        self.save = self.enterContext(patch.object(sync, "save_deck_to_supabase", return_value=True))

    def test_missing_deck_is_deferred_while_healthy_deck_imports(self):
        self.missing.side_effect = [[1, 2], [1]]
        self.fetch.side_effect = [None, "60 Island"]
        self.assertEqual(sync.sync_missing_decks(), 1)
        self.save.assert_called_once()
        record = RetryState(self.path).data["decks"]["1"]
        self.assertEqual(record["last_error"], "missing_404_410")
        self.assertGreater(record["next_retry_at"], time.time() + 86000)
        self.assertEqual(self.report.call_args.args[0]["deferred"], 1)

    def test_next_worker_skips_deferred_id_and_imports_new_id(self):
        state = RetryState(self.path)
        state.failed(1, "missing_404_410", 86400)
        state.save()
        self.missing.side_effect = [[1, 3], [1]]
        self.assertEqual(sync.sync_missing_decks(), 0)
        self.fetch.assert_called_once_with(3)
        self.assertEqual(self.report.call_args.args[0]["status"], "deferred")

    def test_expired_deferred_deck_is_retried(self):
        state = RetryState(self.path)
        state.failed(1, "missing_404_410", -1)
        state.save()
        self.missing.side_effect = [[1], []]
        self.assertEqual(sync.sync_missing_decks(), 0)
        self.fetch.assert_called_once_with(1)
        self.assertEqual(RetryState(self.path).data["decks"], {})

    def test_global_challenge_persists_cooldown_and_stops(self):
        self.missing.return_value = [1, 2]
        self.fetch.side_effect = sync.MTGGoldfishUnavailable("Cloudflare challenge")
        self.assertEqual(sync.sync_missing_decks(), 1)
        self.fetch.assert_called_once()
        self.assertTrue(RetryState(self.path).blocked())
        self.assertFalse(self.report.call_args.args[0]["needs_followup"])

    def test_cooldown_worker_does_not_request_provider(self):
        state = RetryState(self.path)
        state.pause("blocked")
        state.save()
        self.missing.return_value = [1]
        self.assertEqual(sync.sync_missing_decks(), 1)
        self.fetch.assert_not_called()

    def test_progress_with_remaining_work_requests_one_followup(self):
        self.enterContext(patch.object(sync, "MAX_DECKS_PER_RUN", 1))
        self.missing.side_effect = [[1, 2], [2]]
        self.assertEqual(sync.sync_missing_decks(), 0)
        self.fetch.assert_called_once_with(1)
        self.assertTrue(self.report.call_args.args[0]["needs_followup"])

    def test_followup_limit_prevents_endless_chaining(self):
        self.enterContext(patch.dict(os.environ, {"DECK_SYNC_CONTINUATION": "3"}))
        self.enterContext(patch.object(sync, "MAX_DECKS_PER_RUN", 1))
        self.missing.side_effect = [[1, 2], [2]]
        self.assertEqual(sync.sync_missing_decks(), 0)
        self.assertFalse(self.report.call_args.args[0]["needs_followup"])

    def test_zero_progress_never_requests_followup(self):
        self.missing.return_value = [1]
        self.fetch.return_value = None
        self.assertEqual(sync.sync_missing_decks(), 1)
        self.assertFalse(self.report.call_args.args[0]["needs_followup"])

    def test_accepted_write_must_remove_id_from_missing_queue(self):
        self.missing.return_value = [1]
        self.assertEqual(sync.sync_missing_decks(), 1)
        report = self.report.call_args.args[0]
        self.assertEqual(report["success"], 0)
        self.assertEqual(report["failed"], 1)
        self.assertFalse(report["needs_followup"])
        self.assertEqual(RetryState(self.path).data["decks"]["1"]["last_error"], "unconfirmed_write")

    def test_runtime_budget_stops_before_next_provider_request(self):
        self.enterContext(patch.object(sync, "MAX_RUN_SECONDS", 1))
        self.missing.return_value = [1]
        with patch.object(sync.time, "monotonic", side_effect=[0, 2, 2]):
            self.assertEqual(sync.sync_missing_decks(), 0)
        self.fetch.assert_not_called()
        self.assertTrue(self.report.call_args.args[0]["budget_exhausted"])

    def test_three_bad_widgets_stop_broad_schema_failure(self):
        self.missing.return_value = [1, 2, 3, 4]
        self.fetch.side_effect = sync.InvalidDeckWidget("missing deck field")
        self.assertEqual(sync.sync_missing_decks(), 1)
        self.assertEqual(self.fetch.call_count, 3)
        self.assertTrue(RetryState(self.path).blocked())

    def test_repeated_blocked_runs_are_visible_in_health_counters(self):
        state = RetryState(self.path)
        state.pause("blocked")
        state.save()
        self.missing.return_value = [1]
        for _ in range(3):
            self.assertEqual(sync.sync_missing_decks(), 1)
        self.assertEqual(self.report.call_args.args[0]["no_progress_runs"], 3)
        self.fetch.assert_not_called()


class StateAndPaginationTests(unittest.TestCase):
    def test_cache_audit_counts_null_and_empty_mainboards(self):
        pages = [response(206, headers={"Content-Range": "0-0/2"}),
                 response(206, headers={"Content-Range": "0-0/1"})]
        with patch.object(sync.requests, "head", side_effect=pages):
            self.assertEqual(sync.audit_cached_decks(), 3)

    def test_backlog_breakdown_counts_distinct_decks_and_oldest_date(self):
        pages = [response(200, json.dumps([{"id": 1, "event_date": "2026-11-01"}])),
                 response(200, json.dumps([{"deck_id": 2, "date": "2026-11-03"}]))]
        with patch.object(sync.requests, "get", side_effect=pages):
            self.assertEqual(sync.classify_missing_decks([1, 2]), {
                "league_missing": 1, "challenge_missing": 1, "oldest_result_date": "2026-11-01"})

    def test_state_prunes_cached_ids_and_preserves_first_seen_time(self):
        state = RetryState()
        state.observe([1, 2], now=100)
        state.failed(1, "missing", 100, now=100)
        state.observe([2, 3], now=200)
        self.assertEqual(state.data["pending_since"], {"2": 100, "3": 200})
        self.assertEqual(state.data["decks"], {})

    def test_invalid_state_timestamps_are_rejected(self):
        data = RetryState().data
        data["global_retry_at"] = float("nan")
        with self.assertRaises(ValueError):
            RetryState.validate(data)

    def test_rpc_paginates_beyond_supabase_row_cap(self):
        first = response(206, json.dumps(list(range(1, 1001))), {"Content-Range": "0-999/1250"})
        second = response(206, json.dumps(list(range(1001, 1251))), {"Content-Range": "1000-1249/1250"})
        with patch.object(sync.requests, "post", side_effect=[first, second]) as post, patch.object(sync, "log"):
            self.assertEqual(len(sync.get_missing_ids(2000)), 1250)
        self.assertEqual(post.call_args_list[1].kwargs["headers"]["Range"], "1000-1999")

    def test_duplicate_rpc_pages_do_not_silently_hide_backlog(self):
        page = response(206, json.dumps(list(range(1000))), {"Content-Range": "0-999/2000"})
        with patch.object(sync.requests, "post", return_value=page), self.assertRaises(RuntimeError):
            sync.get_missing_ids(3000)


def api_response(data=None, content=None):
    result = Mock()
    result.json.return_value = data
    result.iter_content.return_value = [content] if content else []
    return result


class ActionsHelperTests(unittest.TestCase):
    def test_restore_accepts_only_completed_main_worker_state(self):
        state = RetryState().data
        state["decks"] = {"1": {"attempts": 1, "next_retry_at": 100, "last_error": "missing"}}
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("deck-sync.json", json.dumps(state))
        session = Mock(headers={})
        session.get.side_effect = [api_response({"artifacts": [
            {"id": 30, "expired": False, "workflow_run": {"id": 300, "head_branch": "feature"}},
            {"id": 20, "expired": False, "workflow_run": {"id": 200, "head_branch": "main"}},
        ]}), api_response({"status": "completed", "head_branch": "main", "path": ".github/workflows/sync-decks.yml"}),
            api_response(content=buffer.getvalue())]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "state.json"
            self.assertTrue(restore("owner/repo", "test-token", target, current_run="400", session=session))
            self.assertIn("1", RetryState(target).data["decks"])
        self.assertEqual(session.get.call_count, 3)

    def test_restore_ignores_other_workflows(self):
        session = Mock(headers={})
        session.get.side_effect = [api_response({"artifacts": [
            {"id": 20, "expired": False, "workflow_run": {"id": 200, "head_branch": "main"}},
        ]}), api_response({"status": "completed", "head_branch": "main", "path": ".github/workflows/unrelated.yml"})]
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(restore("owner/repo", "test-token", Path(directory) / "state.json", session=session))

    def test_followup_dispatch_uses_bounded_typed_inputs(self):
        session = Mock(headers={})
        session.post.return_value = response(204)
        self.assertTrue(dispatch("owner/repo", "test-token", {"needs_followup": True}, 1, 100, session))
        self.assertEqual(session.post.call_args.kwargs["json"], {
            "ref": "main", "inputs": {"check_only": False, "max_decks": 100, "continuation": 2}})

    def test_no_progress_or_capped_followup_does_not_dispatch(self):
        session = Mock(headers={})
        self.assertFalse(dispatch("owner/repo", "test-token", {"needs_followup": False}, 0, 100, session))
        self.assertFalse(dispatch("owner/repo", "test-token", {"needs_followup": True}, 3, 100, session))
        session.post.assert_not_called()

    def test_rejected_dispatch_is_not_treated_as_success(self):
        session = Mock(headers={})
        session.post.return_value = response(403)
        with self.assertRaises(requests.HTTPError):
            dispatch("owner/repo", "test-token", {"needs_followup": True}, 0, 100, session)
        session.post.assert_called_once()

    def test_temporary_dispatch_failure_can_recover(self):
        session = Mock(headers={})
        session.post.side_effect = [requests.Timeout("offline"), response(503), response(204)]
        with patch("scripts.dispatch_followup.time.sleep") as sleep:
            self.assertTrue(dispatch("owner/repo", "test-token", {"needs_followup": True}, 0, 100, session))
        self.assertEqual(session.post.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_upstream_freshness_is_measured_separately(self):
        session = Mock(headers={})
        session.get.return_value = api_response({"workflow_runs": [{"updated_at": "2026-11-17T10:00:00Z"}]})
        now = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
        self.assertEqual(source_age_minutes(session, now), 120)


if __name__ == "__main__":
    unittest.main()
