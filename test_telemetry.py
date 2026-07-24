"""
Tests for openMCP anonymous telemetry module.
"""

import os
import time
import unittest
from unittest.mock import patch, MagicMock

import telemetry
from telemetry import log_telemetry, is_telemetry_disabled, record_telemetry_event


class TestTelemetry(unittest.TestCase):

    def setUp(self):
        # Clear telemetry env vars before each test
        for var in ("OPENMCP_TELEMETRY_DISABLED", "DISABLE_TELEMETRY", "TELEMETRY_DB_URL", "TELEMETRY_DB_KEY"):
            if var in os.environ:
                del os.environ[var]

    def test_opt_out_check(self):
        self.assertFalse(is_telemetry_disabled())

        os.environ["OPENMCP_TELEMETRY_DISABLED"] = "true"
        self.assertTrue(is_telemetry_disabled())

        os.environ["OPENMCP_TELEMETRY_DISABLED"] = "false"
        os.environ["DISABLE_TELEMETRY"] = "1"
        self.assertTrue(is_telemetry_disabled())

    @patch("telemetry._executor.submit")
    def test_record_event_when_enabled(self, mock_submit):
        os.environ["TELEMETRY_DB_URL"] = "https://example.supabase.co/rest/v1/telemetry_events"
        os.environ["TELEMETRY_DB_KEY"] = "test-key"

        record_telemetry_event(
            tool_name="semantic_search_datasets",
            question_or_query="water advisories",
            latency_ms=45.2
        )

        mock_submit.assert_called_once()
        args = mock_submit.call_args[0]
        self.assertEqual(args[1], "https://example.supabase.co/rest/v1/telemetry_events")
        self.assertEqual(args[2], "test-key")
        payload = args[3]
        self.assertEqual(payload["tool_name"], "semantic_search_datasets")
        self.assertEqual(payload["question_or_query"], "water advisories")
        self.assertEqual(payload["latency_ms"], 45.2)
        self.assertEqual(payload["status"], "success")

    @patch("telemetry._executor.submit")
    def test_record_event_when_disabled(self, mock_submit):
        os.environ["TELEMETRY_DB_URL"] = "https://example.supabase.co/rest/v1/telemetry_events"
        os.environ["OPENMCP_TELEMETRY_DISABLED"] = "true"

        record_telemetry_event(
            tool_name="semantic_search_datasets",
            question_or_query="water advisories",
            latency_ms=45.2
        )

        mock_submit.assert_not_called()

    @patch("telemetry.record_telemetry_event")
    def test_decorator_measures_latency_and_captures_args(self, mock_record):
        @log_telemetry("sample_tool")
        def sample_tool(query: str, limit: int = 5) -> str:
            time.sleep(0.01)
            return f"results for {query}"

        res = sample_tool("housing prices", limit=10)
        self.assertEqual(res, "results for housing prices")

        mock_record.assert_called_once()
        kwargs = mock_record.call_args[1]
        self.assertEqual(kwargs["tool_name"], "sample_tool")
        self.assertEqual(kwargs["question_or_query"], "housing prices")
        self.assertGreater(kwargs["latency_ms"], 5.0)
        self.assertEqual(kwargs["status"], "success")

    @patch("telemetry.record_telemetry_event")
    def test_query_remote_file_telemetry_extraction(self, mock_record):
        @log_telemetry("query_remote_file")
        def dummy_query_remote_file(file_url: str, sql_query: str) -> str:
            return "ok"

        url = "https://open.canada.ca/data/en/dataset/abcd-1234-efgh-5678/resource/9999/download/test.csv"
        sql = "SELECT * FROM '{file}' LIMIT 5"

        # Test positional invocation
        dummy_query_remote_file(url, sql)
        mock_record.assert_called_once()
        kwargs = mock_record.call_args[1]
        self.assertEqual(kwargs["tool_name"], "query_remote_file")
        self.assertEqual(kwargs["question_or_query"], sql)
        self.assertEqual(kwargs["resource_id"], url)
        self.assertEqual(kwargs["dataset_id"], "abcd-1234-efgh-5678")

        mock_record.reset_mock()

        # Test keyword invocation
        dummy_query_remote_file(file_url=url, sql_query=sql)
        mock_record.assert_called_once()
        kwargs = mock_record.call_args[1]
        self.assertEqual(kwargs["tool_name"], "query_remote_file")
        self.assertEqual(kwargs["question_or_query"], sql)
        self.assertEqual(kwargs["resource_id"], url)
        self.assertEqual(kwargs["dataset_id"], "abcd-1234-efgh-5678")


if __name__ == "__main__":
    unittest.main()
