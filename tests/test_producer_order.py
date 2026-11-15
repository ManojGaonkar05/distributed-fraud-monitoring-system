"""Check that producer disorder modes preserve rows and meet their bounds."""
from __future__ import annotations

import unittest
import csv
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from producer.transaction_producer import disorder_rows, read_rows


class ProducerOrderTests(unittest.TestCase):
    def test_disorder_modes_preserve_all_transactions(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"transaction_id": str(i), "event_timestamp": (start + timedelta(seconds=i * 10)).isoformat().replace("+00:00", "Z")}
                for i in range(20)]
        for threshold in (0, 30, 61):
            result = list(disorder_rows(rows, threshold))
            self.assertEqual(len(result), len(rows))
            self.assertCountEqual([r["transaction_id"] for r in result], [r["transaction_id"] for r in rows])

    def test_csv_reader_preserves_columns_and_file_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            rows = [{"transaction_id": "T2", "event_timestamp": "2026-01-01T00:00:30Z", "amount": "20.00"},
                    {"transaction_id": "T1", "event_timestamp": "2026-01-01T00:00:00Z", "amount": "10.00"}]
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            self.assertEqual(read_rows(path), rows)

    def test_reversal_has_expected_event_time_bound(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"transaction_id": str(i), "event_timestamp": (start + timedelta(seconds=i * 10)).isoformat()}
                for i in range(20)]
        under = list(disorder_rows(rows, 30))
        over = list(disorder_rows(rows, 61))
        def maximum_lateness(result):
            values = [datetime.fromisoformat(r["event_timestamp"]).timestamp() for r in result]
            maximum_seen = values[0]
            lateness = 0
            for value in values[1:]:
                lateness = max(lateness, maximum_seen - value)
                maximum_seen = max(maximum_seen, value)
            return lateness
        self.assertGreater(maximum_lateness(under), 0)
        self.assertLess(maximum_lateness(under), 60)
        self.assertGreaterEqual(maximum_lateness(over), 61)
        self.assertLessEqual(maximum_lateness(over), 120)


if __name__ == "__main__":
    unittest.main()
