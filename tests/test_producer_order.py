"""Check that producer disorder modes preserve rows and meet their bounds."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from producer.transaction_producer import disorder_rows, read_rows


class ProducerOrderTests(unittest.TestCase):
    def test_disorder_modes_preserve_all_transactions(self):
        path = Path("data/transactions.csv")
        rows = read_rows(path)
        for threshold in (0, 30, 61):
            result = list(disorder_rows(rows, threshold))
            self.assertEqual(len(result), len(rows))
            self.assertCountEqual([r["transaction_id"] for r in result], [r["transaction_id"] for r in rows])

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
