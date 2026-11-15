"""Unit checks for field-level verifier behavior."""
import unittest

import pandas as pd

from verifier.verify_outputs import compare_records


class VerifierTests(unittest.TestCase):
    def test_detects_duplicate_and_wrong_rule(self):
        expected = pd.DataFrame([{"transaction_id": "T1", "r2_triggered": True,
                                  "related_alert_ids": '["A1"]'}])
        actual = pd.DataFrame([{"transaction_id": "T1", "r2_triggered": False,
                                "related_alert_ids": '["A1"]'},
                               {"transaction_id": "T1", "r2_triggered": False,
                                "related_alert_ids": '["A1"]'}])
        result = compare_records(expected, actual, "transaction_id", ["r2_triggered", "related_alert_ids"])
        self.assertEqual(result["duplicate_count"], 1)
        self.assertEqual(result["mismatch_count"], 1)
        self.assertIn("r2_triggered", result["mismatches"][0]["fields"])

    def test_normalizes_ground_truth_numeric_account_ids(self):
        expected = pd.DataFrame([{"alert_id": "A1", "source_account": 1234567890,
                                  "destination_account": 9876543210.0}])
        actual = pd.DataFrame([{"alert_id": "A1", "source_account": "1234567890",
                                "destination_account": "9876543210"}])
        result = compare_records(expected, actual, "alert_id", ["source_account", "destination_account"])
        self.assertEqual(result["mismatch_count"], 0)


if __name__ == "__main__":
    unittest.main()
