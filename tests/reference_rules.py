"""Small pandas baseline oracle for checking rule/window semantics against supplied GT."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
R3_AMOUNT_THRESHOLD_INR = 600_000


def evaluate(transactions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate the ordered/on-time fixture, deduplicating on transaction_id."""
    tx = transactions.drop_duplicates("transaction_id", keep="first").copy()
    tx["event_timestamp"] = pd.to_datetime(tx.event_timestamp, utc=True)
    tx["_epoch"] = tx.event_timestamp.astype("int64") / 1e9
    all_alerts: list[dict] = []
    active_windows: dict[str, list[tuple[str, list[str]]]] = {}
    for account, group in tx.groupby("source_account", sort=False):
        group = group.sort_values(["_epoch", "transaction_id"])
        starts = sorted({int(t // 30) * 30 - d for t in group._epoch for d in (90, 60, 30, 0)})
        for start in starts:
            members = group[(group._epoch >= start) & (group._epoch < start + 120)]
            if members.empty:
                continue
            stamp = datetime.fromtimestamp(start, timezone.utc)
            stamp_id = stamp.strftime("%Y%m%d%H%M%S")
            def add(rule, destination, support, scope, count_threshold=None, amount=None, amount_threshold=None, explanation=""):
                alert_id = f"{rule}-{account}-{stamp_id}"
                ids = support.transaction_id.astype(str).tolist()
                all_alerts.append({"alert_id": alert_id, "rule_id": rule, "source_account": str(account),
                    "destination_account": str(destination) if destination is not None else None,
                    "trigger_transaction_id": ids[-1], "finding_scope": scope,
                    "window_start": stamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": datetime.fromtimestamp(start + 120, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "observed_count": len(support), "observed_amount_inr": amount,
                    "count_threshold": count_threshold, "amount_threshold_inr": amount_threshold,
                    "supporting_transaction_ids": json.dumps(ids), "explanation": explanation})
                active_windows.setdefault(rule, []).append((alert_id, ids))
            if len(members) >= 5:
                add("R2", None, members, "WINDOW", 5,
                    explanation=f"Account made {len(members)} transactions in a 120s window.")
            inr = members[members.currency.str.upper() == "INR"]
            total = inr.amount.astype(float).sum()
            if len(inr) >= 2 and total > R3_AMOUNT_THRESHOLD_INR:
                add("R3", None, inr, "WINDOW", 2, total, R3_AMOUNT_THRESHOLD_INR,
                    f"Account made {len(inr)} INR transactions totalling INR {total:.2f} in a 120s window.")
            destinations = [(dest, support) for dest, support in members.groupby("destination_account", sort=False) if len(support) >= 3]
            if destinations:
                dest, support = max(destinations, key=lambda pair: len(pair[1]))
                add("R4", dest, support, "PATTERN", 3,
                    explanation=f"Account sent {len(support)} transactions to one destination in a 120s window.")
    alerts = pd.DataFrame(all_alerts).drop_duplicates("alert_id", keep="last")
    alert_windows = {row.alert_id: (pd.Timestamp(row.window_start, tz="UTC").timestamp(),
                                    pd.Timestamp(row.window_end, tz="UTC").timestamp()) for row in alerts.itertuples()}
    decisions = []
    for _, row in tx.sort_values(["_epoch", "transaction_id"]).iterrows():
        related = sorted(aid for aid, (start, end) in alert_windows.items()
                         if start <= row["_epoch"] < end and
                         str(alerts.loc[alerts.alert_id == aid, "source_account"].iloc[0]) == str(row.source_account))
        rule_ids = {alerts.loc[alerts.alert_id == aid, "rule_id"].iloc[0] for aid in related}
        flags = {"r1_triggered": str(row.currency).upper() == "INR" and float(row.amount) >= 50_000,
                 "r2_triggered": "R2" in rule_ids, "r3_triggered": "R3" in rule_ids, "r4_triggered": "R4" in rule_ids}
        score = sum(flags.values())
        decisions.append({"transaction_id": row.transaction_id, "r1_triggered": flags["r1_triggered"],
            "r2_triggered": flags["r2_triggered"], "r3_triggered": flags["r3_triggered"],
            "r4_triggered": flags["r4_triggered"], "r5_risk_score": score,
            "r5_block_triggered": score >= 2,
            "decision": "BLOCK" if score >= 2 else "REVIEW" if score == 1 else "APPROVE",
            "related_alert_ids": json.dumps(related)})
    return pd.DataFrame(decisions), alerts


if __name__ == "__main__":
    required = [ROOT / "data/transactions.csv", ROOT / "data/ground_truth_transaction_decisions.csv",
                ROOT / "data/ground_truth_account_risk_alerts.csv"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Stage the supplied CSV files first: " + ", ".join(missing))
    tx = pd.read_csv(ROOT / "data/transactions.csv")
    expected_d = pd.read_csv(ROOT / "data/ground_truth_transaction_decisions.csv")
    expected_a = pd.read_csv(ROOT / "data/ground_truth_account_risk_alerts.csv")
    actual_d, actual_a = evaluate(tx)
    assert len(actual_d) == len(expected_d) == 1990, "decision count does not match the fixture"
    assert len(actual_a) == len(expected_a) == 129, "alert count does not match the fixture"
    assert actual_d.transaction_id.is_unique, "reference evaluator duplicated a transaction decision"
    assert actual_a.alert_id.is_unique, "reference evaluator duplicated an alert ID"
    assert set(actual_a.alert_id) == set(expected_a.alert_id), "alert IDs differ from ground truth"
    actual_alerts = actual_a.set_index("alert_id")
    expected_alerts = expected_a.set_index("alert_id")
    for alert_id in expected_alerts.index:
        assert actual_alerts.at[alert_id, "trigger_transaction_id"] == expected_alerts.at[alert_id, "trigger_transaction_id"], alert_id
        assert json.loads(actual_alerts.at[alert_id, "supporting_transaction_ids"]) == json.loads(expected_alerts.at[alert_id, "supporting_transaction_ids"]), alert_id
    expected_rules = expected_d.set_index("transaction_id")
    actual_rules = actual_d.set_index("transaction_id")
    flags = ["r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered", "r5_risk_score", "r5_block_triggered", "decision"]
    mismatch = (expected_rules[flags].astype(str) != actual_rules[flags].astype(str)).any(axis=1)
    assert not mismatch.any(), f"{int(mismatch.sum())} transaction rule outcomes differ"
    expected_links = expected_d.set_index("transaction_id").related_alert_ids.map(json.loads)
    actual_links = actual_d.set_index("transaction_id").related_alert_ids.map(json.loads)
    assert all(sorted(expected_links.loc[key]) == sorted(actual_links.loc[key]) for key in expected_links.index), "related alert links differ"
    print("PASS: 1,990 unique transaction outcomes and 129 alert IDs/support sets match ground truth.")
