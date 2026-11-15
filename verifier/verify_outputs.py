"""Compare exported Iceberg outputs with the supplied ground-truth CSVs."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def norm(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def account_value(value: Any) -> str | None:
    """CSV inference may turn account IDs into integers/floats; compare them as identifiers."""
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
        if number.is_integer():
            return str(int(number))
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def id_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return sorted(str(x) for x in value)
    if value is None or pd.isna(value) or str(value).strip() in {"", "[]", "null", "None"}:
        return []
    try:
        return sorted(str(x) for x in json.loads(value))
    except (json.JSONDecodeError, TypeError):
        return sorted(x.strip() for x in str(value).split(",") if x.strip())


DECISION_FIELDS = ["source_account", "destination_account", "event_timestamp", "amount", "currency",
                   "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered",
                   "r5_risk_score", "r5_block_triggered", "decision", "related_alert_ids"]
ALERT_FIELDS = ["rule_id", "source_account", "destination_account", "trigger_transaction_id",
                "finding_scope", "window_start", "window_end", "observed_count", "observed_amount_inr",
                "count_threshold", "amount_threshold_inr", "supporting_transaction_ids", "explanation"]


def compare_records(expected: pd.DataFrame, actual: pd.DataFrame, key: str,
                    fields: list[str], decision: bool = False) -> dict[str, Any]:
    expected = expected.copy()
    actual = actual.copy()
    expected[key] = expected[key].astype(str)
    actual[key] = actual[key].astype(str)
    duplicates = sorted(actual.loc[actual[key].duplicated(keep=False), key].unique().tolist())
    exp_ids, act_ids = set(expected[key]), set(actual[key])
    missing = sorted(exp_ids - act_ids)
    unexpected = sorted(act_ids - exp_ids)
    exp = expected.drop_duplicates(key, keep="first").set_index(key)
    act = actual.drop_duplicates(key, keep="first").set_index(key)
    mismatches = []
    for record_id in sorted(exp_ids & act_ids):
        differences = {}
        for field in fields:
            if field not in exp.columns or field not in act.columns:
                differences[field] = {"expected": "<column missing>", "actual": "<column missing>"}
                continue
            ev, av = exp.at[record_id, field], act.at[record_id, field]
            if field.endswith("_triggered") or field.startswith("r") and field.endswith("_triggered"):
                ev, av = bool_value(ev), bool_value(av)
            elif field in {"related_alert_ids", "supporting_transaction_ids"}:
                ev, av = id_list(ev), id_list(av)
            elif field in {"source_account", "destination_account"}:
                ev, av = account_value(ev), account_value(av)
            else:
                ev, av = norm(ev), norm(av)
                if field in {"amount", "observed_amount_inr", "amount_threshold_inr"} and ev is not None and av is not None:
                    try:
                        ev, av = round(float(ev), 2), round(float(av), 2)
                    except (TypeError, ValueError):
                        pass
            if ev != av:
                differences[field] = {"expected": ev, "actual": av}
        if differences:
            mismatches.append({key: record_id, "fields": differences})
    return {"expected_count": int(len(expected)), "actual_count": int(len(actual)),
            "missing_count": len(missing), "missing_ids": missing,
            "unexpected_count": len(unexpected), "unexpected_ids": unexpected,
            "duplicate_count": len(duplicates), "duplicate_ids": duplicates,
            "mismatch_count": len(mismatches), "mismatches": mismatches}


def verify_frames(ad: pd.DataFrame, aa: pd.DataFrame, ed: pd.DataFrame,
                  ea: pd.DataFrame) -> dict[str, Any]:
    for frame in (ed, ad):
        if "related_alert_ids" in frame:
            frame["related_alert_ids"] = frame.related_alert_ids.map(id_list)
    for frame in (ea, aa):
        if "supporting_transaction_ids" in frame:
            frame["supporting_transaction_ids"] = frame.supporting_transaction_ids.map(id_list)
    decisions = compare_records(ed, ad, "transaction_id", DECISION_FIELDS, True)
    alerts = compare_records(ea, aa, "alert_id", ALERT_FIELDS)
    return {"status": "PASS" if not any((decisions[k] for k in ("missing_count", "unexpected_count", "duplicate_count", "mismatch_count")))
             and not any((alerts[k] for k in ("missing_count", "unexpected_count", "duplicate_count", "mismatch_count"))) else "FAIL",
            "decisions": decisions, "alerts": alerts}


def verify(actual_decisions: Path, actual_alerts: Path, expected_decisions: Path,
           expected_alerts: Path) -> dict[str, Any]:
    return verify_frames(pd.read_csv(actual_decisions), pd.read_csv(actual_alerts),
                         pd.read_csv(expected_decisions), pd.read_csv(expected_alerts))


def read_iceberg_tables(host: str, port: int, user: str, decisions_table: str,
                        alerts_table: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read actual Iceberg rows through Spark Thrift Server from VM3."""
    from pyhive import hive
    connection = hive.Connection(host=host, port=port, username=user, auth="NONE", database="default")
    try:
        def fetch(table: str) -> pd.DataFrame:
            cursor = connection.cursor()
            try:
                cursor.execute(f"SELECT * FROM {table}")
                names = [column[0].split(".")[-1] for column in cursor.description]
                return pd.DataFrame(cursor.fetchall(), columns=names)
            finally:
                cursor.close()
        return fetch(decisions_table), fetch(alerts_table)
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actual-decisions", type=Path, help="optional exported actual CSV (otherwise read Iceberg over Thrift)")
    parser.add_argument("--actual-alerts", type=Path, help="optional exported actual CSV (otherwise read Iceberg over Thrift)")
    parser.add_argument("--expected-decisions", type=Path, default=Path("data/ground_truth_transaction_decisions.csv"))
    parser.add_argument("--expected-alerts", type=Path, default=Path("data/ground_truth_account_risk_alerts.csv"))
    parser.add_argument("--report", type=Path, default=Path("reports/verification.json"))
    parser.add_argument("--thrift-host", default=__import__("os").getenv("SPARK_THRIFT_HOST", "vm4"))
    parser.add_argument("--thrift-port", type=int, default=int(__import__("os").getenv("SPARK_THRIFT_PORT", "10000")))
    parser.add_argument("--thrift-user", default=__import__("os").getenv("SPARK_THRIFT_USER", "verifier"))
    args = parser.parse_args()
    expected_d, expected_a = pd.read_csv(args.expected_decisions), pd.read_csv(args.expected_alerts)
    if args.actual_decisions and args.actual_alerts:
        actual_d, actual_a = pd.read_csv(args.actual_decisions), pd.read_csv(args.actual_alerts)
    elif args.actual_decisions or args.actual_alerts:
        parser.error("provide both actual CSV paths, or omit both to query Iceberg")
    else:
        import os
        catalog = os.getenv("ICEBERG_CATALOG", "lab")
        run_id = os.getenv("RUN_ID", "run1")
        decisions_table = os.getenv("TRANSACTION_TABLE", f"{catalog}.fraud.transaction_decisions_{run_id}")
        alerts_table = os.getenv("ALERT_TABLE", f"{catalog}.fraud.account_risk_alerts_{run_id}")
        actual_d, actual_a = read_iceberg_tables(args.thrift_host, args.thrift_port, args.thrift_user,
                                                  decisions_table, alerts_table)
    result = verify_frames(actual_d, actual_a, expected_d, expected_a)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"status": result["status"], "decisions": {k: result["decisions"][k] for k in ("expected_count", "actual_count", "missing_count", "unexpected_count", "duplicate_count", "mismatch_count")},
                      "alerts": {k: result["alerts"][k] for k in ("expected_count", "actual_count", "missing_count", "unexpected_count", "duplicate_count", "mismatch_count")}, "report": str(args.report)}, indent=2))
    raise SystemExit(0 if result["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
