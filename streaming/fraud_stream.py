"""Event-time fraud scoring for Kafka, with HDFS checkpoints and Iceberg outputs.

Decisions wait until all 120-second windows containing a transaction have closed
behind the one-minute watermark. Per-account state is bounded to three minutes.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T


TRANSACTION_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("transaction_id", T.StringType()),
    T.StructField("user_id", T.StringType()), T.StructField("customer_id", T.StringType()),
    T.StructField("source_account", T.StringType()), T.StructField("destination_account", T.StringType()),
    T.StructField("merchant_id", T.StringType()), T.StructField("device_id", T.StringType()),
    T.StructField("event_timestamp", T.StringType()), T.StructField("amount", T.StringType()),
    T.StructField("currency", T.StringType()), T.StructField("transaction_type", T.StringType()),
    T.StructField("location", T.StringType()), T.StructField("channel", T.StringType()),
    T.StructField("account_age_days", T.StringType()),
])
STATE_SCHEMA = T.StructType([T.StructField("record_type", T.StringType()), T.StructField("payload", T.StringType())])
GROUP_STATE_SCHEMA = T.StructType([T.StructField("state_json", T.StringType())])
R3_AMOUNT = float(os.getenv("R3_AMOUNT_THRESHOLD_INR", "600000"))
DECISION_FIELDS = ["transaction_id", "source_account", "destination_account", "event_timestamp", "evaluated_at",
                   "amount", "currency", "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered",
                   "r5_risk_score", "r5_block_triggered", "decision", "related_alert_ids"]
ALERT_FIELDS = ["alert_id", "rule_id", "source_account", "destination_account", "trigger_transaction_id",
                "finding_scope", "window_start", "window_end", "observed_count", "observed_amount_inr",
                "count_threshold", "amount_threshold_inr", "supporting_transaction_ids", "explanation", "triggered_at"]
DECISION_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType(), False), T.StructField("source_account", T.StringType()),
    T.StructField("destination_account", T.StringType()), T.StructField("event_timestamp", T.TimestampType()),
    T.StructField("evaluated_at", T.TimestampType()), T.StructField("amount", T.DecimalType(18, 2)),
    T.StructField("currency", T.StringType()), T.StructField("r1_triggered", T.BooleanType()),
    T.StructField("r2_triggered", T.BooleanType()), T.StructField("r3_triggered", T.BooleanType()),
    T.StructField("r4_triggered", T.BooleanType()), T.StructField("r5_risk_score", T.IntegerType()),
    T.StructField("r5_block_triggered", T.BooleanType()), T.StructField("decision", T.StringType()),
    T.StructField("related_alert_ids", T.ArrayType(T.StringType()))])
ALERT_SCHEMA = T.StructType([
    T.StructField("alert_id", T.StringType(), False), T.StructField("rule_id", T.StringType()),
    T.StructField("source_account", T.StringType()), T.StructField("destination_account", T.StringType()),
    T.StructField("trigger_transaction_id", T.StringType()), T.StructField("finding_scope", T.StringType()),
    T.StructField("window_start", T.TimestampType()), T.StructField("window_end", T.TimestampType()),
    T.StructField("observed_count", T.IntegerType()), T.StructField("observed_amount_inr", T.DecimalType(18, 2)),
    T.StructField("count_threshold", T.IntegerType()), T.StructField("amount_threshold_inr", T.DecimalType(18, 2)),
    T.StructField("supporting_transaction_ids", T.ArrayType(T.StringType())), T.StructField("explanation", T.StringType()),
    T.StructField("triggered_at", T.TimestampType())])


def parse_timestamp(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def windows_for(epoch: float) -> list[int]:
    """Return the four 120s window starts on 30s boundaries that contain event time."""
    latest = int(epoch // 30) * 30
    return [latest - 90, latest - 60, latest - 30, latest]


def event_time(row: dict) -> float:
    return parse_timestamp(row["event_timestamp"]).timestamp()


def iso_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def utc_now() -> str:
    """Return a wall-clock audit time; fraud rules themselves use event time only."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def stateful_scoring(key, batches, state):
    """Maintain short-lived account activity; emit each alert/decision at most once."""
    events = []
    emitted = set()
    if state.exists:
        try:
            saved = json.loads(state.get[0])
            events, emitted = saved.get("events", []), set(saved.get("emitted", []))
        except (ValueError, TypeError, IndexError):
            events, emitted = [], set()
    for pdf in batches:
        for row in pdf.to_dict(orient="records"):
            try:
                parsed = parse_timestamp(row["event_timestamp"])
                row["_epoch"] = parsed.timestamp()
                row["_amount"] = float(row["amount"])
                row["_event_iso"] = parsed.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
                # Spark supplies Timestamp/Decimal objects; store JSON-safe scalar values.
                row["event_timestamp"] = parsed.isoformat()
                row["amount"] = f"{row['_amount']:.2f}"
                if not any(old["transaction_id"] == row["transaction_id"] for old in events):
                    events.append(row)
            except (ValueError, TypeError, KeyError):
                continue
    events.sort(key=lambda r: (r["_epoch"], r["transaction_id"]))
    watermark_ms = state.getCurrentWatermarkMs()
    watermark_s = watermark_ms / 1000.0 if watermark_ms is not None and watermark_ms >= 0 else float("-inf")
    account = str(key[0])

    # Aggregate each sliding window exactly once. Windows are aligned to UTC 30s boundaries.
    generated: dict[str, dict] = {}
    all_windows = set(start for event in events for start in windows_for(event["_epoch"]))
    for start in sorted(all_windows):
        end = start + 120
        if end > watermark_s:
            continue
        members = [e for e in events if start <= e["_epoch"] < end]
        if not members:
            continue
        count = len(members)
        inr = [e for e in members if e.get("currency") == "INR"]
        patterns = {
            "R2": (members if count >= 5 else []),
            "R3": (inr if len(inr) >= 2 and sum(e["_amount"] for e in inr) > R3_AMOUNT else []),
        }
        for rule_id, support in patterns.items():
            if not support:
                continue
            aid = f"{rule_id}-{account}-{datetime.fromtimestamp(start, timezone.utc):%Y%m%d%H%M%S}"
            amount = sum(e["_amount"] for e in support) if rule_id == "R3" else None
            threshold_count = 5 if rule_id == "R2" else 2
            explanation = (f"Account made {len(support)} transactions in a 120s window." if rule_id == "R2" else
                           f"Account made {len(support)} INR transactions totalling INR {amount:.2f} in a 120s window.")
            generated[aid] = {"alert_id": aid, "rule_id": rule_id, "source_account": account,
                "destination_account": None, "trigger_transaction_id": support[-1]["transaction_id"],
                "finding_scope": "WINDOW", "window_start": iso_time(start), "window_end": iso_time(end),
                "observed_count": len(support), "observed_amount_inr": amount, "count_threshold": threshold_count,
                "amount_threshold_inr": R3_AMOUNT if rule_id == "R3" else None,
                "supporting_transaction_ids": [e["transaction_id"] for e in support], "explanation": explanation,
                "triggered_at": utc_now()}
        by_dest: dict[str, list[dict]] = {}
        for item in members:
            by_dest.setdefault(str(item.get("destination_account", "")), []).append(item)
        qualifying_destinations = [(dest, support) for dest, support in by_dest.items() if len(support) >= 3]
        # The appendix defines one alert per account/window/rule. If several destinations
        # qualify, choose the one with the most support; stable insertion order breaks ties.
        if qualifying_destinations:
            dest, support = max(qualifying_destinations, key=lambda pair: len(pair[1]))
            aid = f"R4-{account}-{datetime.fromtimestamp(start, timezone.utc):%Y%m%d%H%M%S}"
            generated[aid] = {"alert_id": aid, "rule_id": "R4", "source_account": account,
                "destination_account": dest, "trigger_transaction_id": support[-1]["transaction_id"],
                "finding_scope": "PATTERN", "window_start": iso_time(start), "window_end": iso_time(end),
                "observed_count": len(support), "observed_amount_inr": None, "count_threshold": 3,
                "amount_threshold_inr": None, "supporting_transaction_ids": [e["transaction_id"] for e in support],
                "explanation": f"Account sent {len(support)} transactions to one destination in a 120s window.", "triggered_at": utc_now()}

    output = []
    for aid, alert in generated.items():
        if aid not in emitted:
            emitted.add(aid)
            output.append(("alert", json.dumps(alert, separators=(",", ":"))))

    kept = []
    for transaction in events:
        # Event-time timer is the latest end among all windows that contain this event.
        due = max(windows_for(transaction["_epoch"])) + 120
        if watermark_s < due or transaction.get("_decision_emitted", False):
            continue
        member_alerts = []
        flags = {"R1": transaction["currency"] == "INR" and transaction["_amount"] >= 50000,
                 "R2": False, "R3": False, "R4": False}
        for aid, alert in generated.items():
            alert_start = datetime.strptime(alert["window_start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            alert_end = alert_start + 120
            if alert_start <= transaction["_epoch"] < alert_end:
                member_alerts.append(aid)
                flags[alert["rule_id"]] = True
        score = sum(flags.values())
        decision = {"transaction_id": transaction["transaction_id"], "source_account": account,
            "destination_account": transaction["destination_account"], "event_timestamp": transaction["_event_iso"],
            "evaluated_at": utc_now(), "amount": f"{transaction['_amount']:.2f}",
            "currency": transaction["currency"], "r1_triggered": flags["R1"], "r2_triggered": flags["R2"],
            "r3_triggered": flags["R3"], "r4_triggered": flags["R4"], "r5_risk_score": score,
            "r5_block_triggered": score >= 2, "decision": "BLOCK" if score >= 2 else "REVIEW" if score == 1 else "APPROVE",
            "related_alert_ids": sorted(member_alerts)}
        output.append(("decision", json.dumps(decision, separators=(",", ":"))))
        transaction["_decision_emitted"] = True

    # Retain 3 minutes behind watermark so the oldest overlapping windows remain computable.
    cutoff = watermark_s - 180 if watermark_s != float("-inf") else float("-inf")
    kept = [r for r in events if r["_epoch"] >= cutoff]
    # Keep alert keys only while their windows could still affect a not-yet-final decision.
    still_relevant = {aid for aid in emitted if any(alert.get("alert_id") == aid and
                      int(datetime.strptime(alert["window_start"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()) + 120 >= cutoff
                      for alert in generated.values())}
    emitted = still_relevant | set(generated)
    if kept:
        pending = [r for r in kept if not r.get("_decision_emitted", False)]
        next_timeout = int(min((max(windows_for(r["_epoch"])) + 120 for r in pending), default=watermark_s + 60) * 1000)
        state.update((json.dumps({"events": kept, "emitted": sorted(emitted)}, separators=(",", ":")),))
        timeout_at = datetime.fromtimestamp(next_timeout / 1000.0, timezone.utc).replace(tzinfo=None)
        state.setTimeoutTimestamp(timeout_at)
    else:
        state.remove()
    yield pd.DataFrame(output, columns=["record_type", "payload"])


def main() -> None:
    run_id = os.getenv("RUN_ID", "run1")
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]
    checkpoint_root = os.environ["HDFS_CHECKPOINT_ROOT"].rstrip("/")
    catalog = os.getenv("ICEBERG_CATALOG", "lab")
    decisions_table = os.getenv("TRANSACTION_TABLE", f"{catalog}.fraud.transaction_decisions_{run_id}")
    alerts_table = os.getenv("ALERT_TABLE", f"{catalog}.fraud.account_risk_alerts_{run_id}")
    spark = SparkSession.builder.appName(os.getenv("SPARK_APP_NAME", f"fraud-monitor-{run_id}")).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.fraud")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {decisions_table} (
        transaction_id STRING, source_account STRING, destination_account STRING, event_timestamp TIMESTAMP,
        evaluated_at TIMESTAMP, amount DECIMAL(18,2), currency STRING, r1_triggered BOOLEAN, r2_triggered BOOLEAN,
        r3_triggered BOOLEAN, r4_triggered BOOLEAN, r5_risk_score INT, r5_block_triggered BOOLEAN,
        decision STRING, related_alert_ids ARRAY<STRING>) USING iceberg""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {alerts_table} (
        alert_id STRING, rule_id STRING, source_account STRING, destination_account STRING, trigger_transaction_id STRING,
        finding_scope STRING, window_start TIMESTAMP, window_end TIMESTAMP, observed_count INT,
        observed_amount_inr DECIMAL(18,2), count_threshold INT, amount_threshold_inr DECIMAL(18,2),
        supporting_transaction_ids ARRAY<STRING>, explanation STRING, triggered_at TIMESTAMP) USING iceberg""")

    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic).option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false").load())
    parsed = raw.select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("t")).select("t.*")
    valid = parsed.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
        .filter(F.col("transaction_id").isNotNull() & F.col("source_account").isNotNull() &
                F.col("destination_account").isNotNull() & F.col("event_timestamp").isNotNull() &
                F.col("amount").isNotNull() & (F.col("amount") >= 0) & F.col("currency").isNotNull() &
                (F.upper("transaction_type") == "PAYMENT")) \
        .withColumn("currency", F.upper("currency"))
    # No watermark on this deduplicator: a replayed ID remains suppressed for the
    # entire run, even when a producer loops over the CSV after more than one minute.
    # The ID state is checkpointed and intentionally grows with unique run IDs.
    unique = valid.dropDuplicates(["transaction_id"]).withWatermark(
        "event_timestamp", os.getenv("WATERMARK_DELAY", "1 minute"))
    scored = unique.groupBy("source_account").applyInPandasWithState(
        stateful_scoring, outputStructType=STATE_SCHEMA, stateStructType=GROUP_STATE_SCHEMA,
        outputMode="Append", timeoutConf="EventTimeTimeout")

    def write_idempotently(batch, batch_id: int) -> None:
        decision_rows = batch.filter(F.col("record_type") == "decision").select(F.from_json("payload", DECISION_SCHEMA).alias("r")).select("r.*")
        alert_rows = batch.filter(F.col("record_type") == "alert").select(F.from_json("payload", ALERT_SCHEMA).alias("r")).select("r.*")
        if decision_rows.limit(1).count():
            decision_rows.createOrReplaceTempView("batch_decisions")
            spark.sql(f"MERGE INTO {decisions_table} t USING batch_decisions s ON t.transaction_id=s.transaction_id WHEN NOT MATCHED THEN INSERT *")
        if alert_rows.limit(1).count():
            alert_rows.createOrReplaceTempView("batch_alerts")
            spark.sql(f"MERGE INTO {alerts_table} t USING batch_alerts s ON t.alert_id=s.alert_id WHEN NOT MATCHED THEN INSERT *")

    query = scored.writeStream.outputMode("append").foreachBatch(write_idempotently) \
        .option("checkpointLocation", f"{checkpoint_root}/{run_id}").trigger(processingTime="30 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
