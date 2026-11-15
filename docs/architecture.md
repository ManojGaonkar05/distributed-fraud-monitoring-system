# Architecture, schemas, and implementation notes

## Responsibilities

| VM | Services |
| --- | --- |
| VM1 | Kafka broker, topic creation, Python CSV producer |
| VM2 | Kafka/Spark application submission, Spark master and driver; the only VM with this application's source code |
| VM3 | Spark worker, single-node HDFS, Iceberg warehouse, ground-truth verifier |
| VM4 | Spark worker and Streamlit dashboard |

VM2 runs the only application code. VM3 and VM4 receive executor tasks from Spark; they do not host a second copy of the streaming application. Dashboard access to Iceberg is through the Spark Thrift Server SQL endpoint.

## Rule behavior

- R1: one INR payment >= ₹50,000.
- R2: >= 5 unique payments by a source account in any 120-second window starting on a 30-second boundary.
- R3: >= 2 INR payments and sum > ₹600,000 in such a window. The PDF's extracted text omits the amount. ₹600,000 is supported by the `amount_threshold_inr` values in all supplied R3 ground-truth alerts; it is configurable.
- R4: >= 3 unique payments from a source account to the same destination in any such window.
- R5: number of active rules R1-R4 is the risk score; >= 2 is BLOCK, 1 is REVIEW, otherwise APPROVE.

The evaluated rule flags for a transaction are the union of its qualifying overlapping windows for its source account. This matches the supplied baseline: a transaction in a qualifying R4 account/window can have R4 active even when another transaction in that window is the one sent to the flagged destination; the alert's `supporting_transaction_ids` still lists only payments to that destination. The PDF requires one alert per account/window/rule but does not define how to represent multiple qualifying R4 destinations in one window. The supplied fixture selects the destination with the largest support count; this implementation does that and uses first-seen order to break ties. R2-R4 alert rows are emitted once per account/window/rule. Alert identifiers are deterministic and use UTC window-start times.

## Event-time/state design

The Kafka input is parsed and validated in a first Spark query, then deduplicated by transaction ID for the lifetime of the run and published to a run-specific intermediate Kafka topic. A second Spark query reads that topic, applies a one-minute event-time watermark, and uses one keyed stateful Pandas operation per source account. The separate queries are required because Spark 3.5 does not support chaining deduplication and arbitrary stateful processing in one Append-mode query. The deduplication ID set and per-account `seen_ids` are checkpointed and intentionally grow with unique IDs; this guarantees replays do not get counted twice, even after more than one minute. Account event data is retained at least 210 seconds behind the watermark so the oldest overlapping window support remains available. The stateful operation evaluates all 30-second-aligned windows and finalizes a transaction after every 120-second window containing it is behind the watermark. For a finite `--once` baseline, the producer sends a control watermark record five event-time minutes beyond the final CSV row; the scorer uses it only to advance the watermark and never writes it as a transaction. Continuous real streams advance by their normal event times.

The state is checkpointed to HDFS. State timeout/cleanup follows Spark watermark progress, so a quiet input may require a subsequent event or batch before the final state is flushed. A late event older than the watermark is discarded by Spark by design. As with all watermark-based systems, the one-minute guarantee is relative to the maximum event time observed by the query, not wall clock.

The Kafka sink between the two queries is at-least-once, so the scorer also tracks seen IDs in its own checkpointed account state. Rows from the stateful stream are tagged as decision or alert and split in `foreachBatch`; Iceberg `MERGE` makes each table append idempotent by its primary identifier. The two Iceberg merges are not a cross-table transaction. Keep a unique `RUN_ID`, source and intermediate Kafka topics, checkpoints, and output tables per run. This is a college experiment, not a payment authorization system.

## Table schemas

The PDF appendix fixes column names and meanings but not SQL types. This project uses these explicit types:

### `transaction_decisions`

`transaction_id STRING`, `source_account STRING`, `destination_account STRING`, `event_timestamp TIMESTAMP`, `evaluated_at TIMESTAMP`, `amount DECIMAL(18,2)`, `currency STRING`, `r1_triggered BOOLEAN`, `r2_triggered BOOLEAN`, `r3_triggered BOOLEAN`, `r4_triggered BOOLEAN`, `r5_risk_score INT`, `r5_block_triggered BOOLEAN`, `decision STRING`, `related_alert_ids ARRAY<STRING>`.

### `account_risk_alerts`

`alert_id STRING`, `rule_id STRING`, `source_account STRING`, `destination_account STRING`, `trigger_transaction_id STRING`, `finding_scope STRING`, `window_start TIMESTAMP`, `window_end TIMESTAMP`, `observed_count INT`, `observed_amount_inr DECIMAL(18,2)`, `count_threshold INT`, `amount_threshold_inr DECIMAL(18,2)`, `supporting_transaction_ids ARRAY<STRING>`, `explanation STRING`, `triggered_at TIMESTAMP`.

These choices preserve numeric accuracy for INR amounts and represent related/supporting IDs as arrays. `destination_account`, amount measurements, and thresholds are nullable where they do not apply (for example R2's amount threshold).
