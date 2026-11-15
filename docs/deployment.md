# Four VM deployment guide

Ubuntu 22.04/24.04 is assumed. Pin matching Java and Spark versions on all Spark nodes. The reference `scripts/submit_stream.sh` uses Spark 3.5.3, Scala 2.12, Iceberg 1.6.1, and the matching Spark Kafka connector. Install Python 3.10+ and `requirements.txt` on VM1, VM2, VM3, and VM4; Spark workers need pandas and PyArrow for the keyed state function. Open only lab network ports between named hosts; set addresses in `.env`, not in source.

## VM1: Kafka and producer

1. Install/start Kafka and configure its advertised listener to the VM1 address reachable from VM2.
2. Copy `config/example.env` to `.env`, set `KAFKA_BOOTSTRAP_SERVERS`, install `requirements.txt`, and securely copy `transactions.csv` into `data/transactions.csv`.
3. Run `bash scripts/start_kafka.sh` in one terminal. In another, run `bash scripts/create_topic.sh`, then `bash scripts/start_producer.sh`. CSV order is preserved by default. Producer pacing and bounded disorder options are described with `python -m producer.transaction_producer --help`.

## VM2: Spark master, driver, application

1. Copy the repository to VM2 and set `.env` with the VM hostnames, run ID, and thresholds. Securely copy `transactions.csv` into `data/transactions.csv` for producer startup.
2. Install Java and Spark 3.5.3, configure the master at `SPARK_MASTER`, and ensure VM3/VM4 workers connect to it.
3. Confirm Kafka is reachable and the Iceberg/HDFS endpoints resolve.
4. Run `bash scripts/submit_stream.sh`. It submits `streaming/fraud_stream.py` from VM2 and places its checkpoint in HDFS.

## VM3: worker, HDFS, Iceberg, verifier

1. Install/start one HDFS NameNode and DataNode on VM3. This single-node HDFS is intentional for this project.
2. Create the HDFS warehouse/checkpoint directories with `bash scripts/setup_hdfs.sh` (run as the HDFS owner).
3. Start a Spark worker against VM2's master. Configure the same Iceberg catalog and warehouse as VM2.
4. Copy only `verifier/` and the two ground-truth CSVs to VM3, install Python requirements, set `SPARK_THRIFT_HOST=vm4` and the same run/table settings, and run the verifier. It reads both Iceberg tables through Spark Thrift Server; CSV paths can be passed for offline comparison. Keep the streaming application source on VM2. Example:

```bash
python -m verifier.verify_outputs \
  --actual-decisions exports/transaction_decisions.csv \
  --actual-alerts exports/account_risk_alerts.csv \
  --expected-decisions data/ground_truth_transaction_decisions.csv \
  --expected-alerts data/ground_truth_account_risk_alerts.csv \
  --report reports/run1.json
```

The verifier exits 0 on PASS and 1 on FAIL. It queries Iceberg through Spark Thrift Server. For an offline run, provide both `--actual-decisions` and `--actual-alerts` CSV paths.

## VM4: worker and dashboard

1. Start a Spark worker against VM2's master.
2. Start Spark Thrift Server configured with the same Iceberg catalog and warehouse, listening on a VM4 address reachable by Streamlit.
3. Copy only `dashboard/` to VM4, install Python requirements, set the thrift host/port in `.env`, and run `bash scripts/start_dashboard.sh`.

## Baseline run

Use a fresh topic/table/checkpoint name such as `run1`. Run the full CSV once with `bash scripts/start_producer.sh --once`, let event-time windows drain, then run the verifier. Do not reuse a checkpoint for a new run. Use the same topic, tables, and checkpoint only for an intentional restart experiment.
