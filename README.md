# Distributed Fraud Monitoring System

A four VM reference implementation of the Big Data 2026 fraud monitoring project. It reads Kafka events with PySpark Structured Streaming, evaluates event-time rules, and appends results to Apache Iceberg tables on HDFS.

## Project status

The source, deployment scripts, dashboard, and verifier are implemented in this repository. The services cannot be exercised here because the four Ubuntu VMs, Kafka, Spark, and HDFS are not connected to this workspace. Do not treat the deployment experiments as passed until you run them and capture evidence.

The specification PDF extraction supplied with the project omits the numeric R3 amount threshold. The supplied ground-truth alert rows consistently say ₹600,000; this implementation therefore uses ₹600,000 by default, configurable as `R3_AMOUNT_THRESHOLD_INR`. Confirm this value with the instructor if a corrected specification becomes available. The appendix supplies column meanings but not SQL types; the chosen explicit types are documented in `docs/architecture.md`.

## Quick links

- [Four VM deployment](docs/deployment.md)
- [Architecture and assumptions](docs/architecture.md)
- [Stress experiments and evidence](docs/experiments.md)
- [Environment configuration](config/example.env)

## Baseline inputs

The supplied inputs contain 2,000 transaction rows but only 1,990 unique transaction IDs (10 repeated rows); ground truth has 1,990 unique decisions and 129 alerts. Transaction and ground-truth CSVs are intentionally kept out of GitHub; securely copy the supplied files into `data/` on the VM that needs each file. The producer preserves CSV order and the streaming application deduplicates by `transaction_id`.

## Local development

Install Python dependencies with `python -m pip install -r requirements.txt`. Run the verifier against the Iceberg tables through Spark Thrift Server; optional CSV exports are supported for offline comparison. Full end-to-end validation requires the four VMs.
