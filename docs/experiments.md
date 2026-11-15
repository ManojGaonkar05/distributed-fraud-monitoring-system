# Failure and disorder experiments

Run the baseline first. For every experiment, save the run ID, configuration, verifier JSON, Spark UI screenshots, driver/executor logs, and a short conclusion in `reports/evidence/`. Never label a scenario passed unless it was executed on the four VMs.

All experiments should finish in under 20 minutes. Use a fresh run-specific topic, checkpoint, and pair of Iceberg tables except for the deliberate restart scenario.

## 1. Kill and restart an executor

With a stream running, identify an executor in Spark UI and terminate that worker/executor process. Keep the master and driver running; allow Spark to replace the executor and continue. Inspect driver logs, Spark UI task retries, HDFS checkpoint progress, and output counts. Export tables and run the verifier. Capture before/after Spark UI and verifier report.

## 2. Kill Spark Master and Driver, restart from the same checkpoint

Stop the master and driver while the producer continues or is paused at a recorded offset. Restart the master, then submit the same application with the same `RUN_ID`, Kafka topic, tables, and HDFS checkpoint. Verify recovered offsets/state in logs and UI, export, and run the verifier. Document any Kafka retention/offset or duplicate effects. Do not use a fresh checkpoint for this test.

## 3. Out-of-order events less than one minute

Use a fresh topic/run and launch the producer with `--disorder-seconds 30`. It reverses sequential blocks whose event-time span is 30-59 seconds; larger gaps pass through unchanged. Inspect watermark progress, late-event metrics, and output. Compare with ground truth using the verifier, then capture logs and report.

## 4. Out-of-order events more than one minute

Use a fresh topic/run and launch the producer with `--disorder-seconds 61`. It reverses sequential blocks whose event-time span is 61-120 seconds; larger gaps pass through unchanged. Inspect watermark and dropped-late-row metrics. Affected windows may differ from the on-time ground truth; report the exact missing/changed decisions and alerts. Run the verifier and preserve the FAIL report as useful evidence; do not modify the expected data to make it pass.
