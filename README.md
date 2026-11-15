# Distributed Fraud Monitoring System

A four-VM Big Data project for real-time transaction fraud monitoring using Apache Kafka, PySpark Structured Streaming, HDFS, Apache Iceberg, Spark SQL and Streamlit.

This README is the team setup and execution guide. It is aligned with the scripts and instructions currently in this repository.

> **Important:** Check installed software and versions before installing anything. Do not reinstall working dependencies. The code and scripts are implemented, but the complete four-VM deployment and evaluation experiments must still be run on the team's machines. Do not claim an experiment passed until you have run it and saved evidence.

## 1. Architecture and VM responsibilities

| VM | Responsibility | Components |
|---|---|---|
| VM1 | Kafka and data producer | Kafka broker, topic creation, Python CSV producer |
| VM2 | Spark master and application | Spark Master, Spark Driver, main streaming application |
| VM3 | Storage and verification | Single-node HDFS (NameNode + DataNode), Spark Worker, ground-truth verifier |
| VM4 | Dashboard and query access | Spark Worker, Spark Thrift Server, Streamlit dashboard |

### Data flow

1. VM1 publishes transaction records to Kafka.
2. VM2 reads Kafka data and runs the fraud-monitoring streaming application.
3. The application evaluates event-time fraud rules and writes decisions and alerts to Iceberg tables on HDFS.
4. HDFS on VM3 stores the Iceberg warehouse and Spark checkpoints.
5. VM4's dashboard reads the Iceberg tables through Spark Thrift Server.
6. VM3's verifier compares the results with the supplied ground-truth CSV files through Spark Thrift Server.

**Important architecture notes**
- Submit the main application `streaming/fraud_stream.py` from VM2 only.
- VM3 and VM4 run Spark Workers; they do not submit another copy of the main streaming application.
- HDFS is a single-node HDFS service hosted on VM3 for this project. It is not a multi-node HDFS cluster.
- For convenience, teammates may clone the full repository on each VM. The deployment guide also describes copying only the folders and scripts needed on each VM.

## 2. Project status and known assumptions

- Producer, streaming application, deployment scripts, dashboard and verifier are present in this repository.
- The full four-VM pipeline has not been verified by this repository's development workspace. The team must perform the baseline run and evaluation experiments.
- The extracted project specification does not show the numeric R3 amount threshold. The supplied ground-truth R3 alert rows consistently use ₹600,000, so the implementation uses `R3_AMOUNT_THRESHOLD_INR=600000` by default. Confirm with the instructor if a corrected threshold is supplied.
- The specification appendix gives table column meanings but not SQL types. The implementation's explicit types are documented in [docs/architecture.md](docs/architecture.md).

## 3. Repository setup — all VMs

Ubuntu 22.04/24.04 is assumed. On each VM, install basic tools and clone the repository:

```bash
sudo apt update
sudo apt install -y git curl wget python3 python3-pip python3-venv netcat-openbsd
git clone https://github.com/ManojGaonkar05/distributed-fraud-monitoring-system.git
cd distributed-fraud-monitoring-system
```

If the repository is already cloned, use:

```bash
cd ~/distributed-fraud-monitoring-system
git pull
```

### Check what is already installed

Run on each VM:

```bash
cat /etc/os-release
hostname
hostname -I
free -h
df -h
java -version
python3 --version
python3 -m pip --version
```

Check Kafka (mainly VM1):

```bash
echo "$KAFKA_HOME"
command -v kafka-server-start.sh
command -v kafka-topics.sh
```

Check Hadoop (mainly VM3):

```bash
echo "$HADOOP_HOME"
command -v hadoop
command -v hdfs
command -v start-dfs.sh
```

Check Spark (VM2, VM3 and VM4):

```bash
echo "$SPARK_HOME"
command -v spark-submit
command -v spark-class
```

If a command is not found, search common installation locations before installing a second copy:

```bash
find /opt /usr/local "$HOME" -maxdepth 5 -type f \\
  \( -name spark-submit -o -name hdfs -o -name kafka-server-start.sh \) 2>/dev/null
```

### Python dependencies

The repository's `requirements.txt` contains the Python packages for the producer, dashboard and verifier. It does **not** install Apache Spark or PySpark. Spark must be installed separately on the Spark VMs. Spark workers also need pandas and PyArrow for the keyed state operation.

Check Python imports:

```bash
python3 - <<'PY'
modules = ["kafka", "pandas", "pyarrow", "streamlit", "dotenv", "pyhive"]
for name in modules:
    try:
        module = __import__(name)
        print("INSTALLED:", name, getattr(module, "__version__", ""))
    except Exception as error:
        print("MISSING:", name, error)
PY
```

If packages are missing, create a virtual environment and install them:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If they are already installed in the Python environment you will use, do not reinstall them. Activate the same environment whenever running the Python producer, dashboard or verifier.

## 4. Network and environment configuration

Find the IP address of each VM using `hostname -I` and share the addresses with the whole team.

| VM | Address |
|---|---|
| VM1 | `VM1_IP` |
| VM2 | `VM2_IP` |
| VM3 | `VM3_IP` |
| VM4 | `VM4_IP` |

Replace the placeholders below with the actual reachable IP addresses. Do not use `localhost` or `127.0.0.1` for services accessed from another VM.

### Main ports

| Service | Host | Default port |
|---|---|---:|
| Kafka broker | VM1 | 9092 |
| Spark Master | VM2 | 7077 |
| Spark Master UI | VM2 | 8080 |
| HDFS NameNode RPC | VM3 | 9000 (must match Hadoop config) |
| HDFS NameNode UI | VM3 | 9870 (if enabled) |
| Spark Thrift Server | VM4 | 10000 |
| Streamlit dashboard | VM4 | 8501 |
| Spark Driver | VM2 | 39001 |
| Spark Block Manager | VM2 | 39002 |
| Spark Worker UI | VM3 / VM4 | 8081 (default per worker) |

Open only the required ports on the private lab network/firewall.

### Create the local environment file

On each VM:

```bash
cd ~/distributed-fraud-monitoring-system
cp -n config/example.env .env
nano .env
```

Use the same run-specific topic, run ID, table names, checkpoint root and warehouse settings across the VMs participating in one run. Set the host addresses correctly. The key settings are:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=VM1_IP:9092
KAFKA_TOPIC=transactions-run1

SPARK_MASTER=spark://VM2_IP:7077
SPARK_DRIVER_HOST=VM2_IP
SPARK_DRIVER_PORT=39001
SPARK_BLOCKMANAGER_PORT=39002

HDFS_NAMENODE_URI=hdfs://VM3_IP:9000
HDFS_CHECKPOINT_ROOT=hdfs://VM3_IP:9000/checkpoints
ICEBERG_CATALOG=lab
ICEBERG_WAREHOUSE=hdfs://VM3_IP:9000/warehouse

SPARK_THRIFT_HOST=VM4_IP
SPARK_THRIFT_PORT=10000
SPARK_THRIFT_USER=fraud-user

RUN_ID=run1
TRANSACTION_TABLE=lab.fraud.transaction_decisions_run1
ALERT_TABLE=lab.fraud.account_risk_alerts_run1
R3_AMOUNT_THRESHOLD_INR=600000
```

Keep the other values from `config/example.env`. Replace every IP placeholder with a real address. The `.env` file is ignored by Git and must remain local.

**Note:** Not every shell script automatically reads `.env`. The topic, HDFS setup, streaming submission and Thrift Server scripts source it; the producer and dashboard load it through Python dotenv. For Spark Master and Worker startup, export the required variables in that terminal as shown below. Before running the verifier, source `.env` into the shell because the verifier reads its Thrift settings from environment variables.

## 5. VM1 — Kafka broker and producer

### 5.1 Check Kafka

```bash
echo "$KAFKA_HOME"
command -v kafka-server-start.sh
command -v kafka-topics.sh
find /opt /usr/local "$HOME" -maxdepth 4 -type f -name kafka-server-start.sh 2>/dev/null
```

If Kafka is already installed, set `KAFKA_HOME` to its real installation directory. Example only:

```bash
export KAFKA_HOME=/opt/kafka
export PATH="$KAFKA_HOME/bin:$PATH"
"$KAFKA_HOME/bin/kafka-server-start.sh" --version
```

Install Kafka only if it is missing. Use the Kafka version selected for the lab.

### 5.2 Configure broker networking

Edit:

```bash
nano "$KAFKA_HOME/config/server.properties"
```

Configure the broker listener and advertised listener so VM2 can reach Kafka at VM1's actual IP address. Do not advertise `localhost` or `127.0.0.1` to remote clients. Preserve the other settings required by the installed Kafka version.

### 5.3 Start Kafka

Terminal 1 on VM1:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export KAFKA_HOME=/your/actual/kafka/path
export PATH="$KAFKA_HOME/bin:$PATH"
bash scripts/start_kafka.sh
```

Keep this terminal running.

Terminal 2 on VM1 — create the topic:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export KAFKA_HOME=/your/actual/kafka/path
export PATH="$KAFKA_HOME/bin:$PATH"
bash scripts/create_topic.sh
kafka-topics.sh --bootstrap-server VM1_IP:9092 --list
```

### 5.4 Copy the input data and run producer

The input CSV is intentionally not committed to GitHub. Copy the supplied `transactions.csv` into:

```text
data/transactions.csv
```

Check it:

```bash
ls -lh data/transactions.csv
head -n 3 data/transactions.csv
```

Start the baseline producer **only after VM2's streaming application is running**:

```bash
bash scripts/start_producer.sh --once
```

The producer's default mode continuously replays the CSV. Use `--once` for the finite baseline run. See all supported options:

```bash
python -m producer.transaction_producer --help
```

## 6. VM2 — Spark Master, Driver and streaming application

### 6.1 Check Java and Spark

```bash
java -version
echo "$SPARK_HOME"
command -v spark-submit
find /opt /usr/local "$HOME" -maxdepth 4 -type f -name spark-submit 2>/dev/null
```

The submission script uses Spark 3.5.3, Scala 2.12, the matching Spark Kafka connector 3.5.3 and Iceberg runtime 1.6.1. Use compatible Spark installations on VM2, VM3 and VM4.

If Spark is already installed, set its real path:

```bash
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
spark-submit --version
```

Do not install another Spark copy if the existing version is compatible.

### 6.2 Start Spark Master

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER_HOST=VM2_IP
bash scripts/start_spark_master.sh
```

Keep the terminal running. Open the Master UI:

```text
http://VM2_IP:8080
```

### 6.3 Check connectivity

```bash
nc -vz VM1_IP 9092
nc -vz VM3_IP 9000
```

The HDFS port must match the actual `fs.defaultFS` configuration.

### 6.4 Submit the main streaming application

First ensure HDFS is running on VM3, Kafka is running on VM1, and the VM3 and VM4 workers have registered with the Spark Master.

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
bash scripts/submit_stream.sh
```

The script reads the local `.env`, downloads the configured Spark Kafka and Iceberg packages through Spark's package mechanism if needed, and submits `streaming/fraud_stream.py`. Keep the terminal running and save its logs.

## 7. VM3 — HDFS, Spark Worker and verifier

### 7.1 Check Hadoop and Spark

```bash
echo "$HADOOP_HOME"
echo "$SPARK_HOME"
command -v hadoop
command -v hdfs
command -v start-dfs.sh
command -v spark-submit
```

If installed, set the actual paths:

```bash
export HADOOP_HOME=/your/actual/hadoop/path
export SPARK_HOME=/your/actual/spark/path
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
hadoop version
hdfs version
spark-submit --version
```

### 7.2 Configure HDFS

Configure `core-site.xml` on VM3 and all HDFS clients (including VM2 and VM4) so `fs.defaultFS` points to the VM3 NameNode URI.

On VM3, configure the NameNode and DataNode storage directories in `hdfs-site.xml` and set `dfs.replication=1` for this single-node HDFS design.

**Safety warning:** Format a NameNode only for a confirmed brand-new HDFS namespace, before its first start, following the installed Hadoop version's administration procedure. Never format an existing namespace; doing so can destroy its metadata.

### 7.3 Start HDFS and create directories

```bash
cd ~/distributed-fraud-monitoring-system
export HADOOP_HOME=/your/actual/hadoop/path
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
bash scripts/start_hdfs.sh
jps
```

Confirm NameNode and DataNode are running. Then create the checkpoint and warehouse directories as the HDFS owner:

```bash
bash scripts/setup_hdfs.sh
hdfs dfs -ls /
hdfs dfs -ls /checkpoints
hdfs dfs -ls /warehouse
```

The setup script sources `.env` and creates the configured checkpoint and warehouse paths.

### 7.4 Start Spark Worker

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER=spark://VM2_IP:7077
bash scripts/start_spark_worker.sh
```

Confirm VM3 appears as an alive worker in the Spark Master UI at `http://VM2_IP:8080`.

### 7.5 Copy ground truth and run verifier

Copy these supplied files to VM3's `data/` directory:

- `ground_truth_transaction_decisions.csv`
- `ground_truth_account_risk_alerts.csv`

They are intentionally excluded from GitHub.

The verifier reads actual Iceberg tables through VM4's Spark Thrift Server. It does not automatically load the `.env` file, so source it into the shell before running:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
set -a
source .env
set +a

python -m verifier.verify_outputs \\
  --expected-decisions data/ground_truth_transaction_decisions.csv \\
  --expected-alerts data/ground_truth_account_risk_alerts.csv \\
  --report reports/run1.json
```

Exit code `0` means PASS; exit code `1` means FAIL. Preserve the JSON report. For an offline comparison against exported actual CSVs, provide **both** `--actual-decisions` and `--actual-alerts` options as documented in the verifier help.

## 8. VM4 — Spark Worker, Thrift Server and dashboard

### 8.1 Check Spark

```bash
echo "$SPARK_HOME"
command -v spark-submit
find /opt /usr/local "$HOME" -maxdepth 5 -type f -name start-thriftserver.sh 2>/dev/null
```

Set and verify the actual Spark installation:

```bash
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
spark-submit --version
```

### 8.2 Start Spark Worker

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER=spark://VM2_IP:7077
bash scripts/start_spark_worker.sh
```

Confirm VM4 appears as an alive worker in the Spark Master UI.

### 8.3 Start Spark Thrift Server

VM4 must have matching HDFS client configuration and the same Iceberg catalog and warehouse settings as the streaming application.

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
bash scripts/start_thrift_server.sh
```

The script sources `.env`. Check the Thrift port in another terminal:

```bash
ss -lntp | grep 10000
```

### 8.4 Start Streamlit dashboard

In another terminal on VM4:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
bash scripts/start_dashboard.sh
```

Open:

```text
http://VM4_IP:8501
```

The dashboard reads the run's Iceberg tables through Spark Thrift Server. Confirm that the table names and run ID in VM4's `.env` match the streaming application.

## 9. Baseline end-to-end execution

Use the following order. Do not start the producer before the streaming application is ready.

| Order | VM | Action |
|---:|---|---|
| 1 | VM3 | Start HDFS NameNode and DataNode; create warehouse/checkpoint directories |
| 2 | VM1 | Start Kafka broker and create the source topic |
| 3 | VM2 | Start Spark Master |
| 4 | VM3 | Start Spark Worker |
| 5 | VM4 | Start Spark Worker |
| 6 | VM4 | Start Spark Thrift Server and dashboard |
| 7 | VM2 | Submit the streaming application |
| 8 | VM1 | Run producer once with `--once` |
| 9 | VM3 | Wait for output to flush, then run verifier |
| 10 | VM4 | Show dashboard and investigation view |

The finite baseline producer sends a control watermark record beyond the final CSV event time so the event-time windows can be finalized. Wait for the Spark query and Iceberg writes to complete before verifying.

Check connectivity from VM2:

```bash
nc -vz VM1_IP 9092
nc -vz VM3_IP 9000
```

Check HDFS on VM3:

```bash
hdfs dfsadmin -report
hdfs dfs -ls -R /checkpoints
hdfs dfs -ls -R /warehouse
```

### Run isolation

For each new independent run, use a unique:
- `RUN_ID`
- source and intermediate Kafka topics
- HDFS checkpoint locations
- Iceberg decision and alert table names

Do not reuse a checkpoint for a new independent run. Reuse the same run resources and checkpoint only for the deliberate restart/recovery experiment.

## 10. Evaluation experiments

Read [docs/experiments.md](docs/experiments.md) before running these scenarios. The experiment guide requires a baseline first and asks teams to finish each experiment within 20 minutes.

For every experiment, save the run ID, configuration, verifier JSON, Spark UI screenshots, driver/executor logs and a short conclusion in `reports/evidence/`.

### Experiment 1 — Kill and restart an executor

- Keep the Spark Master and Driver running.
- Identify an executor in the Spark UI and terminate the worker/executor process as described in the experiment guide.
- Observe task retries and recovery.
- Inspect HDFS checkpoint progress and output counts.
- Export outputs and run the verifier.
- Capture before/after Spark UI and verifier report.

### Experiment 2 — Kill Spark Master and Driver

- Stop the Master and Driver while the producer continues or is paused at a recorded offset.
- Restart the Master.
- Resubmit the same application using the same `RUN_ID`, Kafka topic, Iceberg tables and HDFS checkpoint.
- Verify recovered offsets/state in logs and UI.
- Document Kafka retention/offset or duplicate effects.
- Do not use a fresh checkpoint for this restart test.

### Experiment 3 — Out-of-order events less than one minute

Use a fresh run/topic/checkpoint/table set. The producer's bounded disorder option is:

```bash
bash scripts/start_producer.sh --once --disorder-seconds 30
```

The producer reverses sequential blocks spanning 30–59 event-time seconds where the input permits it; larger gaps pass through unchanged. Inspect watermark progress, late-event metrics and output. Compare with ground truth using the verifier and save the report.

### Experiment 4 — Out-of-order events more than one minute

Use another fresh run/topic/checkpoint/table set:

```bash
bash scripts/start_producer.sh --once --disorder-seconds 61
```

The producer reverses sequential blocks spanning 61–120 event-time seconds where the input permits it; larger gaps pass through unchanged. Inspect watermark and dropped-late-row metrics. Affected windows may differ from on-time ground truth. Run the verifier and preserve the FAIL report as evidence; do not change expected data to force a PASS.

### Evidence checklist

For each experiment, preserve:
- Experiment name and date/time.
- Run ID, Kafka topics, Iceberg table names and HDFS checkpoint path.
- Commands used to inject failure or disorder.
- Terminal output before, during and after the experiment.
- Spark UI screenshots and driver/executor logs.
- Verifier JSON and a short conclusion.
- Evidence that previous-run Iceberg tables remain available.

Never label an experiment as passed unless it was actually executed on the four VMs.

## 11. Useful troubleshooting checks

```bash
# All VMs: Java processes
jps

# VM1: Kafka topics
kafka-topics.sh --bootstrap-server VM1_IP:9092 --list

# VM2: check Spark Master UI in browser
# http://VM2_IP:8080

# VM3: HDFS health and stored data
hdfs dfsadmin -report
hdfs dfs -ls -R /checkpoints
hdfs dfs -ls -R /warehouse

# VM4: Thrift Server and dashboard ports
ss -lntp | grep -E '10000|8501'
```

Common causes of failure:
- Wrong IP address or a service advertising `localhost` to remote VMs.
- Kafka broker not reachable from VM2.
- Spark versions do not match the versions expected by the submission script.
- VM3/VM4 workers cannot reach VM2's Spark Master.
- VM2/VM4 HDFS client configuration points to the wrong NameNode.
- HDFS warehouse/checkpoint permissions prevent Spark writes.
- Different run IDs, table names, topics or checkpoint paths in different VMs.
- Producer started before the streaming query.
- Missing input or ground-truth CSV files.
- Verifier launched without sourcing `.env` or without VM4's Thrift Server running.

Do not format HDFS or delete checkpoints as a generic troubleshooting step. First inspect logs and the current namespace/checkpoint state.

## 12. Useful project documentation

- [Four-VM deployment guide](docs/deployment.md)
- [Architecture, fraud rules and schemas](docs/architecture.md)
- [Failure and disorder experiments](docs/experiments.md)
- [Environment variable template](config/example.env)

## 13. Dataset notes

The supplied transaction input contains 2,000 rows and 1,990 unique transaction IDs (10 repeated rows). The ground truth contains 1,990 unique transaction decisions and 129 account-risk alerts. The application deduplicates by `transaction_id`. Keep the original CSV files unchanged and copy them securely to the VMs that need them.

---

**Repository:** https://github.com/ManojGaonkar05/distributed-fraud-monitoring-system


## 14. Copy-and-run command checklist for the team

This section puts the main commands in execution order. Run each block on the VM named in the heading. Replace every `VMx_IP` and `/your/actual/.../path` placeholder first.

### A. All VMs — update the repository and prepare Python

```bash
cd ~/distributed-fraud-monitoring-system
git pull
python3 --version
java -version
hostname -I
```

If the Python environment has not been created yet:

```bash
cd ~/distributed-fraud-monitoring-system
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If `.venv` already exists, activate it instead of creating it again:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
```

### B. VM1 — start Kafka and create topic

Terminal 1:

```bash
cd ~/distributed-fraud-monitoring-system
export KAFKA_HOME=/your/actual/kafka/path
export PATH="$KAFKA_HOME/bin:$PATH"
bash scripts/start_kafka.sh
```

Terminal 2:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export KAFKA_HOME=/your/actual/kafka/path
export PATH="$KAFKA_HOME/bin:$PATH"
bash scripts/create_topic.sh
kafka-topics.sh --bootstrap-server VM1_IP:9092 --list
```

After the main Spark query is running on VM2, start the one-pass baseline producer:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
bash scripts/start_producer.sh --once
```

### C. VM3 — start HDFS and create storage directories

```bash
cd ~/distributed-fraud-monitoring-system
export HADOOP_HOME=/your/actual/hadoop/path
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
bash scripts/start_hdfs.sh
jps
```

Only continue when NameNode and DataNode are running. Then:

```bash
cd ~/distributed-fraud-monitoring-system
bash scripts/setup_hdfs.sh
hdfs dfsadmin -report
hdfs dfs -ls /
hdfs dfs -ls /checkpoints
hdfs dfs -ls /warehouse
```

### D. VM2 — start Spark Master

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER_HOST=VM2_IP
bash scripts/start_spark_master.sh
```

Open `http://VM2_IP:8080` and leave the Master running.

### E. VM3 — start its Spark Worker

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER=spark://VM2_IP:7077
bash scripts/start_spark_worker.sh
```

### F. VM4 — start its Spark Worker

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export SPARK_MASTER=spark://VM2_IP:7077
bash scripts/start_spark_worker.sh
```

Check the Master UI on VM2 and confirm both workers are listed as ALIVE.

### G. VM4 — start Thrift Server and dashboard

Terminal 1:

```bash
cd ~/distributed-fraud-monitoring-system
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
bash scripts/start_thrift_server.sh
```

Terminal 2:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
bash scripts/start_dashboard.sh
```

Open `http://VM4_IP:8501`.

### H. VM2 — submit the streaming application

Run only after Kafka, HDFS, Spark Master and both Spark Workers are ready:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
export SPARK_HOME=/your/actual/spark/path
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
nc -vz VM1_IP 9092
nc -vz VM3_IP 9000
bash scripts/submit_stream.sh
```

Keep this terminal running. Then go to VM1 and run the producer command in section B.

### I. VM3 — verify the baseline output

After the finite producer has finished and the stream has flushed its event-time windows:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
set -a
source .env
set +a
python -m verifier.verify_outputs \
  --expected-decisions data/ground_truth_transaction_decisions.csv \
  --expected-alerts data/ground_truth_account_risk_alerts.csv \
  --report reports/run1.json
echo "Verifier exit code: $?"
```

Exit code 0 is PASS; exit code 1 is FAIL. Keep the report.

### J. Stop services safely after a run

Stop a foreground process with `Ctrl+C` in the terminal where it is running. For background services, use the matching stop scripts from the installed distributions:

VM1 Kafka:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

VM2 Spark Master:

```bash
"$SPARK_HOME/sbin/stop-master.sh"
```

VM3 and VM4 Spark Workers (run on each worker VM):

```bash
"$SPARK_HOME/sbin/stop-worker.sh"
```

VM3 HDFS (only after Spark jobs have stopped and the team has finished using HDFS):

```bash
"$HADOOP_HOME/sbin/stop-dfs.sh"
```

Do not stop services in the middle of a required experiment. For the Master/Driver recovery experiment, follow `docs/experiments.md` and preserve the same checkpoint rather than doing a normal clean shutdown.

### K. Evaluation disorder producer commands

Run each disorder experiment with a separate fresh run configuration/topic/checkpoint/table set. Start the streaming application for that run before starting the producer.

Less than one minute:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
bash scripts/start_producer.sh --once --disorder-seconds 30
```

More than one minute:

```bash
cd ~/distributed-fraud-monitoring-system
source .venv/bin/activate
bash scripts/start_producer.sh --once --disorder-seconds 61
```

For the restart recovery experiment only, reuse the same run ID, Kafka topics, Iceberg tables and HDFS checkpoint. Do not reuse those resources for a separate fresh experiment.

> **Installation note:** These project scripts start software that is already installed; they do not install Kafka, Hadoop or Spark. Their distribution/version and configuration must match the lab environment. The repository pins the Spark-side package coordinates in `scripts/submit_stream.sh`; do not guess a different Spark release or overwrite an existing working installation. If one of these platform tools is missing, first share the VM's OS/version and the output of the check commands in section 3 so the team can install the compatible distribution.
