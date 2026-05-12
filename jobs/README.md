#  Spark Streaming Jobs — KafkaSparkArch

Real-time financial transaction processing using Apache Spark Structured Streaming. This layer sits between the Kafka brokers and downstream consumers, handling windowed aggregations and anomaly detection at high throughput.

---

## Architecture

```
Java Producer (TransactionProducer.java)
        │
        ▼
Kafka Topic: financial_transactions
  (16 partitions · replication factor 3)
        │
        ▼
Spark Structured Streaming (spark_processor.py)
        │
        ├──▶ Kafka Topic: transaction_aggregates
        │         (5-min windowed totals per merchant)
        │
        └──▶ Kafka Topic: transaction_anomalies
                  (transactions > $140,000)
```

**Two streaming queries run in parallel:**

| Query | Logic | Output Mode | Trigger |
|---|---|---|---|
| Aggregates | Windowed `SUM` + `COUNT` per merchant | `update` | every 10s |
| Anomalies | Filter `amount > 140,000` | `append` | every 10s |

---

## Performance (Observed)

Metrics captured from Spark UI (`localhost:4040`) during a live run:

| Metric | Value |
|---|---|
| Input Rate | ~300,000 records/sec |
| Process Rate | ~120,000–150,000 records/sec |
| Records per Batch | ~3,000,000 |
| Batch Duration | ~10,000–40,000 ms |
| Executors | 3 workers + 1 driver |
| Scheduling Mode | FIFO |

### Spark Jobs Timeline
![Spark Jobs](./Imgs/spark_jobs_active.png)

![Spark Jobs — Completed](./Imgs/spark_jobs_completed.png)

### Streaming Query Stats — Aggregates
![Aggregates Query](./Imgs/aggregates_query.png)

### Streaming Query Stats — Anomalies
![Anomalies Query](./Imgs/anomalies_query.png)
![Spark Jobs](./Imgs/spark_jobs_active.png)
![Spark Jobs Completed](./Imgs/spark_jobs_completed.png)

### Aggregates
![Aggregates](./Imgs/aggregates_query.png)

### Anomalies
![Anomalies](./Imgs/anomalies_query.png)
---

## Prerequisites

- Docker + Docker Compose
- The full KafkaSparkArch stack running (see root `docker-compose.yml`)
- Kafka brokers healthy on `kafka-broker-1:9092`, `kafka-broker-2:9092`, `kafka-broker-3:9092`
- Java producer running and publishing to `financial_transactions`

---

## How to Run

### 1. Start the stack
```bash
docker compose up -d
```

Wait ~30 seconds for brokers to be ready:
```bash
docker compose ps   # all services should show "Up"
```

### 2. Start the Java producer
```bash
./gradlew run
```

### 3. Submit the Spark job
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/jobs/spark_processor.py
```

### 4. Monitor
| UI | URL |
|---|---|
| Spark Master | http://localhost:9190 |
| Streaming Stats | http://localhost:4040 → Structured Streaming tab |
| Redpanda Console (topics) | http://localhost:8085 |

---

## Configuration

All config lives at the top of `jobs/spark_processor.py`:

```python
KAFKA_BROKERS   = 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092'
SOURCE_TOPIC    = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC  = 'transaction_anomalies'

CHECKPOINT_DIR  = '/mnt/spark-checkpoints'
STATES_DIR      = '/mnt/spark-state'
```

>  **Important:** Inside Docker, always use internal broker hostnames (`kafka-broker-N:9092`), not `localhost:29092`. The Java producer runs on the host machine so it uses `localhost` — Spark runs inside Docker so it doesn't.

### Spark Session Tuning

```python
.config("spark.sql.shuffle.partitions", 4)   # Default 200 kills local machines
.config("spark.executor.memory", "512m")
.config("spark.driver.memory", "512m")
```

> On a 16GB machine with the full stack running, keep `shuffle.partitions` at 4. The default of 200 creates massive overhead for local streaming.

### Aggregation Window

```python
.withWatermark("transactionTimestamp", "10 minutes")   # tolerate 10min late data
.groupBy(
    window(col("transactionTimestamp"), "5 minutes"),   # 5-min windows
    col("merchantId")
)
```

### Anomaly Threshold

```python
anomalies_df = transactions_df.filter(col("amount") > 140000)
```

Change the threshold here to tune sensitivity.

---

## Schema

Incoming JSON messages from Kafka:

```python
StructType([
    StructField('transactionId',   StringType()),
    StructField('userId',          StringType()),
    StructField('merchantId',      StringType()),
    StructField('amount',          DoubleType()),
    StructField('transactionTime', LongType()),    # epoch ms → converted to timestamp
    StructField('transactionType', StringType()),
    StructField('location',        StringType()),
    StructField('paymentMethod',   StringType()),
    StructField('isInternational', StringType()),  # "True"/"False" → converted to Boolean
    StructField('currency',        StringType()),
])
```

---

## Checkpointing

Checkpoints are stored in `./mnt/checkpoints/` (mounted into the container at `/mnt/spark-checkpoints`):

```
mnt/checkpoints/
├── aggregates/     ← offsets + commits for aggregation query
└── anomalies/      ← offsets + commits for anomaly query
```

If the job crashes and restarts, Spark automatically resumes from the last committed offset — **no data loss, no duplicates**.

To do a clean restart (wipe checkpoints):
```bash
rm -rf mnt/checkpoints/aggregates mnt/checkpoints/anomalies
```

---

## Troubleshooting

### Spark can't connect to Kafka
**Symptom:** `Connection to node -1 (localhost/127.0.0.1:29092) could not be established`

**Cause:** `KAFKA_BROKERS` is set to `localhost` — this is correct for the host machine but wrong inside Docker.

**Fix:** Make sure `spark_processor.py` uses internal hostnames:
```python
KAFKA_BROKERS = 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092'
```

---

### Machine is slow / RAM maxed out
**Fix 1:** Reduce Spark workers in `docker-compose.override.yml` to 1 worker with limited memory.

**Fix 2:** Ensure `shuffle.partitions` is set to 4, not the default 200.

**Fix 3:** Stop services you don't need (Elasticsearch, Kibana, Prometheus, Grafana) if you're only working on the streaming pipeline.

---

### No data in output topics
Check in order:
1. Is the Java producer running? → Check IntelliJ console for throughput logs
2. Is `financial_transactions` topic getting messages? → Redpanda Console → Topics
3. Did the Spark job start without errors? → Spark UI → Structured Streaming tab
4. Are checkpoints stale? → Delete `mnt/checkpoints/` and resubmit

---

### `spark.sql.adaptive.enabled is not supported in streaming`
This is a **warning, not an error**. Spark automatically disables Adaptive Query Execution for streaming DataFrames. Safe to ignore.

---

## File Structure

```
jobs/
└── spark_processor.py    ← the streaming job

mnt/
├── checkpoints/
│   ├── aggregates/       ← checkpoint state for aggregation query
│   └── anomalies/        ← checkpoint state for anomaly query
└── spark-state/          ← stateful aggregation store
```
