# Transaction Producers

This project includes two Kafka producers that publish synthetic financial transactions to the `financial_transactions` topic. They are designed to run **together** — the Java producer maximizes throughput while the Python producer targets the anomaly detection pipeline.

---

## File Structure

```
KafkaSparkArch/
│
├── producers/                          
│   ├── Python_Producer.py              ← Python producer
│   └── README.md                       

```

---

## At a Glance

| | Java Producer | Python Producer |
|---|---|---|
| File | `src/.../TransactionProducer.java` | `Python_Producer.py` |
| Library | `kafka-clients` | `confluent-kafka` |
| Threads | CPU cores − 1 | 3 |
| Throughput | ~300,000 records/sec | Lower (flush per message) |
| Compression | snappy | gzip |
| Partition key | `transactionId` | `userId` |
| Amount range | $10 – $510 | $50,000 – $150,000 |
| Anomaly trigger | Rarely | Frequently ✓ |
| Best for | Max throughput load | Anomaly detection testing |

> Run both simultaneously to mix normal and anomalous traffic in the same topic.

---

## Transaction Schema

Both producers publish the same JSON schema to `financial_transactions`:

```json
{
  "transactionId":   "uuid-v4",
  "userId":          "user_42",
  "merchantId":      "merchant_2",
  "amount":          127450.75,
  "transactionTime": 1715500000,
  "transactionType": "purchase",
  "location":        "location_12",
  "paymentMethod":   "credit_card",
  "isInternational": "True",
  "currency":        "USD"
}
```

>  `isInternational` is serialized as a string (`"True"` / `"False"`), not a boolean. The Spark processor converts it to a proper boolean during ingestion.

---

## Java Producer

### Prerequisites
- JDK 11+
- Gradle wrapper included (`./gradlew`)
- Kafka brokers running (`docker compose up -d`)

### Run
```bash
./gradlew run
```

Or via the pre-built JAR:
```bash
java -jar build/libs/KafkaSparkArch-1.0-SNAPSHOT.jar
```

### Expected output
```
INFO  Starting 8 producer threads on 8 cores
INFO  Topic 'financial_transactions' already exists.
INFO  Throughput: 280,000 records/sec | Total: 280,000
INFO  Throughput: 310,000 records/sec | Total: 590,000
```

### Graceful shutdown
`Ctrl+C` flushes all buffered messages before exit:
```
INFO  Shutting down — flushing remaining messages...
INFO  Done. Total sent: 4,200,000
```

### Kafka Config
| Config | Value | Why |
|---|---|---|
| `BATCH_SIZE` | 512 KB | Fewer network round trips |
| `LINGER_MS` | 10 ms | Wait to fill batch |
| `COMPRESSION` | snappy | Fast, ~2–3x size reduction |
| `ACKS` | 1 | Leader ack only — max speed |
| `BUFFER_MEMORY` | 256 MB | In-memory buffer before backpressure |

---

## Python Producer

### Prerequisites
- Python 3.10+
- Kafka brokers running (`docker compose up -d`)

### Setup
```bash

source StreamFlow/bin/activate


pip install -r requirements.txt
```

### Run
```bash

python producers/Python_Producer.py


cd producers
python Python_Producer.py
```

### Expected output
```
INFO Topic 'financial_transactions' already exists.
Thread 0: Produced transaction: {'transactionId': 'a1b2...', 'amount': 142500.75, ...}
Record user_42 produced successfully!
```

### Kafka Config
```python
{
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes':   512000,
    'batch.num.messages':           1000,
    'linger.ms':                    10,
    'acks':                         1,
    'compression.type':             'gzip'
}
```

### Why the amount range is 50k–150k
The Spark anomaly detector flags transactions above **$140,000**. The Python producer's range is intentionally set to cross this threshold frequently, making it the go-to tool for testing the anomaly pipeline end-to-end.

The Java producer caps at ~$510 so it almost never triggers anomalies — it's purely for throughput load.

---

## Running Both Together



**Terminal 1 — Java (throughput)**
```bash
cd ~/KafkaSparkArch
./gradlew run
```

**Terminal 2 — Python (anomalies)**
```bash
cd ~/KafkaSparkArch
source StreamFlow/bin/activate
python producers/Python_Producer.py
```

Then verify in Redpanda Console (`localhost:8085`) that both `transaction_aggregates` and `transaction_anomalies` topics are receiving data.

---

## Topic Config

Both producers auto-create the topic if it doesn't exist:

| Config | Value |
|---|---|
| Topic name | `financial_transactions` |
| Partitions | 16 |
| Replication factor | 3 |
| Bootstrap servers | `localhost:29092,39092,49092` |