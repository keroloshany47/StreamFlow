# Python Producer

A multi-threaded Kafka producer that publishes synthetic financial transactions to the `financial_transactions` topic. It is intentionally designed to generate high-value transactions that frequently cross the Spark anomaly detection threshold — making it the go-to tool for testing the anomaly pipeline end-to-end.

> Run this alongside the Java producer to get a realistic mix of normal and anomalous traffic in the same topic.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Comparison with Java Producer](#comparison-with-java-producer)
3. [Prerequisites](#prerequisites)
4. [Setup & Run](#setup--run)
5. [Transaction Schema](#transaction-schema)
6. [Kafka Configuration](#kafka-configuration)
7. [Anomaly Design](#anomaly-design)
8. [Running Both Producers Together](#running-both-producers-together)

---

## How It Works

```
main()
  │
  ├── create_topic()           AdminClient checks / creates topic if needed
  │
  └── produce_data_in_parallel(num_threads=3)
        │
        ├── Thread 0  → infinite produce loop
        ├── Thread 1  → infinite produce loop
        └── Thread 2  → infinite produce loop
                │
                ├── generate_transaction()   random field values
                ├── json.dumps(transaction)  serialize to compact JSON
                └── producer.produce()       async send + flush
```

Each thread runs an infinite loop generating transactions and flushing them synchronously. This `flush()` per message approach limits raw throughput intentionally — the Python producer is optimized for anomaly coverage, not maximum records-per-second.

---

## Comparison with Java Producer

| | Java Producer | Python Producer |
|---|---|---|
| File | `src/.../TransactionProducer.java` | `Python_producer/Python_Producer.py` |
| Library | `kafka-clients` | `confluent-kafka` |
| Threads | CPU cores − 1 | 3 |
| Throughput | ~300,000 records/sec | Lower (flush per message) |
| Compression | snappy | gzip |
| Partition key | `transactionId` | `userId` |
| Amount range | $10 – $510 | $50,000 – $150,000 |
| Anomaly trigger | Rarely | Frequently ✓ |
| Best for | Max throughput load | Anomaly detection testing |

---

## Prerequisites

- Python 3.10+
- Kafka brokers running via `docker compose up -d`

---

## Setup & Run

### Install dependencies

```bash
# From the repo root
pip install -r requirements.txt
```

### Run

```bash
# From repo root
python Python_producer/Python_Producer.py

# Or from inside the Python_producer folder
cd Python_producer
python Python_Producer.py
```

### Expected output

```
INFO  Topic 'financial_transactions' already exists.
Thread 0: Produced transaction: {'transactionId': 'a1b2...', 'amount': 142500.75, ...}
Record user_42 produced successfully!
Thread 1: Produced transaction: {'transactionId': 'c3d4...', 'amount': 89300.20, ...}
Record user_17 produced successfully!
```

---

## Transaction Schema

```json
{
  "transactionId":   "550e8400-e29b-41d4-a716-446655440000",
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

**Field distributions:**

| Field | Values |
|---|---|
| `transactionId` | UUID v4 |
| `userId` | `user_1` – `user_100` |
| `amount` | Uniform random $50,000 – $150,000 |
| `merchantId` | `merchant_1`, `merchant_2`, `merchant_3` |
| `transactionType` | `purchase` / `refund` (50/50) |
| `location` | `location_1` – `location_50` |
| `paymentMethod` | `credit_card`, `paypal`, `bank_transfer` |
| `isInternational` | `"True"` / `"False"` (string — see note below) |
| `currency` | `USD`, `EUR`, `GBP` |

> **Note:** `isInternational` is serialized as a string (`"True"` / `"False"`), not a native JSON boolean. The Spark processor converts it to a proper boolean during ingestion. Switching to a native boolean would save ~4–5 bytes per event.

---

## Kafka Configuration

```python
producer_conf = {
    'bootstrap.servers':            'localhost:29092,localhost:39092,localhost:49092',
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes':   512000,
    'batch.num.messages':           1000,
    'linger.ms':                    10,
    'acks':                         1,
    'compression.type':             'gzip'
}
```

| Config | Value | Why |
|---|---|---|
| `linger.ms` | 10ms | Wait to accumulate messages into a batch |
| `acks` | 1 | Leader ack only — trades durability for speed |
| `compression.type` | gzip | Higher compression ratio than snappy (payload is repetitive JSON) |
| `batch.num.messages` | 1000 | Max messages per batch |

---

## Anomaly Design

The Spark anomaly detector flags transactions above **$140,000**. The Python producer's amount range is $50,000–$150,000, which means roughly 1 in 10 transactions crosses this threshold — creating a steady, testable flow of anomalies into the `transaction_anomalies` Kafka topic.

The Java producer's amount range is capped at ~$510, so it almost never triggers anomalies. Running both together gives you:
- `transaction_aggregates` — dominated by Java producer volume
- `transaction_anomalies` — driven entirely by Python producer output

---

## Running Both Producers Together

**Terminal 1 — Java (throughput)**
```bash
cd ~/StreamFlow
./gradlew run
```

**Terminal 2 — Python (anomalies)**
```bash
cd ~/StreamFlow
python Python_producer/Python_Producer.py
```

Then verify in Redpanda Console (`http://localhost:8085`) that both `transaction_aggregates` and `transaction_anomalies` topics are receiving data.

---

## Topic Configuration

The producer auto-creates the topic if it doesn't exist:

| Config | Value |
|---|---|
| Topic name | `financial_transactions` |
| Partitions | 16 |
| Replication factor | 3 |
| Bootstrap servers | `localhost:29092,39092,49092` |
