# KafkaSparkArch 🚀

A production-grade, fully containerized real-time data streaming pipeline built on **Apache Kafka** (KRaft mode) + **Apache Spark** + **Elasticsearch**, with a complete observability stack.

---

## 🏗️ Architecture Overview

```
Producers
   │
   ▼
┌─────────────────────────────────────┐
│         Apache Kafka (KRaft)        │
│  3 Controllers  +  3 Brokers        │
│  Schema Registry  │  Redpanda UI    │
└─────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────┐
│         Apache Spark 3.5            │
│  1 Master  +  3 Workers             │
│  Structured Streaming Jobs          │
└─────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────┐
│      Elasticsearch + Kibana         │
│      Search & Visualization         │
└─────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────┐
│     Prometheus + Grafana            │
│     Metrics & Alerting              │
└─────────────────────────────────────┘
```

---

## 📦 Stack

| Component | Version | Purpose |
|---|---|---|
| Apache Kafka | 3.8.1 | Distributed message broker (KRaft, no ZooKeeper) |
| Confluent Schema Registry | 7.5.1 | Avro/Protobuf schema management |
| Redpanda Console | 2.5.2 | Kafka UI |
| Apache Spark | 3.5.0 | Stream processing engine |
| Elasticsearch | 8.12.0 | Search & analytics store |
| Kibana | 8.12.0 | Elasticsearch UI |
| Prometheus | 3.0.0 | Metrics collection |
| Alertmanager | 0.27.0 | Alert routing |
| Grafana | 10.4.0 | Metrics dashboards |

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Redpanda Console | http://localhost:8085 | — |
| Schema Registry | http://localhost:18081 | — |
| Spark Master UI | http://localhost:9190 | — |
| Spark App UI | http://localhost:4040 | — |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Alertmanager | http://localhost:9093 | — |

---

## 🚀 Quick Start

### Prerequisites
- Docker >= 24
- Docker Compose >= 2.20
- 8 GB RAM available for Docker
- `curl` installed on host

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/KafkaSparkArch.git
cd KafkaSparkArch
```

### 2. Download JMX Exporter jar
```bash
curl -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -o volumes/jmx_exporter/jmx_prometheus_javaagent.jar
```

### 3. Create required directories
```bash
mkdir -p jobs mnt/checkpoints mnt/spark-state
```

### 4. Start everything
```bash
docker compose up -d
```

### 5. Verify
```bash
docker compose ps
curl http://localhost:9200/_cluster/health?pretty
```

### Stop
```bash
docker compose down
# To also remove volumes (full reset):
docker compose down -v
```

---

## 📁 Project Structure

```
KafkaSparkArch/
├── docker-compose.yml          # Full stack definition
├── .gitignore
├── README.md                   # ← you are here
├── jobs/                       # PySpark streaming jobs
│   └── README.md
├── volumes/
│   └── jmx_exporter/           # JMX → Prometheus bridge
│       ├── README.md
│       └── kafka-broker.yml
├── monitoring/
│   ├── README.md
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── rules/
│   │       └── alert_rules.yml
│   ├── alertmanager/
│   │   └── alertmanager.yml
│   └── grafana/
│       └── provisioning/
│           └── datasources/
│               └── datasources.yml
└── mnt/
    ├── checkpoints/            # Spark checkpoint storage
    └── spark-state/            # Spark state store
```

---

## 🔌 Kafka Connectivity

| From | Bootstrap Servers |
|---|---|
| Inside Docker (Spark, Schema Registry) | `kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092` |
| From your host machine | `localhost:29092,localhost:39092,localhost:49092` |

---

## 📝 Notes

- Kafka runs in **KRaft mode** — no ZooKeeper required.
- Elasticsearch security (`xpack.security`) is disabled for local dev. Enable it for production.
- Grafana anonymous access is enabled for convenience. Change `GF_AUTH_ANONYMOUS_ENABLED` for production.
- The JMX Exporter jar is excluded from git (see `.gitignore`) — download it manually per the Quick Start above.

---

## 🤝 Contributing

Pull requests are welcome. For major changes, open an issue first.
