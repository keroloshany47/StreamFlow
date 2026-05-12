# Monitoring & Observability

This folder contains the full observability stack for the **StreamFlow** platform, providing real-time monitoring across Apache Kafka, Apache Spark, and Elasticsearch.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Stack Components](#stack-components)
- [Quick Start](#quick-start)
- [Grafana Dashboard](#grafana-dashboard)
- [Metrics Reference](#metrics-reference)
- [Alert Rules](#alert-rules)
- [Folder Structure](#folder-structure)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          StreamFlow Platform                             │
│                                                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                       │
│  │kafka-broker │  │kafka-broker │  │kafka-broker │  JMX Exporter         │
│  │    -1:9401  │  │    -2:9402  │  │    -3:9403  │  (per broker)         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                       │
│         │                │                │                              │
│  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐                       │
│  │kafka-ctrl-1 │  │kafka-ctrl-2 │  │kafka-ctrl-3 │  JMX Exporter         │
│  │    :9301    │  │    :9302    │  │    :9303    │  (per controller)     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                       │
│         │                                                                │
│  ┌──────┴──────┐                                                         │
│  │spark-master │  JMX Exporter :9404                                     │
│  └──────┬──────┘                                                         │
│         │                                                                │
│  ┌──────┴──────┐                                                         │
│  │elasticsearch│  Native Prometheus endpoint :9200/_prometheus/metrics   │
│  └──────┬──────┘                                                         │
│         │                                                                │
│         └─────────────────────────────┐                                  │
│                                       ▼                                  │
│                         ┌─────────────────────────┐                      │
│                         │   Prometheus :9090      │                      │
│                         │  scrape_interval: 10s   │                      │
│                         │  retention: 7 days      │                      │
│                         └────────────┬────────────┘                      │
│                                      │                                   │
│                         ┌────────────▼────────────┐                      │
│                         │     Grafana :3000       │                      │
│                         │  StreamFlow Dashboard   │                      │
│                         │  Auto-provisioned DS    │                      │
│                         └─────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────────┘
```

**Data flow:** Each service exposes metrics via JMX Exporter (Kafka, Spark) or a native endpoint (Elasticsearch). Prometheus scrapes all targets every 10–15 seconds. Grafana queries Prometheus and renders the StreamFlow Dashboard in real time with a 30-second auto-refresh.

---

## Stack Components

| Component | Port | Scrape Interval | Purpose |
|---|---|---|---|
| **Prometheus** | `9090` | — | Metrics collection & storage (7-day retention) |
| **JMX Exporter** (brokers) | `9401–9403` | 10s | Exposes Kafka broker JVM + broker metrics |
| **JMX Exporter** (controllers) | `9301–9303` | 10s | Exposes Kafka controller metrics |
| **JMX Exporter** (Spark) | `9404` | 15s | Exposes Spark master JVM metrics |
| **Elasticsearch** | `9200` | 15s | Native `/_prometheus/metrics` endpoint |
| **Grafana** | `3000` | — | Visualization & dashboards |
| **Alertmanager** | `9093` | — | Routes alerts (email / Slack / webhook) |

---

## Quick Start

### 1. Start the monitoring stack

```bash
docker compose -f docker-compose.monitoring.yml up -d
```

### 2. Verify Prometheus targets

Open [http://localhost:9090/targets](http://localhost:9090/targets) and confirm all targets show **UP**:

```
kafka-brokers       → 3/3 UP  (kafka-broker-1:9401, -2:9402, -3:9403)
kafka-controllers   → 3/3 UP  (kafka-controller-1:9301, -2:9302, -3:9303)
spark               → 1/1 UP  (spark-master:9404)

```

![Prometheus Targets](../Imgs/Prometheus.png)

### 3. Open Grafana

Navigate to [http://localhost:3000](http://localhost:3000)

```
Username: admin
Password: admin
```

> The Prometheus datasource is **auto-provisioned** via `grafana/provisioning/` — no manual setup needed.

### 4. Import the StreamFlow Dashboard

**Dashboards → Import → Upload JSON file**, then select:

```
monitoring/grafana/ dashboards/StreamFlow_dashboard.json
```

---

## Grafana Dashboard

![StreamFlow Dashboard](../Imgs/Grafana.png)

The dashboard is split into three sections, each auto-refreshing every **30 seconds**.

### Section 1 — Healthcheck

| Panel | PromQL | Healthy value |
|---|---|---|
| **# of Brokers Online** | `count(kafka_server_replicamanager_leadercount{job="kafka-brokers"})` | = 3 |
| **Online Partitions** | `sum(kafka_server_replicamanager_partitioncount{job="kafka-brokers"})` | Matches topic config |
| **Offline Partitions** | `sum(kafka_controller_kafkacontroller_offlinepartitionscount)` | **Must be 0** |
| **Preferred Replica Imbalance** | `sum(kafka_controller_kafkacontroller_preferredreplicaimbalancecount)` | 0 is ideal |
| **Broker Network Throughput** | `bytesinpersec` + `bytesoutpersec` (combined) | Depends on load |

### Section 2 — Throughput In/Out

| Panel | PromQL | Why it matters |
|---|---|---|
| **Messages In Per Broker** | `sum by (instance) (messagesinpersec)` | Detects uneven producer load |
| **Bytes In Per Broker** | `sum by (instance) (bytesinpersec)` | Actual data volume per broker |
| **Bytes Out Per Broker** | `sum by (instance) (bytesoutpersec)` | Consumer fetch traffic per broker |

> If one broker consistently shows higher throughput than the others, partition assignment needs rebalancing.

### Section 3 — System

| Panel | PromQL | Why it matters |
|---|---|---|
| **CPU Usage** | `sum by (instance) (rate(process_cpu_seconds_total{job=~"kafka.*"}[5m]))` | Detects hot brokers |
| **Time Spent in GC** | `sum by (instance, gc) (rate(jvm_gc_collection_seconds_sum[5m]))` | GC pressure causes latency spikes |

> G1 Concurrent GC is expected and minimal. Watch for **G1 Old Generation** spikes — they indicate the broker JVM heap needs tuning.

---

## Metrics Reference

### Kafka Broker

| Metric | Description |
|---|---|
| `kafka_server_replicamanager_leadercount` | Number of leader partitions on this broker |
| `kafka_server_replicamanager_partitioncount` | Total partitions assigned to this broker |
| `kafka_server_replicamanager_underreplicatedpartitions` | Partitions not fully replicated — alert on > 0 |
| `kafka_server_brokertopicmetrics_messagesinpersec` | Producer message rate (summed across topics) |
| `kafka_server_brokertopicmetrics_bytesinpersec` | Incoming byte rate from producers |
| `kafka_server_brokertopicmetrics_bytesoutpersec` | Outgoing byte rate to consumers |

### Kafka Controller

| Metric | Description |
|---|---|
| `kafka_controller_kafkacontroller_offlinepartitionscount` | Partitions with no active leader — **critical if > 0** |
| `kafka_controller_kafkacontroller_preferredreplicaimbalancecount` | Partitions not on their preferred leader |

### JVM (all Kafka processes)

| Metric | Description |
|---|---|
| `process_cpu_seconds_total` | CPU time consumed by the JVM process |
| `jvm_gc_collection_seconds_sum` | Time spent in GC per collector type |

### Elasticsearch

| Metric | Description |
|---|---|
| `elasticsearch_cluster_health_status{color="red"}` | Cluster red = primary shards missing, writes failing |
| `elasticsearch_jvm_memory_used_bytes` | JVM heap currently in use |
| `elasticsearch_jvm_memory_max_bytes` | Maximum configured JVM heap |

---

## Alert Rules

All rules live in `prometheus/rules/kafka_alerts.yml` and are evaluated every **10 seconds**.

###  Critical — Kafka Broker Down

```yaml
alert: KafkaBrokerDown
expr: up{job="kafka-brokers"} == 0
for: 1m
```

**Why:** A down broker reduces fault tolerance. With replication factor 3, losing 2 brokers makes partitions unavailable.

---

###  Critical — Kafka Controller Down

```yaml
alert: KafkaControllerDown
expr: up{job="kafka-controllers"} == 0
for: 1m
```

**Why:** Controllers manage partition leadership elections. A downed controller can stall recovery after broker failures.

---

###  Critical — Elasticsearch Cluster Red

```yaml
alert: ElasticsearchClusterRed
expr: elasticsearch_cluster_health_status{color="red"} == 1
for: 1m
```

**Why:** Red status means one or more primary shards are unassigned — indexing will fail and data loss is possible.

---

###  Warning — Under-Replicated Partitions

```yaml
alert: KafkaUnderReplicatedPartitions
expr: kafka_server_replicamanager_underreplicatedpartitions > 0
for: 2m
```

**Why:** Under-replicated partitions mean the cluster is operating without full redundancy. A broker failure at this moment causes data loss.

---

###  Warning — High Request Latency

```yaml
alert: KafkaHighRequestLatency
expr: kafka_request_latency_ms > 100
for: 1m
```

**Why:** Latency above 100ms sustained for 1 minute indicates I/O pressure, network saturation, or GC pauses affecting producer/consumer throughput.

---

###  Warning — High Consumer Lag

```yaml
alert: KafkaConsumerLag
expr: kafka_consumergroup_lag > 1000
for: 5m
```

**Why:** A lag above 1000 messages for 5 minutes means consumers are falling behind producers. Downstream processing (e.g. Spark jobs) will receive stale data.

---

###  Warning — Elasticsearch High JVM Heap

```yaml
alert: ElasticsearchHighJVMHeap
expr: elasticsearch_jvm_memory_used_bytes / elasticsearch_jvm_memory_max_bytes > 0.85
for: 5m
```

**Why:** When heap exceeds 85%, Elasticsearch triggers aggressive GC, blocking indexing and search. At 95%+ the node enters circuit-breaker state and rejects all requests.

---

## Folder Structure

```
monitoring/
├── prometheus/
│   ├── prometheus.yml              # Global config, scrape targets & alertmanager routing
│   └── rules/
│       └── kafka_alerts.yml        # All alert rules (Kafka + Elasticsearch)
├── grafana/
│   ├── provisioning/
│   │   └── datasources/
│   │       └── prometheus.yml      # Auto-provisions Prometheus datasource on startup
│   └── dashboards/
│       └── StreamFlow_dashboard.json
├── Imgs/
│   ├── Grafana.png                 # StreamFlow dashboard screenshot
│   └── Prometheus.png              # Prometheus targets health screenshot
└── README.md
```

---

## Notes

- Prometheus retention defaults to **7 days**. Adjust via `--storage.tsdb.retention.time` in `docker-compose.monitoring.yml`.
- Spark JMX Exporter requires `SPARK_DAEMON_JAVA_OPTS` to load the javaagent — see the main project README for the required env var.
- All Kafka PromQL queries use `sum by (instance)` to prevent duplicate series when a broker exports metrics per topic.
- The dashboard `uid` is `StreamFlow_dashboard` — re-importing updates the existing dashboard rather than creating a duplicate.