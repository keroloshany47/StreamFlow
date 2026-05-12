# volumes/jmx_exporter/

This folder contains the configuration for the **JMX Prometheus Java Agent** — a lightweight Java agent that attaches to the JVM of each Kafka and Spark process and exposes internal metrics over HTTP so Prometheus can scrape them.

> **Why JMX?**  
> Kafka and Spark expose their internal health and performance data via Java Management Extensions (JMX). The JMX exporter acts as a bridge, converting those JMX bean values into the Prometheus text format at a configurable HTTP endpoint.

---

## Folder structure

```
volumes/jmx_exporter/
├── kafka-broker.yml              # Metric rules for Kafka controllers & brokers
├── spark.yml                     # Metric rules for Spark Master
├── jmx_prometheus_javaagent.jar  # NOT in git — download manually (see below)
└── README.md
```

---

## Quick start — download the agent jar

The jar is excluded from version control because of its size. Download it once before running the stack:

```bash
curl -L \
  https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -o volumes/jmx_exporter/jmx_prometheus_javaagent.jar
```

Verify the download:
```bash
ls -lh volumes/jmx_exporter/jmx_prometheus_javaagent.jar
# Expected: ~~ 360 KB
```

> You can find all available versions on [Maven Central](https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/).

---

## How it works

Each container loads the jar as a JVM agent via the `KAFKA_OPTS` (or `SPARK_DAEMON_JAVA_OPTS`) environment variable:

```
-javaagent:/usr/share/jmx_exporter/jmx_prometheus_javaagent.jar=<PORT>:<CONFIG_FILE>
```

At startup, the agent:
1. Reads the YAML rule file to know which JMX beans to expose.
2. Opens an HTTP server on `<PORT>`.
3. Responds to `GET /metrics` with Prometheus-format text — Prometheus scrapes this endpoint on its configured interval.

---

## Metrics ports

### Kafka

| Container              | Role       | Metrics Port |
|------------------------|------------|:------------:|
| `kafka-controller-1`   | Controller | `9301`       |
| `kafka-controller-2`   | Controller | `9302`       |
| `kafka-controller-3`   | Controller | `9303`       |
| `kafka-broker-1`       | Broker     | `9401`       |
| `kafka-broker-2`       | Broker     | `9402`       |
| `kafka-broker-3`       | Broker     | `9403`       |

You can verify a broker is exposing metrics:
```bash
curl -s http://localhost:9401/metrics | grep kafka_server
```

### Spark

| Container        | Role          | Metrics Port |
|------------------|---------------|:------------:|
| `spark-master`   | Spark Master  | `9501`       |

> **Note:** Spark workers don't expose JMX metrics in this setup. To enable them, add `SPARK_WORKER_JAVA_OPTS` with the javaagent flag and a unique port per worker in `docker-compose.yml`.

Verify Spark metrics are live:
```bash
curl -s http://localhost:9501/metrics | grep spark_
```

---

## Configuration files

### `kafka-broker.yml`

Controls which Kafka JMX beans get scraped and how they are named in Prometheus. Key metric groups covered:

| Group | Example metric | What it tells you |
|---|---|---|
| `BrokerTopicMetrics` | `kafka_server_brokertopicmetrics_bytesinpersec` | Throughput per topic |
| `ReplicaManager` | `kafka_server_replicamanager_underreplicatedpartitions` | Replication health |
| `Controller` | `kafka_controller_kafkacontroller_activecontrollercount` | Leader election |
| `Network` | `kafka_network_requestmetrics_requestspersec` | Request rates |
| `Log` | `kafka_log_log_size` | On-disk log size per topic/partition |
| JVM | `jvm_memory_heap_used`, `process_cpu_seconds_total` | JVM health |

### `spark.yml`

Controls which Spark Master JMX beans are scraped:

| Group | Example metric | What it tells you |
|---|---|---|
| Spark internals | `spark_master_apps` | Active application count |
| JVM memory | `spark_jvm_heap_used_bytes` | Heap pressure |
| JVM GC | `spark_jvm_gc_collection_count` | GC frequency and type |
| Catch-all | `spark_*` | Any other bean not explicitly matched |

---

## How to add a new metric rule

Rules in both YAML files are matched top-to-bottom — the **first matching pattern wins**. Each rule has three main parts:

```yaml
- pattern: 'java.lang<type=OperatingSystem><>ProcessCpuTime'   # JMX bean path (regex OK)
  name: process_cpu_seconds_total                               # Prometheus metric name
  type: GAUGE                                                   # GAUGE or COUNTER
  valueFactor: 0.000000001                                      # Optional: unit conversion
  labels:                                                       # Optional: extra labels
    some_label: "$1"                                            # $1 = first regex capture group
```

### Step-by-step: expose a new Kafka metric

1. **Find the JMX bean name.** Run this from inside a broker container:
   ```bash
   docker exec -it kafka-broker-1 bash
   # Then use jconsole or:
   kafka-run-class.sh kafka.tools.JmxTool \
     --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
     --report-format properties 2>/dev/null | grep -i "your_keyword"
   ```

2. **Write the pattern.** JMX object names map like this:
   ```
   kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
                 ↓ in YAML pattern syntax ↓
   kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec><>OneMinuteRate
   ```

3. **Add the rule** to `kafka-broker.yml` (above the catch-all rules at the bottom):
   ```yaml
   - pattern: kafka.server<type=BrokerTopicMetrics, name=FailedFetchRequestsPerSec><>OneMinuteRate
     name: kafka_server_brokertopicmetrics_failedfetchrequestspersec
     type: GAUGE
   ```

4. **Restart the broker** to reload the config:
   ```bash
   docker compose restart kafka-broker-1
   ```

5. **Verify** the new metric appears:
   ```bash
   curl -s http://localhost:9401/metrics | grep failedfetch
   ```

---

## Connecting to Grafana

### Prerequisites
- Prometheus is already scraping the JMX endpoints (check `prometheus.yml` targets).
- Grafana is running and has Prometheus added as a data source.

### Importing a dashboard

1. Open Grafana → **Dashboards → Import**.
2. Paste the dashboard JSON ID or upload a `.json` file.
3. Select your Prometheus data source and click **Import**.

Recommended community dashboards:
| Dashboard | Grafana ID |
|---|---|
| Kafka Overview (by Confluent) | `7589` |
| Kafka Exporter Overview | `11962` |
| JVM Micrometer | `4701` |

### Key Prometheus queries for Kafka

```promql
# Messages ingested per second (all topics)
rate(kafka_server_brokertopicmetrics_messagesinpersec{topic!=""}[5m])

# Broker network bytes in
rate(kafka_server_brokertopicmetrics_bytesinpersec[5m])

# Under-replicated partitions (should be 0)
kafka_server_replicamanager_underreplicatedpartitions

# Active controller count (should be exactly 1)
kafka_controller_kafkacontroller_activecontrollercount

# JVM heap usage per broker
jvm_memory_heap_used / jvm_memory_heap_max
```

### Key Prometheus queries for Spark

```promql
# Heap memory used by Spark Master
spark_jvm_heap_used_bytes

# GC time rate
rate(spark_jvm_gc_collection_time_ms[5m])
```

---

## Troubleshooting

### `/metrics` returns empty or connection refused
- Confirm the jar exists: `ls -lh volumes/jmx_exporter/jmx_prometheus_javaagent.jar`
- Confirm the port is mapped in `docker-compose.yml` (e.g. `9401:9401`)
- Check container logs: `docker compose logs kafka-broker-1 | grep jmx`

### Metrics appear but have unexpected names
- Rules are matched top-to-bottom; a catch-all earlier in the file may be winning.  
- Add your specific rule **above** any broad `.*` patterns.

### `lowercaseOutputName: true` is stripping underscores
- This setting only lowercases the metric name — it does not affect underscores.  
- If the output name looks wrong, check `valueFactor` isn't accidentally set on a name-only rule.

### Prometheus shows target as `DOWN`
- Verify the scrape config in `prometheus.yml` matches the port assigned to that container.
- Check network: `docker network inspect <your_network>` to confirm containers are reachable by hostname.
