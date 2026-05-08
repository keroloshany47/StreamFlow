# volumes/jmx_exporter/

This folder contains the **JMX Prometheus Java Agent** setup that exposes Kafka metrics to Prometheus.

---

## Files

| File | Description |
|---|---|
| `kafka-broker.yml` | Metric filter config — defines which JMX beans to expose |
| `jmx_prometheus_javaagent.jar` | The agent jar — **not committed to git, download manually** |

---

## Download the jar

```bash
curl -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -o volumes/jmx_exporter/jmx_prometheus_javaagent.jar
```

---

## How it works

Each Kafka container (controllers + brokers) loads the jar via `KAFKA_OPTS`:

```
-javaagent:/usr/share/jmx_exporter/jmx_prometheus_javaagent.jar=<PORT>:/usr/share/jmx_exporter/kafka-broker.yml
```

Prometheus then scrapes each container on its assigned port:

| Container | Metrics Port |
|---|---|
| kafka-controller-1 | 9301 |
| kafka-controller-2 | 9302 |
| kafka-controller-3 | 9303 |
| kafka-broker-1 | 9401 |
| kafka-broker-2 | 9402 |
| kafka-broker-3 | 9403 |
