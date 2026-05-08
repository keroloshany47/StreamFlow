# monitoring/

This folder contains the full observability stack configuration:

| Folder | Tool | Purpose |
|---|---|---|
| `prometheus/` | Prometheus | Scrapes metrics from Kafka (via JMX Exporter) and other services |
| `prometheus/rules/` | Prometheus | Alert rules (e.g. broker down, consumer lag) |
| `alertmanager/` | Alertmanager | Routes alerts to email/Slack/webhook |
| `grafana/provisioning/` | Grafana | Auto-provisions Prometheus as a datasource on startup |

---

## Grafana

- URL: http://localhost:3000
- Login: `admin` / `admin`
- Prometheus datasource is auto-provisioned — no manual setup needed.
- To add dashboards: import from [grafana.com/dashboards](https://grafana.com/dashboards) — recommended IDs:
  - **7589** — Kafka Overview
  - **11962** — Kafka Lag Exporter

## Prometheus

- URL: http://localhost:9090
- Config: `prometheus/prometheus.yml`
- Retention: 7 days

## Alertmanager

- URL: http://localhost:9093
- Config: `alertmanager/alertmanager.yml`
- Edit the config to point to your Slack webhook or email SMTP.
