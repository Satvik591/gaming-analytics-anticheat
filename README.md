# 🎮 Gaming Analytics & Anti-Cheat System
### Lambda Architecture | Real-Time + Batch Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?logo=apachekafka)](https://kafka.apache.org)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-E6522C?logo=apacheflink)](https://flink.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis)](https://redis.io)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)](https://getdbt.com)
[![Docker](https://img.shields.io/badge/Docker%20Compose-3.8-2496ED?logo=docker)](https://docker.com)

---

## 📌 Project Overview

A **production-grade Lambda Architecture** data pipeline that processes millions of in-game events to:
- 🚨 **Detect cheaters in real-time** (< 200ms) using Apache Flink rule engine + ML
- 📊 **Compute accurate hourly leaderboards** using Apache Spark batch jobs
- 💾 **Serve merged batch + speed views** via PostgreSQL + Redis
- 📈 **Visualize everything** on Apache Superset + Grafana dashboards

---

## 🏗️ Architecture

```
                    ┌──────────── BATCH LAYER ─────────────────────────────────┐
                    │  Apache Spark (local mode, runs hourly via cron)          │
                    │  ├── leaderboard_batch.py  → serving_leaderboard table    │
5 Kafka Topics ───► │  ├── player_stats_batch.py → serving_player_profiles      │──► PostgreSQL ──► Superset
(10+ events/sec)    │  └── ml_training.py        → ml/model/anticheat_model.pkl │    (Batch Views)   Dashboard
                    └──────────────────────────────────────────────────────────┘
                                              │
                    ┌──────────── SPEED LAYER ─────────────────────────────────┐
                    │  Apache Flink (streaming, exactly-once)                   │──► Redis ──────► Grafana
                    │  ├── anticheat_job.py  → fraud-alerts Kafka topic         │    (Speed Views)  Real-time
                    │  └── live_stats_job.py → live match aggregates            │                   Alerts
                    └──────────────────────────────────────────────────────────┘
                                              │
                                   ┌──────────▼──────────┐
                                   │   SERVING LAYER      │
                                   │   Merged View (dbt)  │
                                   │   Batch + Speed      │
                                   └─────────────────────┘
```

### Lambda vs Kappa — Why Lambda?

| Feature | Lambda (This Project) | Kappa |
|---|---|---|
| Leaderboard accuracy | ✅ Full historical recompute | ⚠️ Approximate |
| Fraud detection latency | ✅ < 200ms (speed layer) | ✅ < 200ms |
| Historical analytics | ✅ Spark batch (complete) | ⚠️ Limited to stream window |
| Code complexity | Higher (two paths) | Lower |
| Replay capability | ✅ Kafka + Spark batch | ✅ Kafka only |

---

## 📦 Project Structure

```
gaming-analytics-anticheat/
├── 🐳 docker-compose.yml          # Full stack: Kafka, Flink, Redis, Postgres, Superset
├── 📋 requirements.txt            # Python dependencies
├── 🔐 .env.example               # Environment variable template
│
├── producers/                     # 5 Event Producers (Speed: ≥10 events/sec)
│   ├── config.py                  # Shared Kafka config & constants
│   ├── player_events_producer.py  # kills, deaths, movements, abilities
│   ├── match_logs_producer.py     # match start/end, scores, stats
│   ├── chat_producer.py           # team/global chat, toxic detection
│   ├── purchases_producer.py      # skins, weapons, battle passes
│   ├── game_state_producer.py     # server ticks: position, velocity, headshot rate
│   └── run_all.py                 # Start all 5 producers simultaneously
│
├── kafka/
│   └── schemas/                   # Avro data contracts
│       ├── player_event.avsc
│       ├── match_log.avsc
│       ├── chat_message.avsc
│       ├── purchase.avsc
│       └── game_state.avsc
│
├── flink/                         # SPEED LAYER
│   └── jobs/
│       ├── anticheat_job.py       # Real-time cheat detection (rule-based + ML)
│       └── live_stats_job.py      # Live match aggregations to Redis
│
├── spark/                         # BATCH LAYER (local mode, not Docker)
│   └── jobs/
│       ├── leaderboard_batch.py   # Hourly full leaderboard recompute
│       ├── player_stats_batch.py  # Player profile aggregations
│       └── ml_training.py        # Daily Isolation Forest model training
│
├── dbt/                           # SQL Transformation Layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/               # Raw to cleaned views
│       ├── intermediate/          # Business logic
│       └── marts/                 # Final serving tables
│
├── ml/
│   ├── train.py                   # Standalone ML training script
│   ├── predict.py                 # Real-time scoring helper
│   └── model/                     # Saved model artifacts
│
├── dashboards/
│   ├── superset/                  # Superset dashboard config
│   └── grafana/                   # Grafana dashboard JSON
│
├── data_quality/                  # Great Expectations checks
└── infra/
    ├── init_db.sql                # PostgreSQL schema initialization
    └── setup_ec2.sh               # EC2 setup script
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (8 GB RAM minimum)
- Python 3.11+
- Apache Spark (local install for batch jobs)
- Git

### 1. Clone and Configure
```bash
git clone https://github.com/Satvik591/gaming-analytics-anticheat.git
cd gaming-analytics-anticheat
cp .env.example .env
```

### 2. Start All Docker Services
```bash
docker-compose up -d
```

### 3. Verify Services
| Service | URL | Credentials |
|---|---|---|
| Kafka | `localhost:29092` | — |
| Schema Registry | `http://localhost:8081` | — |
| Flink UI | `http://localhost:8080` | — |
| PostgreSQL | `localhost:5432` | gameadmin / gamepass123 |
| Redis | `localhost:6379` | — |
| Apache Superset | `http://localhost:8088` | admin / admin123 |
| Grafana | `http://localhost:3000` | admin / admin123 |

### 4. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 5. Start All 5 Producers
```bash
cd producers
python run_all.py
```

### 6. Run Spark Batch Jobs (locally)
```bash
spark-submit --master local[*] spark/jobs/leaderboard_batch.py
```

### 7. Run dbt Transformations
```bash
cd dbt
dbt run
dbt test
```

---

## 🎯 Kafka Topics

| Topic | Producer | Consumer | Retention |
|---|---|---|---|
| `player-events` | player_events_producer | Flink, Spark | 7 days |
| `match-logs` | match_logs_producer | Flink, Spark | 7 days |
| `chat-messages` | chat_producer | Flink | 7 days |
| `purchases` | purchases_producer | Spark | 7 days |
| `game-state` | game_state_producer | Flink (anti-cheat) | 7 days |
| `fraud-alerts` | Flink (output) | Grafana, PostgreSQL | 7 days |

---

## 🕵️ Anti-Cheat Detection Rules

| Cheat Type | Detection Method | Threshold |
|---|---|---|
| **Aimbot** | Headshot rate in 1-min window | > 85% |
| **Aimbot** | Reaction time | < 100ms |
| **Speedhack** | Movement velocity | > 300 units/sec |
| **Macro** | Accuracy consistency | > 85% over 5+ events |
| **Wallhack** | ML Isolation Forest score | Anomaly score < -0.1 |

---

## 🗓️ Project Timeline

| Phase | Deliverable | Status |
|---|---|---|
| Phase 1 | Architecture + Tech decisions + Repo setup | ✅ Done |
| Phase 2 | Docker stack + Kafka + All 5 Producers | 🔄 In Progress |
| Phase 3 | Flink speed layer (anti-cheat job) | ⏳ Pending |
| Phase 4 | Spark batch layer + ML training | ⏳ Pending |
| Phase 5 | dbt models + Serving layer | ⏳ Pending |
| Phase 6 | Superset + Grafana dashboards | ⏳ Pending |
| Phase 7 | Great Expectations data quality | ⏳ Pending |
| Phase 8 | Testing + Documentation | ⏳ Pending |

---

## 👤 Author

**Satvik591** | Data Engineering Project | 2026
