# Melbourne Weather Streaming Pipeline

A real-time data engineering pipeline that ingests live weather data for Melbourne from the OpenWeatherMap API, streams it through Kafka, processes it with Spark Structured Streaming, and stores it in ClickHouse for analytics.

---

## Architecture

```
┌─────────────────────┐
│  OpenWeatherMap API │
│  (Melbourne, AU)    │
│  lat=-37.8136       │
│  lon=144.9631       │
└──────────┬──────────┘
           │  HTTPS (every 5s for 60s)
           ▼
┌─────────────────────┐
│      Airflow        │
│  ┌───────────────┐  │
│  │   Scheduler   │  │  pulls weather JSON,
│  │   Webserver   │  │  flattens it, sends to Kafka
│  └───────┬───────┘  │
└──────────┼──────────┘
           │  Kafka Producer
           ▼
┌─────────────────────┐
│    Apache Kafka     │
│  ┌───────────────┐  │
│  │   Zookeeper   │  │  topic: weather_data
│  │     Broker    │  │  partitions: 1
│  └───────────────┘  │
└──────────┬──────────┘
           │  Kafka Consumer (Structured Streaming)
           ▼
┌─────────────────────┐
│   Apache Spark      │
│  ┌───────────────┐  │
│  │ Master        │  │  parse JSON → enforce schema
│  │ Worker        │  │  → write every 15s
│  └───────┬───────┘  │
└──────────┼──────────┘
           │  JDBC (foreachBatch)
           ▼
┌─────────────────────┐
│     ClickHouse      │
│   default.weather   │
│   (MergeTree)       │
└─────────────────────┘
```

### Data Flow Summary

1. **Ingestion** — An Airflow DAG calls the OpenWeatherMap API every 5 seconds for 60 seconds, flattens the JSON response into a flat record, and publishes each record to the Kafka topic `weather_data`.
2. **Streaming** — A Spark Structured Streaming job subscribes to `weather_data`, parses each message against a fixed schema, and triggers a micro-batch every 15 seconds.
3. **Storage** — Each micro-batch is appended to the ClickHouse table `default.weather` via JDBC (`foreachBatch`).

---

## Tech Stack

| Component | Image / Version | Purpose |
|---|---|---|
| Apache Airflow | `apache/airflow:2.10.0-python3.11` | Orchestration + API ingestion |
| Apache Kafka | `confluentinc/cp-kafka:7.4.0` | Message broker |
| Zookeeper | `confluentinc/cp-zookeeper:7.4.0` | Kafka coordination |
| Apache Spark | `apache/spark:3.5.1-python3` | Stream processing |
| ClickHouse | `clickhouse/clickhouse-server:24.3` | Analytical data store |
| PostgreSQL | `postgres:13` | Airflow metadata DB |

---

## Prerequisites

- Docker Desktop (macOS/Linux/Windows) with at least **8 GB RAM** allocated
- Docker Compose v2
- A free **OpenWeatherMap API key** — https://home.openweathermap.org/api_keys
  - New keys can take 10–60 minutes to activate

---

## Project Structure

```
spark-clickhouse-streaming/
├── .env                          # API key + Airflow credentials (gitignored)
├── .gitignore
├── Dockerfile                    # Airflow image with extra pip deps
├── docker-compose.yml            # Full 8-container stack
├── requirements.txt              # Airflow-side Python deps
├── dags/
│   └── weather_2kafka.py         # Ingestion DAG: API → Kafka
├── jobs/
│   └── spark_streaming.py        # Spark: Kafka → ClickHouse
├── logs/                         # Airflow task logs (mounted)
└── plugins/                      # Airflow plugins (mounted)
```

---

## Setup

### 1. Clone and create directories

```bash
git clone <your-repo-url> spark-clickhouse-streaming
cd spark-clickhouse-streaming
mkdir -p dags jobs logs plugins
```

### 2. Create `.env`

```env
# OpenWeatherMap
OPENWEATHER_API_KEY=your_real_api_key_here

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 3. Add `.gitignore`

```gitignore
.env
logs/
plugins/
__pycache__/
*.pyc
.idea/
.vscode/
```

### 4. Build and start the stack

```bash
docker compose up -d --build
```

First build takes ~3–5 minutes (pulls images, installs pip deps).

### 5. Wait for bootstrap (~60 s)

Airflow runs DB migrations and creates the admin user on first boot. Check:

```bash
docker compose ps
```

All 8 containers should be `Up`. `airflow-webserver` will additionally become `(healthy)` after another ~30 s.

---

## Running the Pipeline

### Step 1 — Create the Kafka topic (once per fresh stack)

```bash
docker exec -it broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create --topic weather_data \
  --partitions 1 --replication-factor 1
```

### Step 2 — Start the Spark streaming job

Open a dedicated terminal and leave this running:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.6.3,com.clickhouse:clickhouse-client:0.6.3,com.clickhouse:clickhouse-http-client:0.6.3,com.clickhouse:clickhouse-data:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/spark_streaming.py
```

Wait for:

```
Spark session created.
ClickHouse table 'weather' ready (HTTP 200).
Kafka dataframe created.
```

The job then blocks on `awaitTermination()` — that's expected.

### Step 3 — Trigger the Airflow DAG

1. Open **http://localhost:8080**
2. Login: `airflow` / `airflow`
3. Unpause the **`weather_2kafka`** DAG
4. Click **▶ Trigger DAG**

The task runs for ~60 seconds, publishing ~12 Melbourne readings.

### Step 4 — Watch batches flow

In the Spark terminal you should see within ~15–30 s:

```
Batch 0 written to ClickHouse.
Batch 1 written to ClickHouse.
```

### Step 5 — Query ClickHouse

```bash
docker exec -it clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT city, weather_main, temp, humidity, wind_speed, toDateTime(dt) AS measured_at
           FROM default.weather
           ORDER BY dt DESC
           LIMIT 10"
```

Expected:

```
Melbourne  Clouds  14.3  72  1.5  2026-10-06 07:45:00
Melbourne  Clouds  14.2  73  1.4  2026-10-06 07:44:55
...
```

Or use the ClickHouse Play UI: **http://localhost:8123/play** (user `default`, password `clickhouse_pass`).

---

## Data Schema

The `default.weather` ClickHouse table (created automatically by the Spark job):

| Column | Type | Source |
|---|---|---|
| `record_id` | UUID | generated per record |
| `city` | String | `name` |
| `country` | String | `sys.country` |
| `latitude` | Float64 | `coord.lat` |
| `longitude` | Float64 | `coord.lon` |
| `weather_main` | String | `weather[0].main` |
| `weather_description` | String | `weather[0].description` |
| `temp` | Float64 | `main.temp` (°C) |
| `feels_like` | Float64 | `main.feels_like` |
| `temp_min` | Float64 | `main.temp_min` |
| `temp_max` | Float64 | `main.temp_max` |
| `pressure` | Int32 | `main.pressure` (hPa) |
| `humidity` | Int32 | `main.humidity` (%) |
| `wind_speed` | Float64 | `wind.speed` (m/s) |
| `wind_deg` | Int32 | `wind.deg` (meteorological) |
| `clouds` | Int32 | `clouds.all` (%) |
| `dt` | Int64 | Unix timestamp of measurement |

---

## Service Endpoints

| Service | URL / Address | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| Spark Master UI | http://localhost:9090 | — |
| Spark Application UI | http://localhost:4040 | — |
| ClickHouse HTTP | http://localhost:8123 | `default` / `clickhouse_pass` |
| ClickHouse Play UI | http://localhost:8123/play | `default` / `clickhouse_pass` |
| Kafka (from host) | `localhost:9092` | — |
| Kafka (intra-stack) | `broker:29092` | — |

---

## Useful Commands

```bash
# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f spark-master

# Inspect a Kafka topic
docker exec -it broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic weather_data --from-beginning --max-messages 5

# Check row count in ClickHouse
docker exec -it clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT count() FROM default.weather"

# Airflow task logs
docker exec -it airflow-scheduler airflow tasks states-for-dag-run weather_2kafka <run_id>

# Re-run the DAG for another 60 seconds of data
# → Airflow UI, click "Trigger DAG"
```

---

## Re-running the Pipeline

Once the stack is up:

```bash
# 1. Start Spark (if not running)
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.6.3,com.clickhouse:clickhouse-client:0.6.3,com.clickhouse:clickhouse-http-client:0.6.3,com.clickhouse:clickhouse-data:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/spark_streaming.py

# 2. Trigger the DAG from the Airflow UI
```

Kafka topic and ClickHouse table persist between runs (unless you `down -v`).

---

## Shutdown

```bash
# Keep data (containers stop, volumes remain)
docker compose down

# Full reset (wipes Postgres, ClickHouse, Ivy cache)
docker compose down -v
```

---

## Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `OPENWEATHER_API_KEY` empty in containers | `.env` is not in the same directory as `docker-compose.yml`. Run `docker exec airflow-scheduler printenv OPENWEATHER_API_KEY`. |
| Airflow DAG import error (`No module named 'kafka'`) | `requirements.txt` not baked in. Rebuild: `docker compose build --no-cache airflow-webserver airflow-scheduler`. |
| `could not translate host name "postgres"` | Missing `networks: - pipeline` on `x-airflow-common`. |
| ClickHouse returns `403` from Spark | `default` user has no password (Docker blocks remote access). Set `CLICKHOUSE_PASSWORD` in the `clickhouse` service. |
| `Provided Maven Coordinates must be in the form 'groupId:artifactId:version'` | Don't use the `:all` classifier with `--packages`. Use the individual coordinates or `--jars`. |
| `Failed to find data source: kafka` | `spark.jars.packages` in the script's `SparkSession` isn't resolved. Pass packages via `spark-submit --packages` instead. |
| Spark writes nothing | Verify Kafka has messages (see *Useful Commands*) and that the Spark terminal is still alive. |
| Port 8080 already in use | Another service (local Airflow, Jenkins, etc.). Change to `"8090:8080"` under `airflow-webserver`. |
| `airflow-webserver` exits with code 0 | Usually means DB unreachable. Check `docker compose logs airflow-webserver`. |

---

## Design Notes

- **Idempotent table creation** — The Spark job's `CREATE TABLE IF NOT EXISTS` is safe to re-run.
- **Checkpointing** — Spark writes stream state to `/tmp/checkpoint/weather`. Removing it forces a fresh offset read.
- **`foreachBatch` + JDBC** — Chosen over ClickHouse's native Spark connector for portability. The `clickhouse-jdbc` driver handles the HTTP insert path automatically.
- **Metric units** — The DAG requests `units=metric`, so temperatures are in °C and wind speed in m/s.
- **Fixed Melbourne coordinates** — `-37.8136, 144.9631`. To add more cities, expand the DAG to loop over a city list.

---

## License

MIT — for educational use.