# Ecommerce Data Platform

Beginner-friendly batch data engineering project using Spark, Hive, and Airflow.

## What this project does

Raw CSV -> Spark ETL -> Parquet + Hive table -> Airflow automation -> SQL analytics.

## Project Structure

- `data/raw/` - source CSV files
- `data/processed/` - cleaned Spark output
- `spark_jobs/` - executable Spark entry scripts
- `dags/` - Airflow DAGs
- `hive/conf/` - Hive configuration
- `logs/` - Airflow logs

## Main Files

- `data/raw/orders.csv` - sample orders data
- `spark_jobs/orders_etl.py` - runs the ETL job
- `dags/orders_etl_dag.py` - Airflow pipeline
- `docker-compose.yml` - local stack

## What gets created when the pipeline runs

- Cleaned Parquet output in `data/processed/orders/`
- Hive database `ecommerce`
- Hive table `ecommerce.orders_clean`

## Step-by-Step Run Guide

### 1. Start the stack

From the project root:

```powershell
docker compose down
docker compose up --build -d
docker compose ps
```

Expected:
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-postgres`
- `spark-master`
- `spark-worker`
- `hive-metastore`

### 2. Run the ETL manually

```powershell
docker compose exec airflow-webserver python /opt/workspace/spark_jobs/orders_etl.py
```

Expected:
- CSV is read
- duplicates are removed
- `total_amount` is created
- `order_date` is created
- Parquet is written
- Hive table is saved
- `ETL completed successfully`

### 3. Query Hive manually

After the ETL runs, open a Spark shell or use PySpark to run:

```python
spark.sql("""
SELECT
    customer_id,
    SUM(total_amount) AS revenue
FROM ecommerce.orders_clean
GROUP BY customer_id
ORDER BY revenue DESC
""").show()
```

Expected:
- Hive database is created
- Hive table is written
- revenue by customer is printed

### 4. Open Airflow

Open:

```text
http://localhost:8081
```

Login:
- username: `admin`
- password: `admin`

Then:
- enable `orders_etl_dag`
- trigger it manually
- check task logs

## How to check outputs

### Raw input

Open:
- `data/raw/orders.csv`

### Processed files

Check:

```powershell
Get-ChildItem ecommerce-data-platform\data\processed\orders
```

You should see:
- `part-*.parquet`
- `_SUCCESS`

## Beginner Notes

- Use notebooks for experimenting.
- Use `.py` files for reusable pipeline code.
- Use Airflow for automation.

## ETL code (easy-to-read modules)

For a simpler view of the ETL steps, the Spark job is split into small modules:

- `retail_etl/extract.py` - create Spark session + read CSV
- `retail_etl/transform.py` - cleaning + derived columns
- `retail_etl/load.py` - write Parquet + write Hive table

The orchestrating entry script is still:
- `spark_jobs/orders_etl.py`

## Phase 2: Warehouse (fact + dimensions)

Phase 2 adds multiple source tables and builds a simple star schema:

- Raw inputs:
  - `data/raw/orders.csv`
  - `data/raw/customers.csv`
  - `data/raw/products.csv`
- Spark warehouse job: `spark_jobs/warehouse_etl.py`
- Airflow DAG: `dags/warehouse_pipeline.py`

### Run the warehouse ETL manually

```powershell
docker compose exec airflow-webserver python /opt/workspace/spark_jobs/warehouse_etl.py
```

Expected:
- `orders` is cleaned and joined to `customers` + `products` (broadcast join for products)
- partitioned Parquet is written under `data/processed/`
- Hive tables are created:
  - `ecommerce.fact_orders` (partitioned by `order_date`)
  - `ecommerce.dim_customers`
  - `ecommerce.dim_products`

### Phase 2 processed folder layout

- `data/processed/fact_orders/run_id=.../order_date=.../` (partitioned by `order_date`)
- `data/processed/dimensions/dim_customers/run_id=.../`
- `data/processed/dimensions/dim_products/run_id=.../`
- `data/processed/orders/orders_clean/run_id=.../` (optional debugging output)

## Phase 3: Real-Time Streaming and Lakehouse

Phase 3 adds enterprise-grade streaming, Delta Lake, Kafka, and monitoring.

### New Stack Components

- `kafka` and `zookeeper` added to `docker-compose.yml`
- `prometheus` and `grafana` added for monitoring
- `delta-spark` added to `requirements.txt`
- `kafka-python` added for producing events

### New files

- `spark_jobs/streaming_pipeline.py` — Spark Structured Streaming medallion pipeline (bronze/silver/gold)
- `spark_jobs/delta_merge_orders.py` — Delta Lake merge/upsert example for CDC
- `dags/streaming_pipeline_dag.py` — Airflow streaming DAG with sensors, task groups, SLA, retries, and backfill support
- `streaming/order_producer.py` — Kafka producer for order events
- `streaming/validate_streams.py` — simple Delta output validation
- `monitoring/prometheus.yml` — Prometheus scrape configuration
- `.github/workflows/ci.yml` — GitHub Actions CI for syntax checks

### Phase 3 architecture

The new end-to-end flow is:

```text
Website/App Events
        ↓
Kafka Streaming
        ↓
Spark Structured Streaming
        ↓
Bronze Delta
        ↓
Silver Delta
        ↓
Gold Aggregations
        ↓
Hive / Delta Tables
        ↓
Airflow Orchestration
        ↓
Analytics Dashboard
```

### Start the full stack

```powershell
docker compose down
docker compose up --build -d
docker compose ps
```

Expected new services:
- `zookeeper`
- `kafka`
- `prometheus`
- `grafana`

### Create and produce streaming events

From the host or inside the Airflow container:

```powershell
docker compose exec airflow-webserver python /opt/workspace/streaming/order_producer.py
```

This sends sample order events to the Kafka `orders` topic.

### Run the streaming pipeline

```powershell
docker compose exec airflow-webserver python /opt/workspace/spark_jobs/streaming_pipeline.py --duration 120
```

This Spark job uses Spark package downloads for Kafka and Delta and writes the Ivy cache into `/tmp/.ivy2` inside the container.

This Spark job:
- reads streaming events from Kafka
- writes raw events to Delta bronze
- cleans and enriches events to Delta silver
- performs windowed gold aggregations and materializes a Delta gold path
- registers `ecommerce.orders_gold` as a Delta Hive table

### Run the Airflow streaming DAG

Open Airflow at `http://localhost:8081` and enable `streaming_pipeline`.

The DAG includes:
- a Kafka readiness sensor
- dynamic streaming tasks grouped in a TaskGroup
- retry policy and SLA monitoring
- backfill-friendly hourly schedule

### Run Delta Lake merge/upsert demo

```powershell
docker compose exec airflow-webserver python /opt/workspace/spark_jobs/delta_merge_orders.py
```

This demonstrates a lakehouse CDC pattern with Delta merge/upsert.

### Monitoring

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Note: Prometheus and Grafana are available as a monitoring stack. Spark and Airflow endpoints are configured as scrape targets in `monitoring/prometheus.yml`.
