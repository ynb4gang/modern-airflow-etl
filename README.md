# Airflow ETL Project: API to Postgres

## Project Overview
This project implements ETL pipelines using Apache Airflow, Postgres, and public APIs. The goal is to demonstrate building ETL workflows with a **staging → transform → load** structure, API data aggregation, and error handling.

---

## Project Structure
```
airflow/
├─ dags/
│  ├─ api_comments_etl_v2.py          # ETL DAG for JSONPlaceholder comments API
│  ├─ crypto_prices_etl.py            # ETL DAG for CoinGecko cryptocurrency API
├─ logs/                              # Airflow DAG execution logs
├─ docker-compose.yaml                # Docker Compose for Airflow + Postgres + PgAdmin
├─ README.md                          # This file
```

---

## Docker Compose
The project uses Docker Compose to run the full stack:

- **Airflow Webserver:** http://localhost:8080  
- **PgAdmin:** http://localhost:5050  
- **Postgres:** Database for Airflow + ETL

Example Postgres service:
```yaml
services:
  postgres-airflow:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow_db
    ports:
      - "5432:5432"
```

### Airflow Connection to Postgres

```
Connection ID: main_postgres
Connection Type: Postgres
Host: postgres-airflow
Schema: airflow_db
Login: airflow
Password: airflow
Port: 5432
```

---

## DAG 1: API Comments ETL

**File:** `dags/api_comments_etl_v2.py`

### Description
- Fetches comments from `https://jsonplaceholder.typicode.com/comments`
- Stores data in staging table **comments_staging**
- Computes aggregated metrics:
  - `comments_count` — number of comments per postId  
  - `unique_emails` — unique email addresses per postId  
  - `avg_body_length`, `min_body_length`, `max_body_length` — comment text length stats
- Loads results into **comments_aggregated**

### Table Schema

```sql
CREATE TABLE IF NOT EXISTS comments_staging (
    postId INTEGER,
    id INTEGER,
    name TEXT,
    email TEXT,
    body TEXT
);

CREATE TABLE IF NOT EXISTS comments_aggregated (
    postId INTEGER PRIMARY KEY,
    comments_count INTEGER,
    unique_emails INTEGER,
    avg_body_length FLOAT,
    min_body_length INTEGER,
    max_body_length INTEGER,
    processed_at TIMESTAMP
);
```

### DAG Workflow
```
create_staging_table >> fetch_api_data >> transform_data >> load_to_postgres
```

---

## DAG 2: Crypto Prices ETL

**File:** `dags/crypto_prices_etl.py`

### Description
- Fetches crypto price data from CoinGecko API for:
  - bitcoin  
  - ethereum  
  - cardano  
- Stores raw data in **crypto_staging**
- Aggregates:
  - `avg_price`, `min_price`, `max_price`
  - `processed_at` timestamp
- Loads into **crypto_aggregated**

### Table Schema

```sql
CREATE TABLE IF NOT EXISTS crypto_staging (
    timestamp TIMESTAMP,
    price FLOAT,
    coin TEXT
);

CREATE TABLE IF NOT EXISTS crypto_aggregated (
    coin TEXT PRIMARY KEY,
    avg_price FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    processed_at TIMESTAMP
);
```

### DAG Workflow
```
create_staging_table >> fetch_crypto_data >> transform_crypto_data >> load_crypto_to_postgres
```

---

## Features
- Handles API rate limits  
- Retry mechanism for Airflow PythonOperator tasks  
- Lowercase column names for Postgres compatibility  
- Robust staging → transform → load flow  
- `processed_at` timestamps for audit  

---

## How to Run

### 1. Clone repository and move to project folder
```bash
git clone https://github.com/ynb4gang/modern-airflow-etl.git
```
### 2. Change into the project directory
```bash
cd modern-airflow-etl
```
### 3. Start services:
```bash
docker-compose up -d
```

### 3. Verify services:
- Airflow: http://localhost:8080  
- PgAdmin: http://localhost:5050  
- Postgres: `localhost:5432`

### 4. Activate DAGs in the Airflow UI and trigger manually or wait for schedules.

---

## Airflow Connection: main_postgres

```
Host: postgres-airflow
Port: 5432
Schema: airflow_db
Login: airflow
Password: airflow
Type: Postgres
```

---

## Example SQL Queries

### Top 5 posts with the most comments
```sql
SELECT postid, comments_count
FROM comments_aggregated
ORDER BY comments_count DESC
LIMIT 5;
```

### Latest aggregated cryptocurrency prices
```sql
SELECT *
FROM crypto_aggregated;
```

