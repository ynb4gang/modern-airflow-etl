from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import json

DAG_ID = "crypto_prices_etl"

COINS = ["bitcoin", "ethereum", "cardano"]
CURRENCY = "usd"

def fetch_crypto_data(**context):
    """Fetch crypto prices from CoinGecko API into staging table"""
    all_data = []
    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency={CURRENCY}&days=1"
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()["prices"]
                df = pd.DataFrame(data, columns=["timestamp", "price"])
                df["coin"] = coin
                all_data.append(df)
                time.sleep(5)
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    print(f"Rate limit hit for {coin}, sleeping 10s...")
                    time.sleep(10)
                else:
                    raise

    df_all = pd.concat(all_data, ignore_index=True)
    df_all.drop_duplicates(inplace=True)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], unit="ms")

    context["ti"].xcom_push(key="crypto_raw", value=df_all.to_json(orient="records"))

    hook = PostgresHook(postgres_conn_id="main_postgres")
    engine = hook.get_sqlalchemy_engine()
    df_all.to_sql(
        "crypto_staging",
        engine,
        if_exists="replace",
        index=False
    )

def transform_crypto_data(**context):
    """Aggregate data from staging table"""
    hook = PostgresHook(postgres_conn_id="main_postgres")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql("SELECT * FROM crypto_staging", engine)

    agg = df.groupby("coin").agg(
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()

    agg["processed_at"] = datetime.utcnow()

    context["ti"].xcom_push(key="crypto_agg", value=agg.to_json(orient="records"))

def load_crypto_to_postgres(**context):
    """Load aggregated data into final table"""
    ti = context["ti"]
    df = pd.DataFrame(json.loads(ti.xcom_pull(key="crypto_agg", task_ids="transform_crypto_data")))

    df.columns = [c.lower() for c in df.columns]

    hook = PostgresHook(postgres_conn_id="main_postgres")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        "crypto_aggregated",
        engine,
        if_exists="replace",
        index=False
    )

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 12, 11),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["crypto", "api", "etl"]
) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        postgres_conn_id="main_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS crypto_staging (
            timestamp TIMESTAMP,
            price FLOAT,
            coin TEXT
        );
        """
    )

    create_final_table = PostgresOperator(
        task_id="create_final_table",
        postgres_conn_id="main_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS crypto_aggregated (
            coin TEXT PRIMARY KEY,
            avg_price FLOAT,
            min_price FLOAT,
            max_price FLOAT,
            processed_at TIMESTAMP
        );
        """
    )

    fetch_staging = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_crypto_data,
        retries=3,
        retry_delay=timedelta(seconds=30)
    )

    transform = PythonOperator(
        task_id="transform_crypto_data",
        python_callable=transform_crypto_data
    )

    load_final = PythonOperator(
        task_id="load_crypto_to_postgres",
        python_callable=load_crypto_to_postgres
    )

    create_staging_table >> fetch_staging
    create_final_table >> transform
    fetch_staging >> transform >> load_final
