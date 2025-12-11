from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
import pandas as pd
import requests


DAG_ID = "crypto_prices_etl"

COINS = ["bitcoin", "ethereum", "cardano"]
CURRENCY = "usd"


def fetch_crypto_data(**context):
    all_data = []
    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency={CURRENCY}&days=1"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()["prices"]
        df = pd.DataFrame(data, columns=["timestamp", "price"])
        df["coin"] = coin
        all_data.append(df)
    
    df_all = pd.concat(all_data, ignore_index=True)
    context["ti"].xcom_push(key="crypto_raw", value=df_all.to_json(orient="records"))


def transform_crypto_data(**context):
    import json
    ti = context["ti"]
    df = pd.DataFrame(json.loads(ti.xcom_pull(key="crypto_raw", task_ids="fetch_crypto_data")))

    agg = df.groupby("coin").agg(
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()

    ti.xcom_push(key="crypto_agg", value=agg.to_json(orient="records"))


def load_crypto_to_postgres(**context):
    import json
    ti = context["ti"]
    df = pd.DataFrame(json.loads(ti.xcom_pull(key="crypto_agg", task_ids="transform_crypto_data")))

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

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="main_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS crypto_aggregated (
                coin TEXT,
                avg_price FLOAT,
                min_price FLOAT,
                max_price FLOAT
            );
        """
    )

    fetch = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_crypto_data
    )

    transform = PythonOperator(
        task_id="transform_crypto_data",
        python_callable=transform_crypto_data
    )

    load = PythonOperator(
        task_id="load_crypto_to_postgres",
        python_callable=load_crypto_to_postgres
    )

    create_table >> fetch >> transform >> load
