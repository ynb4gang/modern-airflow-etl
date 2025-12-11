from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
import json

DAG_ID = "api_to_postgres_pipeline_v2"

COOLDOWN_API_RETRIES = 3

def fetch_api_data(**context):
    url = "https://jsonplaceholder.typicode.com/comments"
    for attempt in range(COOLDOWN_API_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            df.drop_duplicates(inplace=True)
            df.dropna(subset=["postId", "id", "email", "body"], inplace=True)
            context["ti"].xcom_push(key="raw_df", value=df.to_json(orient="records"))
            return
        except Exception as e:
            if attempt < COOLDOWN_API_RETRIES - 1:
                continue
            else:
                raise e

def transform_data(**context):
    ti = context["ti"]
    raw_json = ti.xcom_pull(key="raw_df", task_ids="fetch_api_data")
    df = pd.DataFrame(json.loads(raw_json))

    df["body_length"] = df["body"].str.len()

    aggregated = df.groupby("postId").agg(
        comments_count=("id", "count"),
        unique_emails=("email", pd.Series.nunique),
        avg_body_length=("body_length", "mean"),
        max_body_length=("body_length", "max"),
        min_body_length=("body_length", "min")
    ).reset_index()

    aggregated["processed_at"] = datetime.utcnow()

    ti.xcom_push(key="agg_df", value=aggregated.to_json(orient="records"))

def load_to_postgres(**context):
    ti = context["ti"]
    agg_json = ti.xcom_pull(key="agg_df", task_ids="transform_data")
    df = pd.DataFrame(json.loads(agg_json))

    # Все имена колонок в lower case для Postgres
    df.columns = [c.lower() for c in df.columns]

    hook = PostgresHook(postgres_conn_id="main_postgres")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        "comments_aggregated",
        engine,
        if_exists="replace",
        index=False
    )

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["example", "postgres", "api", "enhanced"]
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="main_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS comments_aggregated (
                postid INTEGER PRIMARY KEY,
                comments_count INTEGER,
                unique_emails INTEGER,
                avg_body_length FLOAT,
                max_body_length INTEGER,
                min_body_length INTEGER,
                processed_at TIMESTAMP
            );
        """
    )

    fetch_api = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_pg = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    create_table >> fetch_api >> transform >> load_pg
