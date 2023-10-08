import json
import logging
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import json_normalize

from airflow import DAG


def _process_user(ti):
    users = ti.xcom_pull(
        task_ids=["extract_user"],
    )
    user = users[0]["results"][0]

    processed_user = json_normalize(
        {
            "firstname": user["name"]["first"],
            "lastname": user["name"]["last"],
            "country": user["location"]["country"],
            "username": user["login"]["username"],
            "password": user["login"]["password"],
            "email": user["email"],
        }
    )
    processed_user.to_csv("/tmp/processed_user.csv", index=None, header=False)


def _store_user():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.copy_expert(
        sql="COPY users FROM STDIN WITH (FORMAT csv, DELIMITER ',')",
        filename="/tmp/processed_user.csv",
    )
    logging.info("Successfully stored user data.")


with DAG(
    "user_processing",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL
        );
        """,
    )
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="/api",
        timeout=60,  # timeout after 1 minute
        poke_interval=10,  # poke every 10 seconds
    )

    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="/api",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user,
    )

    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user,
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user
