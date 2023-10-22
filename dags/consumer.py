from datetime import datetime

from airflow.decorators import task

from airflow import DAG, Dataset

file = Dataset("/tmp/file.txt")
file2 = Dataset("/tmp/file2.txt")

with DAG(
    "consumer",
    schedule=[file, file2],
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as consumer:

    @task()
    def consume():
        with open(file.uri, "r", encoding="utf-8") as f:
            print(f.read())

    consume()
