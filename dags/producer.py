from datetime import datetime

from airflow.decorators import task

from airflow import DAG, Dataset

file = Dataset("/tmp/file.txt")
file2 = Dataset("/tmp/file2.txt")


with DAG(
    "producer",
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as producer:

    @task(outlets=[file])
    def update():
        with open(file.uri, "a+", encoding="utf-8") as f:
            f.write("producer update")

    @task(outlets=[file2])
    def update2():
        with open(file2.uri, "a+", encoding="utf-8") as f:
            f.write("producer update")

    update() >> update2()
