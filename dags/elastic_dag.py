from datetime import datetime

from airflow.operators.python import PythonOperator
from hooks.elastic.elastic_hook import ElasticHook

from airflow import DAG


def _print_es_info():
    hook = ElasticHook()
    print(hook.info())


def _add_doc(ti):
    hook = ElasticHook()
    doc = {"name": "test"}
    print(hook.add_doc(index="test", id=1, doc=doc))
    ti.xcom_push(key="doc_id", value=1)


def _get_doc(ti):
    hook = ElasticHook()
    doc_id = ti.xcom_pull(task_ids="add_doc", key="doc_id")

    print(hook.get_doc(index="test", doc_id=doc_id))


with DAG(
    "elastic_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    print_es_info = PythonOperator(
        task_id="print_es_info",
        python_callable=_print_es_info,
    )

    add_doc = PythonOperator(
        task_id="add_doc",
        python_callable=_add_doc,
    )

    get_doc = PythonOperator(
        task_id="get_doc",
        python_callable=_get_doc,
    )

    print_es_info >> add_doc >> get_doc
