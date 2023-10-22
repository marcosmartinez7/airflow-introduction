from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow import DAG


def _t1(ti):
    ti.xcom_push(key="my_key", value="my_value")


def _t2(ti):
    value = ti.xcom_pull(key="my_key", task_ids=["t1"])
    print(value)


def _branch(ti):
    value = ti.xcom_pull(key="my_key", task_ids=["t1"])
    if value == "my_value":
        return "t3"
    else:
        return "t2"


with DAG(
    "xcom_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="t1", python_callable=_t1)

    branch = BranchPythonOperator(task_id="branch", python_callable=_branch)

    t2 = PythonOperator(task_id="t2", python_callable=_t2)

    t3 = BashOperator(task_id="t3", bash_command="echo ''")

    t4 = BashOperator(task_id="t4", bash_command="echo ''", trigger_rule="all_done")

    t1 >> branch >> [t2, t3] >> t4
