from airflow import DAG
from airflow.operators.dummy import DummyOperator

from datetime import datetime

with DAG("dag_versioning_v_1_0", start_date=datetime(2020, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    task_a = DummyOperator(
        task_id="task_a"
    )

    task_c = DummyOperator(
        task_id="task_c"
    )

    task_b = DummyOperator(
        task_id="task_b"
    )

    task_a >> task_c >> task_b