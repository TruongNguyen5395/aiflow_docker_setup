from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def _function_a():
    return "Truong Day"

with DAG("dag_versioning_v_1_0", start_date=datetime(2020, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo 'TRUONG OI' & sleep 50"
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=_function_a
    )

    task_b = DummyOperator(
        task_id="task_b"
    )

    task_a >> task_c >> task_b