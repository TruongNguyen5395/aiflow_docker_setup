from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime

with DAG("ex_dag_v_1_0", start_date=days_ago(5),
         schedule_interval="*/1 * * * *", catchup=False) as dag:

    task_a = BashOperator(
        owner="Truong",
        task_id="task_a",
        bash_command="echo 'task_a'"
    )

    task_b = BashOperator(
        owner="Nha",
        task_id="task_b",
        bash_command="echo 'task_b'"
    )

    task_a >> task_b