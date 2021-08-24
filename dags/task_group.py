from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime


def _function_a():
    return "Truong Day"


with DAG("task_group_ex", start_date=datetime(2020, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo 'TRUONG OI' & sleep 5"
    )

    with TaskGroup('processing_task') as processing_tasks:
        task_c = PythonOperator(
            task_id="task_c",
            python_callable=_function_a
        )

        with TaskGroup('sub_1_processing_task') as sub_1_processing_task:
            task_f = BashOperator(
                task_id="task_f",
                bash_command="echo 'TRUONG OI DOI TI' && sleep 20",
                priority_weight=2
            )

        with TaskGroup('sub_2_processing_task') as sub_2_processing_task:
            task_f = BashOperator(
                task_id="task_f",
                bash_command="echo 'TRUONG OI DOI TI - 2' && sleep 40",
                priority_weight=3
            )

        task_e = PythonOperator(
            task_id="task_e",
            python_callable=_function_a
        )

        task_c >> sub_1_processing_task >> task_e
        task_c >> sub_2_processing_task

    waiting_task_sub_1 = ExternalTaskSensor(
        task_id="task_sensor_wait_task_f",
        external_dag_id="task_group_ex",
        external_task_id="task_f"
    )

    task_d = DummyOperator(
        task_id="task_d"
    )

    task_a >> processing_tasks >> waiting_task_sub_1 >> task_d
