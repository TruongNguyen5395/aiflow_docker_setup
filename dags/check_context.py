from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def my_func(**context):
    print("TRUONG OI:", context)


def execution_date_context(ds):
    print("TRUONG OI:", ds)



with DAG('check_context_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    my_task = PythonOperator(
        task_id='check_context_task',
        python_callable=execution_date_context
    )