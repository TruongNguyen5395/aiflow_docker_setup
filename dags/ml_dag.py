from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime
import yaml

default_args = {
    'start_date': datetime(2021, 1, 1)
}


def _check_holidays(ds):
    with open('ex-dag/files/day_off.yml', 'r') as f:
        day_off = set(yaml.load(f, Loader=yaml.FullLoader))
        if ds not in day_off:
            return 'process'
    return 'stop'


with DAG('ml_dag', schedule_interval="@daily",
         default_args=default_args, catchup=False) as dag:

    check_holidays = BranchPythonOperator(
        task_id="check_holidays",
        python_callable=_check_holidays
    )

    process = DummyOperator(task_id='process')
    cleaning_stock = DummyOperator(task_id="cleaning_stock")
    stop = DummyOperator(task_id='stop')

    check_holidays >> [process, stop]
    process >> cleaning_stock
