from airflow.models import DAG
from airflow.models import Variable

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.mongo.sensors.mongo import MongoSensor

from datetime import datetime
import json


def _get_last_success_date(prev_execution_date_success):
    if isinstance(prev_execution_date_success, datetime):
        # return json.dumps(prev_execution_date_success.isoformat())
        # return prev_execution_date_success.strftime("%Y-%m-%d")
        Variable.set('latest_date', datetime.strptime(str(prev_execution_date_success), "%Y-%m-%dT%H:%M:%S%f%z").strftime('%Y-%m-%d'))
    else:
        # return json.dumps(datetime(2021, 5, 18).isoformat())
        Variable.set('latest_date', datetime(2021, 5, 18).strftime('%Y-%m-%d'))



with DAG("mongo_check", start_date=datetime(2021, 8, 15),
         schedule_interval="*/5 * * * *", catchup=False) as dag:

    check_date = PythonOperator(
        task_id="check_date",
        python_callable=_get_last_success_date,
    )

    Check_MongoDB = MongoSensor(
        task_id="mongo_sensor",
        mongo_conn_id="weatherdb_mongo_connection",
        collection="weather",
        # query={'timestamp': {"$gte": "{{ ti.xcom_pull(task_ids='check_date', key='return_value') }}"}},
        query={'timestamp': {"$gte": datetime.strptime(Variable.get('latest_date'), '%Y-%m-%d')}},
        poke_interval=10,
    )

    check_date >> Check_MongoDB


