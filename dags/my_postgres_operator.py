from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')


# Puss value to XCOM
def _my_task():
    return 'tweets.csv'


with DAG('my_postgres_dag', schedule_interval='@daily',
         start_date=datetime(2021, 1, 1), catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='sql/CREATE_TABLE_MY_TABLE.sql'
    )

    my_task = PythonOperator(
        task_id="my_task",
        python_callable=_my_task
    )

    store = CustomPostgresOperator(
        task_id='store',
        postgres_conn_id='postgres',
        sql=["sql/INSERT_TO_MY_TABLE.sql",
             "SELECT * FROM my_table"
             ],
        parameters={
            'filename': '{{ ti.xcom_pull(task_ids=["my_task"])[0] }}'
        }
    )