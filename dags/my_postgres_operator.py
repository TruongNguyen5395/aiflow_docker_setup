from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


with DAG('my_postgres_dag', schedule_interval='@daily',
         start_date=datetime(2021, 1, 1), catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='CREATE TABLE my_table (table_value TEXT NOT NULL, PRIMARY KEY (table_value));'
    )

    store = PostgresOperator(
        task_id='store',
        postgres_conn_id='postgres',
        sql="INSERT INTO my_table VALUES ('my_value')"
    )