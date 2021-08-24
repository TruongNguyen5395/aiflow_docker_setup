from airflow.models import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

with DAG("my_bash_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    execute_command = BashOperator(
        task_id="execute_command",
        bash_command="scripts/commands.sh",
        skip_exit_code=77,
        do_xcom_push=False,
        env={
            "api_aws": "{{ var.value.api_key_aws }}"
        }
    )