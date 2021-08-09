from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


def _task_a():
    print("TRUONG OI")


@task(task_id="task_a")
def process(my_settings):
    context = get_current_context()
    print(f"{my_settings['path']}/{my_settings['filename']} - {context['ds']}")


with DAG('my_python_dag', start_date=datetime(2021, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    store = BashOperator(
        task_id="store",
        bash_command="echo 'store'"
    )

    process(Variable.get('my_setting', deserialize_json=True)) >> store
