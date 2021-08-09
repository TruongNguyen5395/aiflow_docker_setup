from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream, chain
from airflow.exceptions import AirflowTaskTimeout

from datetime import datetime, timedelta

default_args = {
        "email": ['1450165@hcmut.edu.vn'],
        "email_on_retry": False,
        "email_on_failure": True,
}


def _my_func(ti, execution_date):
    xcoms = ti.xcom_pull(task_ids=['process_a', 'process_b', 'process_c'], key="return_value")
    print(xcoms)
    if execution_date.day == 5:
        raise ValueError("Error")


def _extract_a_on_success(context):
    print(context)


def _extract_a_on_failure(context):
    if (isinstance(context['exception'], AirflowTaskTimeout)):
        print('The task timed out')
    else:
        print('Other error')


def _extract_b_on_failure(context):
    if (isinstance(context['exception'], AirflowTaskTimeout)):
        print('The task timed out')
    else:
        print('Other error')


with DAG("ex_dag_v_1_0", default_args=default_args, start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=True) as dag:

    extract_a = BashOperator(
        owner="Truong",
        task_id="extract_a",
        bash_command="echo 'task_a' && sleep 10",
        wait_for_downstream=True,
        execution_timeout=timedelta(seconds=15),
        # on_success_callback=_extract_a_on_success,
        on_failure_callback=_extract_a_on_failure,
        # task_concurrency=1
    )

    extract_b = BashOperator(
        owner="Truong",
        task_id="extract_b",
        bash_command="echo 'task_a' && sleep 7",
        wait_for_downstream=True,
        on_failure_callback=_extract_a_on_failure,
        # task_concurrency=1
    )

    process_a = BashOperator(
        owner="Nha",
        task_id="process_a",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        bash_command="echo '{{ ti.priority_weight }}' && exit 0",
        pool="process_tasks",
        do_xcom_push=True
    )

    process_b = BashOperator(
        owner="Nha",
        task_id="process_b",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        bash_command="echo '{{ ti.priority_weight }}' && exit 0",
        pool="process_tasks",
        priority_weight=2,
        do_xcom_push=True
    )

    process_c = BashOperator(
        owner="Nha",
        task_id="process_c",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        bash_command="echo '{{ ti.xcom_push(key='truong_xcom', value='TRUONG OI') }}' && sleep 20",
        pool="process_tasks",
        priority_weight=3,
        do_xcom_push=True
        # weight_rule='downstream'
    )

    clean_a = BashOperator(
        owner="Bo&Me",
        task_id='clean_a',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    clean_b = BashOperator(
        owner="Bo&Me",
        task_id='clean_b',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    clean_c = BashOperator(
        owner="Bo&Me",
        task_id='clean_c',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    store = PythonOperator(
        owner="Xin&Bon",
        task_id="store",
        python_callable=_my_func,
        depends_on_past=True
    )

    cross_downstream([extract_a, extract_b], [process_a, process_b, process_c])
    chain([process_a, process_b, process_c], [clean_a, clean_b, clean_c])
    cross_downstream([process_a, process_b, process_c], [store])
