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


with DAG("usr_ltv_prediction_v3.2.2", default_args=default_args, start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=True) as dag:

    extract_a = BashOperator(
        owner="Truong",
        task_id="extract_from_BigQuery",
        bash_command="echo 'task_a' && sleep 10",
        wait_for_downstream=True,
        execution_timeout=timedelta(seconds=15),
        # on_success_callback=_extract_a_on_success,
        on_failure_callback=_extract_a_on_failure,
        # task_concurrency=1
    )

    extract_b = BashOperator(
        owner="Truong",
        task_id="Craw_FB",
        bash_command="echo 'task_a' && sleep 7",
        wait_for_downstream=True,
        on_failure_callback=_extract_a_on_failure,
        # task_concurrency=1
    )

    process_a = BashOperator(
        owner="Truong",
        task_id="cleaning_bias_user",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        bash_command="echo '{{ ti.priority_weight }}' && exit 0",
        pool="process_tasks",
        do_xcom_push=True
    )

    process_b = BashOperator(
        task_id="Merge_user_job",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        bash_command="echo '{{ ti.priority_weight }}' && exit 0",
        pool="process_tasks",
        priority_weight=2,
        do_xcom_push=True
    )

    process_c = BashOperator(
        task_id="predict_user_behavior",
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
        task_id='clean_data_bias_user',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    clean_b = BashOperator(
        task_id='clean_data_job',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    clean_c = BashOperator(
        task_id='clean_prediction',
        bash_command='echo "clean process_a"',
        trigger_rule="all_failed"
    )

    store = PythonOperator(
        task_id="store_to_AWS_Redshift",
        python_callable=_my_func,
        depends_on_past=True
    )

    cross_downstream([extract_a, extract_b], [process_a, process_b, process_c])
    chain([process_a, process_b, process_c], [clean_a, clean_b, clean_c])
    cross_downstream([process_a, process_b, process_c], [store])
