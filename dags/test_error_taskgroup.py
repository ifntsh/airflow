import importlib
import sys

from airflow import DAG
from airflow import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def success_test(**context):
    task_instance = context['task_instance']
    task_instance.xcom_push("ErrorSample", "Test value from test")
    task_instance.xcom_push("ErrorSample0", "00000000000000000000")
    task_instance.xcom_push("ErrorSample2", "Test value from test2222")
    raise AirflowException("Test error from test")

with DAG(
    dag_id="test_error_taskgroup",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    start_task = BashOperator(
        task_id="dag_start",
        bash_command="echo dag_start",
        dag=dag
    )
    end_task = BashOperator(
        task_id="dag_end",
        trigger_rule="none_failed_min_one_success",
        bash_command="echo dag_end",
        dag=dag
    )
    print_hello = PythonOperator(
        task_id="print_hello",
        provide_context=True,
        python_callable=success_test,
        dag=dag
    )

    start_task >> print_hello >> end_task