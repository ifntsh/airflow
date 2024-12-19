from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ELT 함수 정의
def extract_and_load_from_api():
    print("Extracting and Loading data from API to the warehouse...")

def extract_and_load_from_csv():
    print("Extracting and Loading data from CSV to the warehouse...")

def transform_in_warehouse():
    print("Transforming data in the warehouse using SQL...")

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='elt_sales_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Extract & Load Tasks
    load_from_api_task = PythonOperator(
        task_id='extract_and_load_api',
        python_callable=extract_and_load_from_api,
    )

    load_from_csv_task = PythonOperator(
        task_id='extract_and_load_csv',
        python_callable=extract_and_load_from_csv,
    )

    # Transform Task
    transform_task = PythonOperator(
        task_id='transform_in_warehouse',
        python_callable=transform_in_warehouse,
    )

    # Task Dependencies
    [load_from_api_task, load_from_csv_task] >> transform_task
