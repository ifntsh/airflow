from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
}

dag = DAG(
    'simple_hourly_logging',  # DAG ID
    default_args=default_args,
    schedule_interval='@hourly',  # 매 시간 실행
    catchup=False,  # 과거 실행 여부
    tags=['example']
)

# 실행될 Python 함수 정의
def log_current_time():
    current_time = datetime.now().time()
    print(f"Current time is {current_time}")

# DAG에 포함될 태스크 정의
task_log_time = PythonOperator(
    task_id='log_current_time',
    python_callable=log_current_time,
    dag=dag,
)

# 태스크 간의 의존성 설정
task_log_time

if __name__ == "__main__":
    dag.cli()
