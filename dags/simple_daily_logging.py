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
    'simple_daily_logging',
    default_args=default_args,
    schedule_interval='@daily',  # 매일 실행
    catchup=False,  # 과거 실행 여부
    tags=['example']
)

# 실행될 Python 함수 정의
def log_today_date():
    today = datetime.now().date()
    print(f"Today's date is {today}")

# DAG에 포함될 태스크 정의
task_log_date = PythonOperator(
    task_id='log_today_date',
    python_callable=log_today_date,
    dag=dag,
)

# 태스크 간의 의존성 설정
task_log_date

if __name__ == "__main__":
    dag.cli()
