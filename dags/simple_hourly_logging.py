from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_hourly_logging_with_slack',
    default_args=default_args,
    schedule_interval='@hourly',  # 매 시간 실행
    catchup=False,  # 과거 실행 여부
    tags=['example']
)

# 실행될 Python 함수 정의
def log_current_time(**context):
    current_time = datetime.now().time()
    message = f"Current time is {current_time}"
    print(message)
    return message

# PythonOperator 태스크 정의
task_log_time = PythonOperator(
    task_id='log_current_time',
    python_callable=log_current_time,
    provide_context=True,
    dag=dag,
)

# SlackWebhookOperator 태스크 정의
slack_alert = SlackWebhookOperator(
    task_id='send_slack_message',
    slack_webhook_conn_id='slack_webhook',  # Airflow connection 설정
    message="{{ task_instance.xcom_pull(task_ids='log_current_time') }}",
    dag=dag,
)

# 태스크 간의 의존성 설정
task_log_time >> slack_alert

if __name__ == "__main__":
    dag.cli()
