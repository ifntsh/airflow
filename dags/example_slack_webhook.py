from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import os
from datetime import datetime

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "slack_webhook_example_dag"
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
IMAGE_URL = "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png"

def log_current_time(**context):
    current_time = datetime.now().time()
    message = f":slack: UTC time is {current_time}"
    print(message)
    return message

with DAG(
    dag_id=DAG_ID,
    schedule_interval='@hourly',  # 매 시간 실행
    start_date=datetime(2024, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["example"],
) as dag:
    # 로그 메시지를 생성하는 PythonOperator
    generate_log_message = PythonOperator(
        task_id='generate_log_message',
        python_callable=log_current_time,
        provide_context=True,
    )

    # [START slack_webhook_operator_text_howto_guide]
    slack_webhook_operator_text = SlackWebhookOperator(
        task_id="slack_webhook_send_text",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='generate_log_message') }}",
    )
    # [END slack_webhook_operator_text_howto_guide]

    generate_log_message >> slack_webhook_operator_text
