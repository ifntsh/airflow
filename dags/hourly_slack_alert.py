from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import os
from datetime import datetime, time
import pytz

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "hourly_slack_alert"  # DAG ID 변경
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
KST = pytz.timezone('Asia/Seoul')

def log_current_time(**context):
    now_utc = datetime.now()
    now_kst = datetime.now(KST)
    message = f":slack: UTC time is {now_utc.time()}, KST time is {now_kst.time()}"
    print(message)
    return message

def task_fail_slack_alert(context):
    slack_msg = f"""
    :red_circle: Task Failed.
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {context.get('task_instance').log_url}
    """
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_failed',
        http_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=slack_msg,
        username='airflow',
    )
    
    return failed_alert.execute(context=context)

def is_weekday_working_hours():
    now_kst = datetime.now(KST)
    start_time = time(6, 0)  # 06:00 AM KST
    end_time = time(22, 0)  # 10:00 PM KST
    if now_kst.weekday() < 5 and start_time <= now_kst.time() <= end_time:
        return 'generate_log_message'
    else:
        return 'skip_task'

with DAG(
    dag_id=DAG_ID,
    schedule_interval='@hourly',  # 매 시간 실행
    start_date=datetime(2024, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["example"],
    default_args={
        'on_failure_callback': task_fail_slack_alert,
    }
) as dag:
    check_time = BranchPythonOperator(
        task_id='check_time',
        python_callable=is_weekday_working_hours,
    )

    generate_log_message = PythonOperator(
        task_id='generate_log_message',
        python_callable=log_current_time,
        provide_context=True,
    )

    skip_task = PythonOperator(
        task_id='skip_task',
        python_callable=lambda: print("Skipping task because it's outside of working hours or it's the weekend"),
    )

    slack_webhook_operator_text = SlackWebhookOperator(
        task_id="slack_webhook_send_text",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='generate_log_message') }}",
    )

    check_time >> [generate_log_message, skip_task]
    generate_log_message >> slack_webhook_operator_text