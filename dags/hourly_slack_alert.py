from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import os
from datetime import datetime, time, timedelta
import pytz

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID_MORNING = "hourly_slack_alert_morning"
DAG_ID_EVENING = "hourly_slack_alert_evening"
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
KST = pytz.timezone('Asia/Seoul')

def log_current_time(**context):
    now_utc = datetime.now().strftime('%H:%M:%S')
    now_kst = datetime.now(KST)
    now_kst_str = now_kst.strftime('%H:%M:%S')

    end_of_work_day = datetime.now(KST).replace(hour=17, minute=0, second=0, microsecond=0)
    if now_kst > end_of_work_day:
        end_of_work_day += timedelta(days=1)
        
    time_until_end_of_day = end_of_work_day - now_kst
    hours_until_end_of_day, remainder = divmod(time_until_end_of_day.seconds, 3600)
    minutes_until_end_of_day = remainder // 60

    message = (f":slack: UTC time is {now_utc}, KST time is {now_kst_str}.\n"
               f"{hours_until_end_of_day} hours {minutes_until_end_of_day} minutes "
               "left until 5 PM KST.")
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
    end_time = time(23, 0)  # 11:00 PM KST
    if now_kst.weekday() < 5 and start_time <= now_kst.time() <= end_time:
        return 'generate_log_message'
    else:
        return 'skip_task'

# Morning DAG: 00:00 to 09:00 KST (15:00 to 00:00 UTC)
with DAG(
    dag_id=DAG_ID_MORNING,
    schedule_interval='0 0-9 * * *',  # 00:00 to 09:00 KST
    start_date=datetime(2024, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["example"],
    default_args={
        'on_failure_callback': task_fail_slack_alert,
    }
) as dag_morning:
    check_time_morning = BranchPythonOperator(
        task_id='check_time_morning',
        python_callable=is_weekday_working_hours,
    )

    generate_log_message_morning = PythonOperator(
        task_id='generate_log_message_morning',
        python_callable=log_current_time,
        provide_context=True,
    )

    skip_task_morning = PythonOperator(
        task_id='skip_task_morning',
        python_callable=lambda: print("Skipping task because it's outside of working hours or it's the weekend"),
    )

    slack_webhook_operator_text_morning = SlackWebhookOperator(
        task_id="slack_webhook_send_text_morning",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='generate_log_message_morning') }}",
    )

    check_time_morning >> [generate_log_message_morning, skip_task_morning]
    generate_log_message_morning >> slack_webhook_operator_text_morning

# Evening DAG: 21:00 to 23:59 KST (12:00 to 14:59 UTC)
with DAG(
    dag_id=DAG_ID_EVENING,
    schedule_interval='0 12-14 * * *',  # 21:00 to 23:59 KST
    start_date=datetime(2024, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["example"],
    default_args={
        'on_failure_callback': task_fail_slack_alert,
    }
) as dag_evening:
    check_time_evening = BranchPythonOperator(
        task_id='check_time_evening',
        python_callable=is_weekday_working_hours,
    )

    generate_log_message_evening = PythonOperator(
        task_id='generate_log_message_evening',
        python_callable=log_current_time,
        provide_context=True,
    )

    skip_task_evening = PythonOperator(
        task_id='skip_task_evening',
        python_callable=lambda: print("Skipping task because it's outside of working hours or it's the weekend"),
    )

    slack_webhook_operator_text_evening = SlackWebhookOperator(
        task_id="slack_webhook_send_text_evening",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='generate_log_message_evening') }}",
    )

    check_time_evening >> [generate_log_message_evening, skip_task_evening]
    generate_log_message_evening >> slack_webhook_operator_text_evening
