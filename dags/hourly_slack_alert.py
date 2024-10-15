from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import os
from datetime import datetime, time, timedelta
import pytz

# 설정 변수들
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "hourly_slack_alert"
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
KST = pytz.timezone('Asia/Seoul')

# 함수 정의
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

    message = (f"On-Time Alarm :timer_clock: \nUTC : {now_utc} / KST : {now_kst_str}\n"
               f"{hours_until_end_of_day} hours {minutes_until_end_of_day} minutes "
               "left until 5 PM KST.")
    print(message)
    return message

def is_weekday_working_hours():
    now_kst = datetime.now(KST)
    start_time = time(7, 0)  # 07:00 AM KST
    end_time = time(16, 50)  # 04:50 PM KST
    exclude_start_time = time(11, 30)  # 11:30 AM KST
    exclude_end_time = time(12, 50)  # 12:50 PM KST

    # 금요일 체크
    if now_kst.weekday() == 4:  # 4는 금요일에 해당
        if 7 <= now_kst.day <= 13 or 21 <= now_kst.day <= 27:
            end_time = time(16, 0)  # 둘째 주 또는 넷째 주 금요일 오후 4시까지로 제한

    if now_kst.weekday() < 5:  # 월~금 체크
        if start_time <= now_kst.time() <= end_time:  # 시작 시간과 종료 시간 체크
            if not (exclude_start_time <= now_kst.time() <= exclude_end_time):  # 점심시간 제외
                return 'generate_log_message'

    return 'skip_task'

# DAG 선언
default_args = {
    'on_failure_callback': task_fail_slack_alert,
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval='50 22-23,0-7 * * *',  # 22:00-23:50, 00:00-07:50 UTC => 07:00-16:50 KST
    start_date=datetime(2024, 6, 1, 22, 0),  # 첫 실행을 맞추기 위해 start_date도 22:00 UTC로 설정
    max_active_runs=1,
    catchup=False,
    tags=["example"],
    default_args=default_args,
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
        username='on-time' 
    )

    check_time >> [generate_log_message, skip_task]
    generate_log_message >> slack_webhook_operator_text
