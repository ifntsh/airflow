from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    catchup=False
) as dag: 
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='103182@naver.com',
        cc='bizifntsh@gmail.com',
        subject='Airflow 성공 메일',
        html_content='Airflow 작업이 완료되었습니다.'
    )