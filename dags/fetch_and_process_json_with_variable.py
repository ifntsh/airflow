from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    # Airflow Variable로부터 API 키 가져오기
    api_key = Variable.get("api_key")  # 'api_key'는 Web UI에 설정한 키 이름
    parent_id = kwargs.get('parent_id', 'A')
    url = f"https://kosis.kr/openapi/statisticsList.do?method=getList&apiKey={api_key}&vwCd=MT_ZTITLE&parentListId={parent_id}&format=json&jsonVD=Y"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data

# JSON 데이터를 처리하는 함수
def process_json_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_json_data')
    for entry in json_data:
        if 'LIST_NM' in entry:
            print(f"Category: {entry['LIST_NM']} (ID: {entry['LIST_ID']})")
        else:
            print(f"Table: {entry.get('TBL_NM', 'Unknown')}")

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'fetch_and_process_json_with_variable',
    default_args=default_args,
    description='Fetch and process JSON data from API using Airflow Variables',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # JSON 데이터 가져오기
    fetch_json_task = PythonOperator(
        task_id='fetch_json_data',
        python_callable=fetch_json_data,
        op_kwargs={'parent_id': 'A'},
    )
    
    # JSON 데이터 처리
    process_json_task = PythonOperator(
        task_id='process_json_data',
        python_callable=process_json_data,
        provide_context=True,
    )
    
    # 의존성 설정
    fetch_json_task >> process_json_task
