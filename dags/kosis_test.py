from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import requests
import json
from airflow.models import Variable

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    api_key = Variable.get("api_key")  # Airflow Variable에서 API Key 가져오기
    parent_id = kwargs.get('parent_id', 'A')
    url = f"https://kosis.kr/openapi/statisticsList.do?method=getList&apiKey={api_key}&vwCd=MT_ZTITLE&parentListId={parent_id}&format=json&jsonVD=Y"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data

# JSON 데이터를 GCS에 업로드하는 함수
def upload_to_gcs(json_data, bucket_name, gcs_file_path, **kwargs):
    # GCS Hook 사용
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # 데이터를 JSON 파일로 저장
    local_file_path = '/tmp/kosis_data.json'  # 임시 로컬 파일에 저장
    with open(local_file_path, 'w') as f:
        json.dump(json_data, f, indent=4)
    
    # GCS에 업로드
    hook.upload(bucket_name, gcs_file_path, local_file_path)
    print(f"File uploaded to GCS: gs://{bucket_name}/{gcs_file_path}")

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
    'kosis_test',
    default_args=default_args,
    description='Fetch JSON data and upload to GCS',
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
    
    # GCS에 업로드하기
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket_name': 'kosis_api_test',  # GCS 버킷 이름
            'gcs_file_path': 'kosis_data.json',  # GCS 내 최상위 경로에 파일 저장
        },
        provide_context=True,
    )
    
    # 의존성 설정: fetch_json_task가 먼저 실행되고, upload_to_gcs_task가 그 다음에 실행되도록 설정
    fetch_json_task >> upload_to_gcs_task
