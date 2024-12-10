from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import csv

def fetch_json_data(**kwargs):
    api_key = Variable.get("api_key")
    url = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={api_key}&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&newEstPrdCnt=3&orgId=132&tblId=DT_V_MOTA_021"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # XCom을 통해 데이터를 반환하여 후속 태스크에서 사용할 수 있게 함
    kwargs['ti'].xcom_push(key='json_data', value=data)

def upload_json_to_csv_gcs(**kwargs):
    # XCom에서 JSON 데이터를 가져옴
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_json_data', key='json_data')
    
    if not json_data:
        raise ValueError("No data found to upload.")

    # GCS 버킷 이름과 파일 경로
    bucket_name = Variable.get("kosis_api_test_bucket")  # Airflow Variable에서 버킷 이름 가져오기
    gcs_file_path = 'kosis_data.csv'  # GCS 내 파일 경로

    # CSV 파일로 변환
    local_csv_file_path = '/tmp/kosis_data.csv'  # 로컬 임시 경로
    with open(local_csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        # 헤더 추출
        if json_data and isinstance(json_data, list):
            headers = json_data[0].keys()  # 첫 번째 항목의 키를 헤더로 사용
            writer.writerow(headers)
            # 데이터 작성
            for item in json_data:
                writer.writerow(item.values())

    # GCS Hook을 사용하여 업로드
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    hook.upload(bucket_name, gcs_file_path, local_csv_file_path)
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
    description='Fetch JSON data, convert to CSV, and upload to GCS',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # JSON 데이터 가져오기
    fetch_json_task = PythonOperator(
        task_id='fetch_json_data',
        python_callable=fetch_json_data,
        provide_context=True,
    )
    
    # CSV로 변환 후 GCS에 업로드하기
    upload_csv_task = PythonOperator(
        task_id='upload_json_to_csv_gcs',
        python_callable=upload_json_to_csv_gcs,
        provide_context=True,
    )
    
    fetch_json_task >> upload_csv_task
