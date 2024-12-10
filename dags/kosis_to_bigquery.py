from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import json
import csv
from airflow.models import Variable

# JSON 데이터를 API에서 가져오는 함수
def fetch_json_data(**kwargs):
    api_key = Variable.get("api_key")  # Airflow Variable에서 API Key 가져오기
    url = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={api_key}&itmId=13103112873NO_ACCI+13103112873NO_DEATH+13103112873NO_WOUND+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&newEstPrdCnt=10000&prdInterval=1&orgId=132&tblId=DT_V_MOTA_021"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # XCom을 통해 데이터를 반환하여 후속 태스크에서 사용할 수 있게 함
    kwargs['ti'].xcom_push(key='json_data', value=data)

# JSON 데이터를 CSV로 변환하고 GCS에 업로드하는 함수
def json_to_csv_and_upload(**kwargs):
    # XCom에서 JSON 데이터를 가져옴
    json_data = kwargs['ti'].xcom_pull(task_ids='fetch_json_data', key='json_data')

    if not json_data:
        raise ValueError("No data found to convert to CSV.")

    # GCS 버킷 이름 및 파일 경로
    bucket_name = Variable.get("kosis_api_test_bucket")
    gcs_file_path = 'kosis_data.csv'
    local_file_path = '/tmp/kosis_data.csv'

    # JSON 데이터를 CSV로 변환
    with open(local_file_path, mode='w', newline='', encoding='utf-8-sig') as csv_file:
        csv_writer = csv.writer(csv_file)

        # JSON 데이터를 변환할 예시
        headers = list(json_data[0].keys())  # JSON 키를 CSV 헤더로 사용
        csv_writer.writerow(headers)  # 헤더 쓰기

        for item in json_data:
            csv_writer.writerow(item.values())  # 값 쓰기

    # GCS에 업로드
    hook = GCSHook(gcp_conn_id='google_cloud_default')
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
    'kosis_to_bigquery',  # DAG 이름
    default_args=default_args,
    description='Fetch JSON data, convert to CSV, upload to GCS, and insert to BigQuery',
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
    
    # JSON 데이터를 CSV로 변환하고 GCS에 업로드
    json_to_csv_task = PythonOperator(
        task_id='json_to_csv_and_upload',
        python_callable=json_to_csv_and_upload,
        provide_context=True,
    )

    # GCS에 있는 CSV 데이터를 BigQuery에 삽입
    gcs_to_bigquery_task = BigQueryInsertJobOperator(
        task_id='load_csv_to_bigquery',
        configuration={
            "load": {
                "sourceUris": [f"gs://{Variable.get('kosis_api_test_bucket')}/kosis_data.csv"],
                "destinationTable": {
                    "projectId": Variable.get("gcp_project_id"),
                    "datasetId": Variable.get("bigquery_dataset_id"),
                    "tableId": Variable.get("bigquery_table_id"),
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,  # 헤더 제외
                "writeDisposition": "WRITE_TRUNCATE",  # 기존 데이터 덮어쓰기
                "fieldDelimiter": ",",
                "encoding": "UTF-8",
                "autodetect": True,  # 스키마 자동 감지 추가
            }
        },
        gcp_conn_id='google_cloud_default',
    )
    
    # 태스크 순서 정의
    fetch_json_task >> json_to_csv_task >> gcs_to_bigquery_task
