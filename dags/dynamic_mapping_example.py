from airflow.decorators import dag, task
from airflow.models import Variable
import json
from airflow.utils.dates import days_ago

@dag(schedule_interval='@daily', start_date=days_ago(1), catchup=False)
def dynamic_mapping_example():
    
    @task
    def get_sub_jobs():
        """
        Airflow Variable 'sub_jobs'에서 JSON 형식의 데이터를 읽어와
        sub_jobs 리스트를 반환합니다.
        """
        sub_jobs_str = Variable.get("sub_jobs", default_var='[]')
        sub_jobs = json.loads(sub_jobs_str)
        return sub_jobs

    @task
    def process_sub_job(sub_job: dict):
        """
        각 sub_job에 대해 필요한 처리를 진행합니다.
        예제에서는 각 딕셔너리의 key와 value를 출력하는 로직을 수행합니다.
        """
        for key, value in sub_job.items():
            print(f"Processing {key}: {value}")
        # 실제 처리 결과를 반환하거나 후속 작업에 활용할 수 있습니다.
        return sub_job

    @task
    def store_results(results: list):
        """
        모든 동적으로 생성된 task의 결과를 모아서 저장합니다.
        필요에 따라 결과를 다시 Airflow Variable이나 외부 스토리지에 저장할 수 있습니다.
        """
        print("Storing results:", results)
        # 예를 들어, Variable.set("processed_results", json.dumps(results))
        return results

    # 첫번째 Task: Airflow Variable에서 sub_jobs 값을 불러옴
    sub_jobs = get_sub_jobs()
    
    # 두번째 Task: Dynamic Task Mapping을 사용해 각 sub_job에 대해 task 생성
    processed_results = process_sub_job.expand(sub_job=sub_jobs)
    
    # 세번째 Task: 모든 처리 결과를 모아서 저장
    store_results(processed_results)

# DAG 인스턴스 생성 (DAG 이름은 함수 이름인 dynamic_mapping_example이 됩니다)
dynamic_mapping_example_dag = dynamic_mapping_example()
