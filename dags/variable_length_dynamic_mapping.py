from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
import json

@dag(schedule_interval='@daily', start_date=days_ago(1), catchup=False)
def dynamic_mapping_example():
    @task
    def fetch_and_preprocess():
        """
        Airflow Variable 'raw_data'에서 값을 가져와 전처리하여 동적으로 생성된 리스트를 반환합니다.
        예제에서는 JSON 형식의 데이터를 가져와 각 원소 앞에 'processed_' 접두사를 붙여 처리합니다.
        """
        # 예를 들어, Airflow Variable에 저장된 값이 '["a", "b", "c"]'와 같다고 가정합니다.
        raw_value = Variable.get("raw_data", default_var='[]')
        data = json.loads(raw_value)
        # 전처리 작업: 원소마다 접두사를 붙여 새로운 리스트 생성 (출력 결과에 따라 동적 개수 발생)
        processed_list = [f"processed_{item}" for item in data]
        return processed_list

    @task
    def process_item(item: str):
        """
        동적으로 생성된 각 아이템을 처리하는 task입니다.
        각 task는 개별적으로 실행되며, 결과를 반환합니다.
        """
        result = f"result_of_{item}"
        print(f"Processing {item}: {result}")
        return result

    @task
    def store_results(results: list):
        """
        모든 동적 task의 결과를 모아서 어딘가(예: 로그 출력 또는 외부 시스템)에 저장하는 task입니다.
        필요에 따라 Airflow Variable에 다시 저장할 수도 있습니다.
        """
        print("Storing results:", results)
        # 예시로 Airflow Variable에 저장하는 경우:
        # Variable.set("processed_results", json.dumps(results))
        return results

    # 첫번째 task: Airflow Variable을 가져와 전처리 후, 동적 리스트 생성
    processed_list = fetch_and_preprocess()
    # 두번째 task: Dynamic Task Mapping을 통해 리스트의 각 아이템에 대해 task 생성
    dynamic_results = process_item.expand(item=processed_list)
    # 세번째 task: 모든 결과를 모아 저장
    final = store_results(dynamic_results)

dynamic_mapping_example = dynamic_mapping_example()
