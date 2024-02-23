from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",  # 첫 번째 주 토요일, 월 단위 작업인 것
    start_date=pendulum.datetime(2024, 1, 30, tz="Asia/Seoul"),
    catchup=False
) as dag:

    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
        # 커맨드를 이렇게 준 이유는, 태스크를 실행하는 주체는 워커 컨테이너이기 때문에,
        # 워커 컨테이너가 쉘 프로그램의 위치를 알 수 있도록 이렇게 써줬다.
    )

    t2_hihi = BashOperator(
        task_id="t2_hihi",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh HIHI"
    )

    t1_orange >> t2_hihi
