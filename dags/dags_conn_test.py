from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="t1"
    )
    t2 = BashOperator(
        task_id="t2"
    )
    t3 = BashOperator(
        task_id="t3"
    )
    t4 = BashOperator(
        task_id="t4"
    )
    t5 = BashOperator(
        task_id="t5"
    )
    t6 = BashOperator(
        task_id="t6"
    )
    t7 = BashOperator(
        task_id="t7"
    )
    t8 = BashOperator(
        task_id="t8"
    )

    t1 >> [t2, t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8