# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "slack_webhook_example_dag"
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
IMAGE_URL = "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png"

def log_current_time(**context):
    current_time = datetime.now().time()
    message = f"Current time is {current_time}"
    print(message)
    return message

with DAG(
    dag_id=DAG_ID,
    schedule_interval='@hourly',  # 매 시간 실행
    start_date=datetime(2024, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["example"],
) as dag:
    # 로그 메시지를 생성하는 PythonOperator
    generate_log_message = PythonOperator(
        task_id='generate_log_message',
        python_callable=log_current_time,
        provide_context=True,
    )

    # [START slack_webhook_operator_text_howto_guide]
    slack_webhook_operator_text = SlackWebhookOperator(
        task_id="slack_webhook_send_text",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='generate_log_message') }}",
    )
    # [END slack_webhook_operator_text_howto_guide]

    generate_log_message >> slack_webhook_operator_text

if __name__ == "__main__":
    dag.cli()
