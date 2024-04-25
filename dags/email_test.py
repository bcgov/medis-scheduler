#
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
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

with DAG(
    dag_id="smtp_notifier",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=[
        send_smtp_notification(
            from_email="airflow@gov.bc.ca",
            to="tatiana.pluzhnikova@cgi.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
            html_content="debug logs",
        )
    ],
):
    BashOperator(
        task_id="mytask",
        on_failure_callback=[
            send_smtp_notification(
                from_email="airflow@gov.bc.ca",
                to="tatina.pluzhnikova@cgi.com",
                subject="[Error] The Task {{ ti.task_id }} failed",
                html_content="debug logs",
            )
        ],
        bash_command="fail",
    )
