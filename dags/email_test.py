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
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

import kubernetes.client as k8s
import kubernetes_asyncio.client as async_k8s

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowFailException

with DAG(
    dag_id="test_ETL_notification",
    #schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "notification"],
   # params={"example_key": "example_value"},
) as dag:

    def pull_xcom_and_act(**kwargs):
        ti = kwargs['ti']
        value = ti.xcom_pull(key='return_value')
        if value == '1':
            raise AirflowFailException


    send_email = EmailOperator( 
        task_id='send_email', 
        to='tatiana.pluzhnikova@cgi.com',
        subject='ETL', 
        html_content="Date: {{ ds }}",
        #trigger_rule="all_failed"
    )

    run_this = BashOperator(
        task_id="run_befor_email",
        bash_command="echo 1",
    )

        def pull_xcom_and_act(**kwargs):
            ti = kwargs['ti']
            value = ti.xcom_pull(key='return_value')
            if value == '1':
                raise AirflowFailException

    run_this >> send_email
