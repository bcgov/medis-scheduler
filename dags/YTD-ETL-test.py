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
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.operators.email_operator import EmailOperator


with DAG(
    dag_id="ytd-medis-etl",
    #schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "medis"],
   # params={"example_key": "example_value"},
) as dag:
    etl_job_task = KubernetesJobOperator(
        task_id='MEDIS_file_upload',
        job_template_file='{{var.value.medis_job}}',
    )

    start_ytd_extract = EmptyOperator(
        task_id="Start_YTD_Extract",
    )

    ytd_fha_task = HttpOperator(
        task_id='YTD_Fraser',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    ytd_iha_task = HttpOperator(
        task_id='YTD_Interior',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"IHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )
 
    
    ytd_viha_task = HttpOperator(
        task_id='YTD_Island',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VIHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 


    ytd_nha_task = HttpOperator(
        task_id='YTD_Northern',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"NHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )


    ytd_vch_task = HttpOperator(
        task_id='YTD_Vancouver',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VCH", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    send_email = EmailOperator( 
        task_id='send_email', 
        to='tatiana.pluzhnikova@cgi.com', 
        subject='ingestion complete', 
        html_content="Date: {{ ds }}", 
        dag=dag_email)

    start_ytd_extract >> ytd_fha_task >> etl_job_task
    start_ytd_extract >> ytd_iha_task >> etl_job_task
    start_ytd_extract >> ytd_viha_task >> etl_job_task
    start_ytd_extract >> ytd_nha_task >> etl_job_task
    start_ytd_extract >> ytd_vch_task >> etl_job_task
    send_email.set_upstream(etl_job_task)

    if __name__ == "__main__":
    dag.ytdtest()