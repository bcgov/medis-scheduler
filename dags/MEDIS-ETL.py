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
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.email import send_email
from airflow.models import Variable


with DAG(
    dag_id="medis-etl",
    #schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "medis"],
   # params={"example_key": "example_value"},
) as dag:

    # Function to generate HTML content for email in case of failure
    def generate_failed_html(failed_ids,dag_id):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        html_content = "<html><head></head><body><h1>Airflow %s DAG run at %s failed</h1><p>Automatically generated message in case of failure.</p><h2>Failed Task IDs</h2><ul>" % (dag_id,current_time)
        for failed_id in failed_ids:
            html_content += f"<li>{failed_id}</li>"
        html_content += "</ul><h4>Please access Airflow and review tasks run: <a href='" + \
           Variable.get("airflow_url") + "'>" + \
           Variable.get("airflow_url") + "</a></h4></body></html>"
        print(html_content)
        return html_content
    
    # Function to generate HTML content for email in case of success
    def generate_success_html(dag_id):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        html_content = "<html><head></head><body><h1>Airflow %s DAG run at %s succeeded</h1><p>Automatically generated message in case of success.</p></body></html>" % (dag_id,current_time)
        print(html_content)
        return html_content

    # Function to check all previous upstream tasks and notify user if dag run results
    def get_failed_ids_send_email(ds=None, **kwargs):
        ti = kwargs['ti']
        dag_run = kwargs['dag_run']
        dag_id = kwargs['dag'].dag_id
        # Get all tasks that are upstream of this task
        upstream_task_ids = ti.task.get_flat_relative_ids(upstream=True)
        # Get list of all tasks that have failed for this DagRun
        failed_task_instances = dag_run.get_task_instances(state='failed')
        # Get intersection of the sets to get upstream tasks that failed
        failed_upstream_task_ids = upstream_task_ids.intersection(
            [task.task_id for task in failed_task_instances])
        print(f"Upstream tasks that failed: {failed_upstream_task_ids}")
        # If no upstream tasks have failed, send an email with success message
        if len(failed_upstream_task_ids) == 0:
            send_email(
                to=Variable.get("ETL_email_list_alerts"),
                subject='Airflow ' + dag_id + ' run SUCCEEDED!',
                html_content=generate_success_html(dag_id),
            )
        # If there are failed upstream tasks, send an email with the failed task IDs
        elif len(failed_upstream_task_ids) > 0:
            send_email(
                to=Variable.get("ETL_email_list_alerts"),
                subject='Airflow ' + dag_id + ' run FAILED!',
                html_content=generate_failed_html(failed_upstream_task_ids,dag_id),
            )
        return failed_upstream_task_ids


    etl_job_task = KubernetesJobOperator(
        task_id='MEDIS_file_upload',
        job_template_file='{{var.value.medis_job}}',
    )

    failed_tasks_notification = PythonOperator(
        task_id="Failed_Tasks_Notification", python_callable=get_failed_ids_send_email, trigger_rule="all_done")

    start_ytd_extract = EmptyOperator(
        task_id="Start_LTC_YTD_Extract",
    )

    ytd_fha_task = HttpOperator(
        task_id='LTC_YTD_Fraser',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    ytd_iha_task = HttpOperator(
        task_id='LTC_YTD_Interior',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"IHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )
    
    ytd_viha_task = HttpOperator(
        task_id='LTC_YTD_Island',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VIHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 

    ytd_nha_task = HttpOperator(
        task_id='LTC_YTD_Northern',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"NHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    ytd_vch_task = HttpOperator(
        task_id='LTC_YTD_Vancouver',
        method='POST',
        endpoint='{{var.value.quarterly_ytd_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VCH", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )
  
    facility_fha_task = HttpOperator(
        task_id='LTC_Facility_Information_Fraser',
        method='POST',
        endpoint='{{var.value.facility_information_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    facility_iha_task = HttpOperator(
        task_id='LTC_Facility_Information_Interior',
        method='POST',
        endpoint='{{var.value.facility_information_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"IHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    facility_viha_task = HttpOperator(
        task_id='LTC_Facility_Information_Island',
        method='POST',
        endpoint='{{var.value.facility_information_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VIHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 

    facility_nha_task = HttpOperator(
        task_id='LTC_Facility_Information_Northern',
        method='POST',
        endpoint='{{var.value.facility_information_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"NHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    facility_vch_task = HttpOperator(
        task_id='LTC_Facility_Information_Vancouver',
        method='POST',
        endpoint='{{var.value.facility_information_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VCH", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_500_1 = BashOperator(
        task_id='http_local_post_500_1',
        bash_command='echo "Failed Task"; exit 1;',
        dag=dag,
    )


    start_facility_extract = EmptyOperator(
        task_id="Start_LTC_Facility_Extract",
    )


    start_facility_extract >> facility_fha_task >> start_ytd_extract
    start_facility_extract >> facility_iha_task >> start_ytd_extract
    start_facility_extract >> facility_viha_task >> start_ytd_extract
    start_facility_extract >> facility_nha_task >> start_ytd_extract
    start_facility_extract >> facility_vch_task >> start_ytd_extract
    start_facility_extract >> http_local_post_500_1 >> start_ytd_extract

    start_ytd_extract >> ytd_fha_task >> etl_job_task
    start_ytd_extract >> ytd_iha_task >> etl_job_task
    start_ytd_extract >> ytd_viha_task >> etl_job_task
    start_ytd_extract >> ytd_nha_task >> etl_job_task
    start_ytd_extract >> ytd_vch_task >> etl_job_task

    etl_job_task >> failed_tasks_notification


    #delay_5s_task = BashOperator(
    #    task_id="Delay",
    #    bash_command="sleep 5s",
    #)

    #delay_5s_task >> facility_viha_task


if __name__ == "__main__":
    dag.test()