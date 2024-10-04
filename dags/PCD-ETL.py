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
    dag_id="pcd-etl",
    #schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "pcd"],
   # params={"example_key": "example_value"},
) as dag:

    # Function to generate HTML content for email in case of failure
    def generate_failed_html(failed_ids,dag_id):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        html_content = "<html><head></head><body>Airflow %s DAG run at %s failed<p>Automatically generated message in case of failure.</p><b>Failed Task IDs</b><ul>" % (dag_id,current_time)
        for failed_id in failed_ids:
            html_content += f"<li>{failed_id}</li>"
        html_content += "</ul>Please access Airflow and review tasks run: <a href='" + \
           Variable.get("airflow_url") + "'>" + \
           Variable.get("airflow_url") + "</a></body></html>"
        print(html_content)
        return html_content
    
    # Function to generate HTML content for email in case of success
    def generate_success_html(dag_id):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        html_content = "<html><head></head><body>Airflow %s DAG run at %s succeeded<p>Automatically generated message in case of success.</p></body></html>" % (dag_id,current_time)
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
                to=Variable.get("PCD_ETL_email_list_success"),
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
        task_id='PCD_file_upload',
        job_template_file='{{var.value.pcd_job}}',
    )

    failed_tasks_notification = PythonOperator(
        task_id="ETL_Notification", python_callable=get_failed_ids_send_email, trigger_rule="all_done")

    start_pcd_extract_1 = EmptyOperator(
        task_id="Start_PCD_Extract_1",
    )

    start_pcd_extract_2 = EmptyOperator(
        task_id="Start_PCD_Extract_2",
    )

    financial_expense_task = HttpOperator(
        task_id='Financial_Expense',
        method='POST',
        endpoint='{{var.value.pcd_financial_expense_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    upcc_financial_reporting_task = HttpOperator(
        task_id='UPCC_Financial_Reportingr',
        method='POST',
        endpoint='{{var.value.pcd_upcc_financial_reporting_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )
    
    chc_financial_reporting_task = HttpOperator(
        task_id='CHC_Financial_reporting',
        method='POST',
        endpoint='{{var.value.pcd_chc_financial_reporting_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 

    pcn_financial_reporting_task = HttpOperator(
        task_id='PCN_Financial_Reporting',
        method='POST',
        endpoint='{{var.value.pcd_pcn_financial_reporting_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    #status_tracker_task = HttpOperator(
    #    task_id='Status_Tracker',
    #    method='POST',
    #    endpoint='{{var.value.pcd_status_tracker_url}}',
    #    response_check=lambda response: response.json()["statusCode"]==200,
    #    data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
    #    headers={"Content-Type": "application/json"},
    #)
  
    decision_log_task = HttpOperator(
        task_id='Decision_Log',
        method='POST',
        endpoint='{{var.value.pcd_decision_log_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    ha_hierarchy_task = HttpOperator(
        task_id='HA_Hierarchy',
        method='POST',
        endpoint='{{var.value.pcd_ha_hierarchy_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    upcc_budget_task = HttpOperator(
        task_id='UPPC_Budget',
        method='POST',
        endpoint='{{var.value.pcd_upcc_budget_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 

    chc_budget_task = HttpOperator(
        task_id='CHC_Budget',
        method='POST',
        endpoint='{{var.value.pcd_chc_budget_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    pcn_budget_task = HttpOperator(
        task_id='PCN_Budget',
        method='POST',
        endpoint='{{var.value.pcd_pcn_budget_url}}',
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":false, "status":"SUBMITTED", "healthAuthority":"", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    check_pcd_folder_task = KubernetesJobOperator(
        task_id='Check_PCD_Shared_Folder',
        job_template_file='{{var.value.pcd_emtydir_job}}',
        wait_until_job_complete=True,
    )

    check_pcd_sftp_folder_task = KubernetesJobOperator(
        task_id='Check_PCD_SFTP_Folder',
        job_template_file='{{var.value.pcd_emtysftp_job}}',
        wait_until_job_complete=True,
    )

    check_pcd_sftp_folder_task >> check_pcd_folder_task >> start_pcd_extract_1
  
 #   start_pcd_extract_1 >> status_tracker_task >> start_pcd_extract_2
    start_pcd_extract_1 >> financial_expense_task >> start_pcd_extract_2
    start_pcd_extract_1 >> upcc_financial_reporting_task >> start_pcd_extract_2
    start_pcd_extract_1 >> chc_financial_reporting_task >> start_pcd_extract_2
    start_pcd_extract_1 >> pcn_financial_reporting_task >> start_pcd_extract_2

 #  start_pcd_extract_2 >> decision_log_task >> etl_job_task
 #  start_pcd_extract_2 >> ha_hierarchy_task >> etl_job_task
    start_pcd_extract_2 >> upcc_budget_task >> etl_job_task
    start_pcd_extract_2 >> chc_budget_task >> etl_job_task
    start_pcd_extract_2 >> pcn_budget_task >> etl_job_task


    etl_job_task >> failed_tasks_notification



if __name__ == "__main__":
    dag.pcd()