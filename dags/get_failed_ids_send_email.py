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

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.exceptions import AirflowSkipException

with DAG(
    dag_id="test_fail_success_send_email",
    # schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "notification"]
) as dag:

    # Function to generate HTML content for email
    def generate_html(failed_ids):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        html_content = "<html><head></head><body><h1>Airflow run at %s failed</h1><p>Automatically generated message in case of failure.</p><h2>Failed Task IDs</h2><ul>" % current_time
        for failed_id in failed_ids:
            html_content += f"<li>{failed_id}</li>"
        html_content += "</ul><h4>Please access Airflow and review tasks run.</h4></body></html>"
        print(html_content)
        return html_content

    def get_failed_ids_send_email(ds=None, **kwargs):
        ti = kwargs['ti']
        dag_run = kwargs['dag_run']
        # Get all tasks that are upstream of this task
        upstream_task_ids = ti.task.get_flat_relative_ids(upstream=True)
        # Get list of all tasks that have failed for this DagRun
        failed_task_instances = dag_run.get_task_instances(state='failed')
        # Get intersection of the sets to get upstream tasks that failed
        failed_upstream_task_ids = upstream_task_ids.intersection(
            [task.task_id for task in failed_task_instances])
        print(f"Upstream tasks that failed: {failed_upstream_task_ids}")
        # If no upstream tasks have failed, skip this task
        if len(failed_upstream_task_ids) == 0:
            raise AirflowSkipException("No upstream tasks have failed")
        # If there are failed upstream tasks, send an email with the failed task IDs
        elif len(failed_upstream_task_ids) > 0:
            send_email(
                to='{{var.value.ETL_email_list_alerts}}',
                subject='subject',
                html_content=generate_html(failed_upstream_task_ids),
            )
        return failed_upstream_task_ids

    get_failed_ids = PythonOperator(
        task_id="get_failed_ids", python_callable=get_failed_ids_send_email, trigger_rule="all_done")

    http_local_post_500_1 = BashOperator(
        task_id='http_local_post_500_1',
        bash_command='echo "Failed Task"; exit 1;',
        dag=dag,
    )

    http_local_post_500_2 = BashOperator(
        task_id='http_local_post_500_2',
        bash_command='echo "Failed Task"; exit 1;',
        dag=dag,
    )

    http_local_post_200_1 = BashOperator(
        task_id='http_local_post_200_1',
        bash_command='echo "Success Task"; exit 0;',
        dag=dag,
    )

    http_local_post_200_2 = BashOperator(
        task_id='http_local_post_200_2',
        bash_command='echo "Success Task"; exit 0;',
        dag=dag,
    )

    http_local_post_200_3 = BashOperator(
        task_id='http_local_post_200_3',
        bash_command='echo "Success Task"; exit 0;',
        dag=dag,
    )

    http_local_post_500_1 >> get_failed_ids
    http_local_post_500_2 >> get_failed_ids
    http_local_post_200_1 >> get_failed_ids
    http_local_post_200_2 >> get_failed_ids
    http_local_post_200_3 >> get_failed_ids