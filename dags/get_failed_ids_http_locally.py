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
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowSkipException

with DAG(
    dag_id="test_ETL_notification",
    #schedule="0 0 * * *",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["etl", "notification"]
) as dag:

    def get_failed_ids(ds=None, **kwargs):
        ti = kwargs['ti']
        dag_run = kwargs['dag_run']
        # Get all tasks that are upstream of this task
        upstream_task_ids = ti.task.get_flat_relative_ids(upstream=True)
        # Get list of all tasks that have failed for this DagRun
        failed_task_instances = dag_run.get_task_instances(state='failed')
        # Get intersection of the sets to get upstream tasks that failed
        failed_upstream_task_ids = upstream_task_ids.intersection([task.task_id for task in failed_task_instances])
        print(f"Upstream tasks that failed: {failed_upstream_task_ids}")
        # If no upstream tasks have failed, skip this task
        if len(failed_upstream_task_ids) == 0:
            raise AirflowSkipException("No upstream tasks have failed")
        return failed_upstream_task_ids

    get_failed_ids = PythonOperator(task_id="get_failed_ids", python_callable=get_failed_ids,trigger_rule="all_done")

    http_local_post_500_1 = HttpOperator(
        task_id='http_local_post_500_1',
        method='POST',
        http_conn_id="Local_HTTP",
        endpoint="/",
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"res":"500","version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_500_2 = HttpOperator(
        task_id='http_local_post_500_2',
        method='POST',
        http_conn_id="Local_HTTP",
        endpoint="/",
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"res":"500","version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_200_1 = HttpOperator(
        task_id='http_local_post_200_1',
        method='POST',
        http_conn_id="Local_HTTP",
        endpoint="/",
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"res":"200","version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_200_2 = HttpOperator(
        task_id='http_local_post_200_2',
        method='POST',
        http_conn_id="Local_HTTP",
        endpoint="/",
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"res":"200","version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_200_3 = HttpOperator(
        task_id='http_local_post_200_3',
        method='POST',
        http_conn_id="Local_HTTP",
        endpoint="/",
        response_check=lambda response: response.json()["statusCode"]==200,
        data='{"res":"200","version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    http_local_post_500_1 >> get_failed_ids
    http_local_post_500_2 >> get_failed_ids
    http_local_post_200_1 >> get_failed_ids
    http_local_post_200_2 >> get_failed_ids
    http_local_post_200_3 >> get_failed_ids
