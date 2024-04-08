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
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow_kubernetes_job_operator.kubernetes_job_operator import KubernetesJobOperator


with DAG(
    dag_id="medis-etl",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    etl_job_task = KubernetesJobOperator(
        task_id= 'MEDIS_file_upload'
    )

    facility_fha_task = HttpOperator(
        task_id='LTC_Facility_Information_Fraser',
        method='POST',
        endpoint='{{var.value.my_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"FHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    )

    facility_fha_task >> MEDIS_file_upload

    facility_viha_task = HttpOperator(
        task_id='LTC_Facility_Information_Island',
        method='POST',
        endpoint='{{var.value.my_url}}',
        data='{"version" : "", "startDate" : "", "endDate":"", "updatedMinDate":"", "updatedMaxDate":"", "draft":false, "deleted":true, "status":"COMPLETED", "healthAuthority":"VIHA", "isHeaderAdded": false}',
        headers={"Content-Type": "application/json"},
    ) 

    facility_viha_task >> MEDIS_file_upload



if __name__ == "__main__":
    dag.test()