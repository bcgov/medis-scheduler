# medis-scheduler

## MoH MEDIS project ETL scheduler

Apache Airflow is an open-source workflow management platform for data engineering pipelines. It was chosen as our ETL process scheduler.

Airflow is deployed to OpenShift using the Apache Airflow Helm chart. The current deployment is based on Airflow `3.2.2` and Helm chart `1.22.0`.

## Airflow installation

The installation requires the OpenShift CLI (`oc`) and Helm to be installed on your OS.

1. Connect to the OpenShift platform. From <https://oauth-openshift.apps.silver.devops.gov.bc.ca/oauth/token/display>, get the API token. Copy the `oc login` command with the token and run it from your laptop. The laptop must have access to the government OpenShift cluster.

   Example:

   ```cmd
   oc login --token=sha256~xxxxxxxxxxxxx --server=https://api.silver.devops.gov.bc.ca:6443
   ```

2. Choose the OpenShift project where Airflow will be installed or upgraded.

   ```cmd
   oc project c2da03-test
   ```

   or:

   ```cmd
   oc project c2da03-prod
   ```

3. Add/update the Apache Airflow Helm chart repository.

   ```cmd
   helm repo add apache-airflow https://airflow.apache.org
   helm repo update
   ```

4. Run the Helm command to install/upgrade Airflow.

   TEST:

   ```cmd
   helm.exe upgrade --install airflow apache-airflow/airflow --namespace c2da03-test --version 1.22.0 -f airflow\override-values-c2da03-test.yaml --reset-values --wait --wait-for-jobs --timeout 30m
   ```

   PROD:

   ```cmd
   helm.exe upgrade --install airflow apache-airflow/airflow --namespace c2da03-prod --version 1.22.0 -f airflow\override-values-c2da03-prod.yaml --reset-values --wait --wait-for-jobs --timeout 30m
   ```

   The override files contain the OpenShift-specific Airflow settings for each environment:

   - `airflow/override-values-c2da03-test.yaml`
   - `airflow/override-values-c2da03-prod.yaml`

   For the Airflow 3 upgrade, use `--reset-values` so old Airflow 2 chart values are not reused.

## Airflow configuration

1. Airflow can be accessed at:

   - <https://airflow-webserver-c2da03-test.apps.silver.devops.gov.bc.ca/>
   - <https://airflow-webserver-c2da03-prod.apps.silver.devops.gov.bc.ca/>

   In Airflow 3, the backend service is `airflow-api-server` instead of the old `airflow-webserver` service. The existing route URL can remain the same, but the OpenShift route should point to service `airflow-api-server` on port `8080`.

2. DAGs are stored in GitHub under `medis-scheduler/dags` and are synced to Airflow from GitHub every 10 seconds:

   - TEST Airflow syncs from the `test` branch.
   - PROD Airflow syncs from the `main` branch.

   Changes should be tested in the `test` branch first, then promoted to `main` through a pull request.

3. Airflow Variables are used for flexibility, so some parameters can be modified without changing the DAG code. Variables can be exported/imported as JSON files and are stored in the `airflow` folder in this repository.

## Airflow PCD-ETL, MEDIS-ETL, and related DAGs

Airflow scheduler performs the same ETL steps as described in the project ETL design documentation:

- <https://proactionca.ent.cgi.com/confluence/pages/viewpage.action?spaceKey=BCMOHAD&title=ETL+process+design>
- <https://proactionca.ent.cgi.com/confluence/display/BCMOHAD/Manual+trigger+for+ETL+process>

Additional steps are included for error handling and email notifications.

1. The extract phase calls the ETL service endpoint in OpenShift using Airflow `HttpOperator`. The URLs for each form being extracted are defined as Airflow Variables.

2. The payload is hardcoded in the DAG.

3. File upload is implemented as a job on OpenShift and triggered by Airflow `KubernetesJobOperator`. The job runs a pod that executes `upload.sh`, creates the combined `medis_ltc.flag` file, uploads encrypted files, uploads the newly created flag file, moves files into the archive directory, and deletes archived files older than the configured retention period.

4. DAG notification logic is compatible with Airflow 3.2.2.

## Running locally

### Prerequisites

- Docker: <https://docs.docker.com/engine/install/>
- Docker Compose: <https://docs.docker.com/compose/install/>

### Running locally

The local Docker Compose setup uses Airflow `3.2.2`. The compose service name remains `airflow-webserver`, but it runs the Airflow 3 `api-server` command.

1. Create `config`, `logs`, and `plugins` directories on the same level as `dags`.

2. Run the following command to initialize the Docker containers.

   ```bash
   docker-compose up airflow-init
   ```

3. Run the following command to start the Airflow instance.

   ```bash
   docker-compose up
   ```

4. Access Airflow locally at <http://localhost:8080/> and log in with username `airflow` and password `airflow`.

### Clean up

Run the following command to delete all containers and free up memory.

```bash
docker-compose down --volumes --remove-orphans
```