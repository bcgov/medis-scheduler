# medis-scheduler
## MoH MEDIS project ETL scheduler


Apache Airflow is an open-source workflow management platform for data engineering pipelines. It was chosen as our ETL process scheduler. 
Airflow was deployed to OpenShift using Helm Chart downloaded to the local directory (laptop with Windows). It also can be installed directly from helm-chart repository ([https://airflow.apache.org/docs/helm-chart/stable/index.html])
### Airflow installation.
(The installation requires OpenShift CLI (oc) and helm to be installed on your OS)
1.	Connect to OpenShift platform.
From [https://oauth-openshift.apps.silver.devops.gov.bc.ca/oauth/token/display] get the API token.
Copy oc command with the token to login and run it from your laptop. (Laptop should have access to government’s OpenShift).
The example of your command:
oc login --token=sha256~yxiCAMwFD_XXX --server=https://api.silver.devops.gov.bc.ca:6443
Logged into [https://api.silver.devops.gov.bc.ca:6443] as "username@github" using the token provided.

You have access to the following projects and can switch between them with 'oc project <projectname>':

    c2da03-dev
  * c2da03-prod
    c2da03-test
    c2da03-tools

Using project "c2da03-prod".

2.	Choose the project you going to deploy or upgrade Airflow to.
3.	Run the helm command to install/upgrade Airflow:
helm.exe upgrade --install airflow C:\path-to-helm-chart\helm-v3.14.3-windows-amd64\windows-amd64\airflow-1.13.0\airflow-1.13.0\airflow --namespace c2da03-test -f C:\Users\tatiana.pluzhnikova\Downloads\helm-v3.14.3-windows-amd64\windows-amd64\airflow-1.13.0\airflow-1.13.0\airflow\override-values-c2da03-test.yaml

This command uses the downloaded helm-chart for installation and the default settings will be overwritten with override-values-c2da03-test.yaml file which can be found in GitHub [https://github.com/bcgov/medis-scheduler/blob/6ef9eb0d6c5d61751b796123ae13484d29ca9de8/airflow/override-values-c2da03-test.yaml]


### Airflow configuration
1.	Airflow can be accessed at [https://airflow-webserver-c2da03-test.apps.silver.devops.gov.bc.ca/] and [https://airflow-webserver-c2da03-prod.apps.silver.devops.gov.bc.ca/]  
2.	The DAGs are stored in GitHab  (medis-scheduler/dags at main · bcgov/medis-scheduler (github.com) and being synced to Airflow every 10 seconds from test branch to test Airflow and from main branch to prod Airflow. 
(We are trying to do changes it test brunch first then do pull request to main branch)
3.	We use Airflow Variables for more flexibility, so some parameters can be modified without touching the DAGs. The variables can be exported and imported to/from json format file and we keep them in GitHub [https://github.com/bcgov/medis-scheduler/tree/6ef9eb0d6c5d61751b796123ae13484d29ca9de8/airflow]

### Airflow PCD-ETL and MEDIS-ETL DAGs description.
Airflow scheduler perform the same ETL steps as described there: https://proactionca.ent.cgi.com/confluence/pages/viewpage.action?spaceKey=BCMOHAD&title=ETL+process+design and ([https://proactionca.ent.cgi.com/confluence/display/BCMOHAD/Manual+trigger+for+ETL+process] ) plus we added extra steps for error handling, email notifications…
1.	The extract phase is done by calling ETL service endpoint in OpenShift with Airflow HttpOperator. The URLs for each form being extracted are defined as Airflow variables. The payload is hardcoded in DAG.
2.	Files uploader is implemented as a job on OpenShift and triggered by Airflow KubernetesJobOperator (task PCD_file_upload or MEDIS_file_upload) 
The job spins a pod that executes upload.sh file that creates combined medis_ltc.flag file, uploads all encrypted files, uploads newly created flag file, moves all files into archive directory and also deletes files from archive directory that are older than specified retention period. ([https://proactionca.ent.cgi.com/confluence/display/BCMOHAD/Manual+trigger+for+ETL+process])




## Running Locally

### Pre-requisite

- Docker[https://docs.docker.com/engine/install/]
- Docker Compose[https://docs.docker.com/compose/install/]

### Running locally

- Create "config", "logs" and "plugins" directories on the same level as dags.

- Run the following command to initialize the Docker Containers.

```bash
docker-compose up airflow-init
```

- Run the following command to start the Airflow instance.

```bash
docker-compose up
```

- You can access the Airflow instance at [http://localhost:8080/](http://localhost:8080/), and log in with username "airflow" and password "airflow".

- You can then start or stop the deployment when developing.

### Clean up

- Run the following command to delete all containers and free up memory.

```bash
docker-compose down --volumes --remove-orphans
```