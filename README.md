# medis-scheduler
MoH MEDIS project scheduler

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