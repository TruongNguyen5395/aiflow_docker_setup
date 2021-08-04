# Setup the fast Airflow container with other services

## Create folders for storing logs, dags and plugins
```
mkdir .dags/ .logs/ .plugins/
```

## Create variables file
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

## Initialize the Airflow instance with the job airflowdb init
```
docker-compose up airflow-init
```

## Run all services
```
docker-compose up
```

## Interact with API
```
# Example list all dags
curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags"
```
