# Airflow with CeleryExecutor

## Create folders for storing logs, dags and plugins
```
# Create requirement folders
mkdir ./dags ./logs ./plugins ./includes

# Provide airflow have permission for writeing files
sudo chmod -R dags logs plugins inludes
```

## Create environment variables file 

This will make sure user and hook's permission are the same between local machine folders with container folders

The file automatically load by the docker-compose.
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
## Building Docker images
```
docker build -t airflow-custom-image .
```

## Run Airflow services
```
# For airflow instance
docker-compose up airflow-init

# For all services 
docker-compose up
```

## Update and Install extend packages  
### Airflow Version:
[Check supported versions](https://hub.docker.com/r/apache/airflow/tags?page=1&ordering=last_updated)

config in the Dockerfile

```
FROM apache/airflow:<VERSION>
```


### Python and Airflow's Provider packages:
[Check the information of provider](https://registry.astronomer.io/)

config in the Requirements file

## Interact with API example
```
# Example list all dags
curl -X GET --user "airflow:airflow" "http://localhost:8080/api/v1/dags"
```
