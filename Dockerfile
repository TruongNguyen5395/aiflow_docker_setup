FROM apache/airflow:2.1.3-python3.8

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends zsh \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && apt-get install --reinstall -y build-essential \
 && rm -rf /var/lib/aptttt/list/*

USER airflow
COPY airflow_provider.txt .
COPY python_requirements.txt .

RUN pip3 install --upgrade pip

RUN pip3 --no-cache-dir install -r airflow_provider.txt
RUN pip3 --no-cache-dir install -r python_requirements.txt

