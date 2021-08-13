FROM apache/airflow:2.1.0-python3.8

COPY airflow_provider.txt .

RUN pip install -r airflow_provider.txt

RUN pip install -r python_requirements.txt
