FROM apache/airflow:2.1.0-python3.8

COPY airflow_provider.txt .
COPY python_requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r airflow_provider.txt
RUN pip install -r python_requirements.txt
