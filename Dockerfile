FROM apache/airflow:2.1.0-python3.8

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt --use-feature=2020-resolver