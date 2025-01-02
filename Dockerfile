FROM apache/airflow:2.10.4

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pg-bulk-ingest==0.0.42 psycopg==3.1.10
