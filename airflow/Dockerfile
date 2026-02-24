FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
  "apache-airflow-providers-google>=10.0.0" \
  "google-cloud-bigquery>=3.17.0" \
  "dbt-bigquery>=1.8.0"