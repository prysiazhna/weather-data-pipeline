FROM apache/airflow:2.9.3-python3.12

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
  dbt-core==1.11.2 \
  dbt-postgres==1.10.0