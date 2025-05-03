FROM apache/airflow:3.0.0-python3.11
USER root
RUN apt-get update && apt-get install -y gcc libpq-dev
USER airflow