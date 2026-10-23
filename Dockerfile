FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
		default-jdk-headless \
		&& rm -rf /var/lib/apt/lists/*
USER airflow

COPY --chown=airflow:root pyproject.toml /opt/airflow/
COPY --chown=airflow:root stockpipe/ /opt/airflow/stockpipe/
COPY --chown=airflow:root spark/ /opt/airflow/spark/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

RUN pip install --no-cache-dir -e /opt/airflow
RUN pip install --no-cache-dir pyspark==3.5.3
RUN pip install --no-cache-dir dbt-core dbt-postgres
