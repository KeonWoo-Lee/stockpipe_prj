from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "stockpipe",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stockpipe_daily",
    default_args=default_args,
    description="Daily stock data collection and transformation",
    schedule="30 9 * * 1-5",
    start_date=datetime(2025, 10, 20),
    catchup=False,
) as dag:

    collect_prices = BashOperator(
        task_id="collect_prices",
        bash_command="cd /opt/airflow && pip install -e . 2>/dev/null; stockpipe collect --prices",
    )

    spark_preprocess = BashOperator(
        task_id="spark_preprocess",
        bash_command="cd /opt/airflow/spark && python transform.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )

    collect_prices >> spark_preprocess >> dbt_run
