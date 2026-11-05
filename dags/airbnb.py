from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="s2r_airbnb_dbt",
    start_date=datetime(2026, 11, 1),
    schedule=None,  # 우선 Airflow UI에서 수동 실행
    catchup=False,
    tags=["s2r", "dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "dbt deps "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt "
            "--select tag:s2r_airbnb"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt "
            "--select tag:s2r_airbnb"
        ),
    )

    dbt_deps >> dbt_run >> dbt_test
