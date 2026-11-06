"""Config-driven MongoDB -> Spark -> PG1 -> FDW -> dbt -> PG2 batch runner."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from stockpipe.pipeline.config import load_pipeline_config, resolve_project_path
from stockpipe.pipeline.dbt_runner import run_dbt
from stockpipe.pipeline.flatten import build_relations
from stockpipe.pipeline.mongo_source import create_spark_session, read_mongodb_collection
from stockpipe.pipeline.postgres import ensure_landing_schema, setup_postgres_fdw, write_relation

LOGGER = logging.getLogger("stockpipe.pipeline")


def _validate_input(dataframe: DataFrame) -> int:
    if "_id" not in dataframe.columns:
        raise ValueError("MongoDB collection에 _id 필드가 없습니다.")
    if dataframe.where(F.col("`_id`").isNull()).limit(1).count():
        raise ValueError("MongoDB collection에서 null _id 문서를 발견했습니다.")
    count = dataframe.count()
    if count == 0:
        raise ValueError("MongoDB collection filter 결과가 0건입니다.")
    return count


def _validate_config_only(config: dict[str, Any], config_dir: Path) -> None:
    project_dir = resolve_project_path(config["dbt"]["project_dir"], config_dir)
    if not project_dir.is_dir():
        raise ValueError(f"dbt project directory를 찾을 수 없습니다: {project_dir}")
    jar = resolve_project_path(
        config["postgres_landing"]["jdbc_driver_jar"], config_dir
    )
    if not jar.is_file():
        raise ValueError(f"PostgreSQL JDBC jar를 찾을 수 없습니다: {jar}")
    print(
        "설정 검증 완료: "
        f"pipeline={config['pipeline']['name']}, "
        f"source=mongodb, "
        f"landing={config['postgres_landing']['database']}/"
        f"{config['postgres_landing']['schema']}, "
        f"analytics={config['postgres_analytics']['database']}/"
        f"{config['postgres_analytics']['schema']}, "
        f"dbt_selector={config['dbt']['selector']}"
    )


def run_pipeline(config: dict[str, Any], config_dir: Path) -> None:
    landing = config["postgres_landing"]
    analytics = config["postgres_analytics"]
    fdw = config["postgres_fdw"]
    source = config["source"]["mongodb"]
    flatten = config["flatten"]

    jdbc_driver_jar = resolve_project_path(landing["jdbc_driver_jar"], config_dir)
    if not jdbc_driver_jar.is_file():
        raise FileNotFoundError(f"PostgreSQL JDBC jar를 찾을 수 없습니다: {jdbc_driver_jar}")

    ensure_landing_schema(landing)
    spark = create_spark_session(config["pipeline"]["name"], str(jdbc_driver_jar))
    spark.sparkContext.setLogLevel("WARN")
    dataframe = None
    try:
        dataframe = read_mongodb_collection(spark, source).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        source_count = _validate_input(dataframe)
        LOGGER.info(
            "MongoDB read complete: database=%s collection=%s documents=%d",
            source["database"],
            source["collection"],
            source_count,
        )
        relations = build_relations(
            dataframe,
            table_prefix=flatten["table_prefix"],
            map_policy=flatten.get("map_policy", "json_string"),
        )
        for relation in relations:
            write_relation(
                relation.dataframe,
                relation.table_name,
                landing,
            )
            LOGGER.info(
                "Landing relation written: %s.%s",
                landing["schema"],
                relation.table_name,
            )
    finally:
        if dataframe is not None:
            dataframe.unpersist()
        spark.stop()

    table_names = [relation.table_name for relation in relations]
    admin_connection = dict(analytics)
    admin_connection["user"] = fdw["admin_user"]
    admin_connection["password"] = fdw["admin_password"]
    fdw_options = {
        "host": fdw["host"],
        "port": fdw["port"],
        "database": fdw["database"],
        "remote_schema": fdw["remote_schema"],
        "local_schema": fdw["local_schema"],
        "server_name": fdw["server_name"],
    }
    setup_postgres_fdw(
        admin_connection=admin_connection,
        fdw=fdw_options,
        dbt_user=analytics["user"],
        remote_user=fdw["remote_user"],
        remote_password=fdw["remote_password"],
        landing_tables=table_names,
    )
    LOGGER.info(
        "FDW sources refreshed: schema=%s relations=%d",
        fdw["local_schema"],
        len(table_names),
    )

    dbt_project = resolve_project_path(config["dbt"]["project_dir"], config_dir)
    run_dbt(config, dbt_project, fdw["local_schema"])
    LOGGER.info("Pipeline completed: %s", config["pipeline"]["name"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a configured MongoDB-to-PostgreSQL batch pipeline."
    )
    parser.add_argument("--config", default="pipeline.yml", help="pipeline YAML path")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate config and required local paths without connecting to services",
    )
    args = parser.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    config, config_dir = load_pipeline_config(args.config)
    if args.validate_only:
        _validate_config_only(config, config_dir)
        return
    run_pipeline(config, config_dir)


if __name__ == "__main__":
    main()
