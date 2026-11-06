"""Read one MongoDB collection into a Spark DataFrame using the Mongo connector."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession


MONGO_CONNECTOR_COORDINATE = "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0"


def create_spark_session(app_name: str, jdbc_driver_jar: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", MONGO_CONNECTOR_COORDINATE)
        .config("spark.jars", jdbc_driver_jar)
        .config("spark.driver.extraClassPath", jdbc_driver_jar)
        .getOrCreate()
    )


def read_mongodb_collection(
    spark: SparkSession, config: dict[str, Any]
) -> DataFrame:
    reader = (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", config["uri"])
        .option("database", config["database"])
        .option("collection", config["collection"])
        .option("sampleSize", str(config.get("schema_inference", {}).get("sample_size", 1000)))
    )
    hints = config.get("schema_inference", {}).get("schema_hints")
    if hints is not None:
        if not isinstance(hints, str):
            hints = json.dumps(hints, separators=(",", ":"))
        reader = reader.option("schemaHints", hints)

    query_filter = config.get("filter") or {}
    if query_filter:
        reader = reader.option(
            "aggregation.pipeline",
            json.dumps([{"$match": query_filter}], separators=(",", ":")),
        )
    return reader.load()
