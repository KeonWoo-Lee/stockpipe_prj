"""PostgreSQL landing writer and postgres_fdw setup helpers."""

from __future__ import annotations

from typing import Any, Sequence

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame


def _connect_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "host": config["host"],
        "port": int(config["port"]),
        "dbname": config["database"],
        "user": config["user"],
        "password": config["password"],
        "connect_timeout": 10,
    }


def ensure_landing_schema(config: dict[str, Any]) -> None:
    with psycopg.connect(**_connect_kwargs(config)) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(config["schema"])
            )
        )


def write_relation(
    dataframe: DataFrame,
    table_name: str,
    config: dict[str, Any],
) -> None:
    url = (
        f"jdbc:postgresql://{config['host']}:{int(config['port'])}/"
        f"{config['database']}"
    )
    (
        dataframe.write.format("jdbc")
        .option("url", url)
        .option("dbtable", f"{config['schema']}.{table_name}")
        .option("user", config["user"])
        .option("password", config["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode(config.get("write_mode", "overwrite"))
        .save()
    )


def setup_postgres_fdw(
    admin_connection: dict[str, Any],
    fdw: dict[str, Any],
    dbt_user: str,
    remote_user: str,
    remote_password: str,
    landing_tables: Sequence[str],
) -> None:
    """Expose landing tables in PG1 as foreign tables in PG2.

    The FDW schema is pipeline-owned. Existing imported foreign tables with the
    same names are refreshed; dependent dbt views are recreated by the next dbt run.
    """
    if not landing_tables:
        raise ValueError("FDW로 import할 landing table이 없습니다.")
    kwargs = _connect_kwargs(admin_connection)
    server_name = fdw["server_name"]
    remote_options = {
        "host": fdw["host"],
        "port": str(fdw["port"]),
        "dbname": fdw["database"],
    }
    local_schema = fdw["local_schema"]
    remote_schema = fdw["remote_schema"]

    with psycopg.connect(**kwargs) as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(local_schema)
            )
        )

        row = connection.execute(
            "SELECT srvoptions FROM pg_foreign_server WHERE srvname = %s",
            (server_name,),
        ).fetchone()
        if row is None:
            options = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(key), sql.Literal(value))
                for key, value in remote_options.items()
            )
            connection.execute(
                sql.SQL(
                    "CREATE SERVER {} FOREIGN DATA WRAPPER postgres_fdw OPTIONS ({})"
                ).format(sql.Identifier(server_name), options)
            )
        else:
            existing = dict(option.split("=", 1) for option in (row[0] or []))
            if any(existing.get(key) != value for key, value in remote_options.items()):
                raise RuntimeError(
                    f"FDW server {server_name}의 기존 접속 대상이 pipeline.yml과 다릅니다. "
                    "운영 중인 서버 설정은 자동 변경하지 않습니다."
                )

        role_exists = connection.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (dbt_user,)
        ).fetchone()
        if role_exists is None:
            raise RuntimeError(f"PG2에 dbt 접속 역할 {dbt_user}이 없습니다.")

        connection.execute(
            sql.SQL("GRANT USAGE ON FOREIGN SERVER {} TO {}").format(
                sql.Identifier(server_name), sql.Identifier(dbt_user)
            )
        )

        mapping_exists = connection.execute(
            """SELECT 1
               FROM pg_user_mapping um
               JOIN pg_foreign_server fs ON fs.oid = um.umserver
               JOIN pg_roles r ON r.oid = um.umuser
               WHERE fs.srvname = %s AND r.rolname = %s""",
            (server_name, dbt_user),
        ).fetchone()
        if mapping_exists:
            connection.execute(
                sql.SQL("DROP USER MAPPING FOR {} SERVER {}").format(
                    sql.Identifier(dbt_user), sql.Identifier(server_name)
                )
            )
        connection.execute(
            sql.SQL(
                "CREATE USER MAPPING FOR {} SERVER {} OPTIONS (user {}, password {})"
            ).format(
                sql.Identifier(dbt_user),
                sql.Identifier(server_name),
                sql.Literal(remote_user),
                sql.Literal(remote_password),
            )
        )

        for table in landing_tables:
            row = connection.execute(
                """SELECT c.relkind
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = %s AND c.relname = %s""",
                (local_schema, table),
            ).fetchone()
            if row is None:
                continue
            if row[0] != "f":
                raise RuntimeError(
                    f"{local_schema}.{table}은 foreign table이 아닙니다. "
                    "FDW 전용 schema에 다른 relation을 두지 마세요."
                )
            connection.execute(
                sql.SQL("DROP FOREIGN TABLE {}.{} CASCADE").format(
                    sql.Identifier(local_schema), sql.Identifier(table)
                )
            )

        table_identifiers = sql.SQL(", ").join(
            sql.Identifier(table) for table in landing_tables
        )
        connection.execute(
            sql.SQL(
                "IMPORT FOREIGN SCHEMA {} LIMIT TO ({}) FROM SERVER {} INTO {}"
            ).format(
                sql.Identifier(remote_schema),
                table_identifiers,
                sql.Identifier(server_name),
                sql.Identifier(local_schema),
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(local_schema), sql.Identifier(dbt_user)
            )
        )
        for table in landing_tables:
            connection.execute(
                sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                    sql.Identifier(local_schema),
                    sql.Identifier(table),
                    sql.Identifier(dbt_user),
                )
            )
