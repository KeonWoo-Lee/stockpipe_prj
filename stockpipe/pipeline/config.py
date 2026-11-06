"""Load and validate a pipeline YAML file without exposing resolved secrets."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PipelineConfigError(ValueError):
    """Raised when pipeline.yml is missing or invalid."""


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        resolved = os.environ.get(name)
        if resolved is None or resolved == "":
            raise PipelineConfigError(
                f"필수 환경변수 {name}이 설정되지 않았습니다. 실제 값은 출력하지 않았습니다."
            )
        return resolved

    return _ENV_REF.sub(replace, value)


def require_identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise PipelineConfigError(
            f"{path}는 영문자 또는 밑줄로 시작하는 PostgreSQL 식별자여야 합니다."
        )
    return value


def _require_mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PipelineConfigError(f"{path}는 YAML mapping이어야 합니다.")
    return value


def load_pipeline_config(path: str | Path) -> tuple[dict[str, Any], Path]:
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise PipelineConfigError(f"설정 파일을 찾을 수 없습니다: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise PipelineConfigError(f"pipeline YAML 형식이 올바르지 않습니다: {exc}") from exc

    config = _expand_env(raw)
    config = _require_mapping(config, "root")
    if config.get("version") != 1:
        raise PipelineConfigError("pipeline.yml의 version은 1이어야 합니다.")

    pipeline = _require_mapping(config.get("pipeline"), "pipeline")
    if not isinstance(pipeline.get("name"), str) or not pipeline["name"].strip():
        raise PipelineConfigError("pipeline.name이 필요합니다.")

    source = _require_mapping(config.get("source"), "source")
    if source.get("type") != "mongodb":
        raise PipelineConfigError("현재 runner에서 지원하는 source.type은 mongodb입니다.")
    mongo = _require_mapping(source.get("mongodb"), "source.mongodb")
    for key in ("uri", "database", "collection"):
        if not isinstance(mongo.get(key), str) or not mongo[key]:
            raise PipelineConfigError(f"source.mongodb.{key}가 필요합니다.")
    if not isinstance(mongo.get("filter", {}), dict):
        raise PipelineConfigError("source.mongodb.filter는 YAML mapping이어야 합니다.")
    inference = _require_mapping(
        mongo.get("schema_inference", {}), "source.mongodb.schema_inference"
    )
    sample_size = inference.get("sample_size", 1000)
    if not isinstance(sample_size, int) or sample_size < 1:
        raise PipelineConfigError("schema_inference.sample_size는 양의 정수여야 합니다.")
    hints = inference.get("schema_hints")
    if hints is not None and not isinstance(hints, (str, dict, list)):
        raise PipelineConfigError("schema_inference.schema_hints는 문자열 또는 YAML 구조여야 합니다.")

    flatten = _require_mapping(config.get("flatten"), "flatten")
    if flatten.get("object_policy", "flatten") != "flatten":
        raise PipelineConfigError("현재 object_policy는 flatten만 지원합니다.")
    if flatten.get("array_policy", "child_table") != "child_table":
        raise PipelineConfigError("현재 array_policy는 child_table만 지원합니다.")
    if flatten.get("map_policy", "json_string") not in ("json_string", "error"):
        raise PipelineConfigError("flatten.map_policy는 json_string 또는 error여야 합니다.")
    prefix = flatten.get("table_prefix")
    if not isinstance(prefix, str) or not prefix:
        raise PipelineConfigError("flatten.table_prefix가 필요합니다.")

    landing = _require_mapping(config.get("postgres_landing"), "postgres_landing")
    analytics = _require_mapping(config.get("postgres_analytics"), "postgres_analytics")
    fdw = _require_mapping(config.get("postgres_fdw"), "postgres_fdw")
    for section_name, section in (
        ("postgres_landing", landing),
        ("postgres_analytics", analytics),
    ):
        for key in ("host", "port", "database", "user", "password", "schema"):
            if key not in section or section[key] in (None, ""):
                raise PipelineConfigError(f"{section_name}.{key}가 필요합니다.")
        try:
            section["port"] = int(section["port"])
        except (TypeError, ValueError) as exc:
            raise PipelineConfigError(f"{section_name}.port는 정수여야 합니다.") from exc
        require_identifier(section["schema"], f"{section_name}.schema")

    mode = landing.get("write_mode", "overwrite")
    if mode not in ("overwrite", "append"):
        raise PipelineConfigError("postgres_landing.write_mode는 overwrite 또는 append여야 합니다.")
    landing["write_mode"] = mode
    if not isinstance(landing.get("jdbc_driver_jar"), str):
        raise PipelineConfigError("postgres_landing.jdbc_driver_jar 경로가 필요합니다.")

    for key in (
        "host",
        "port",
        "database",
        "remote_schema",
        "local_schema",
        "server_name",
        "admin_user",
        "admin_password",
        "dbt_user",
        "remote_user",
        "remote_password",
    ):
        if key not in fdw or fdw[key] in (None, ""):
            raise PipelineConfigError(f"postgres_fdw.{key}가 필요합니다.")
    try:
        fdw["port"] = int(fdw["port"])
    except (TypeError, ValueError) as exc:
        raise PipelineConfigError("postgres_fdw.port는 정수여야 합니다.") from exc
    for key in ("remote_schema", "local_schema", "server_name"):
        require_identifier(fdw[key], f"postgres_fdw.{key}")

    dbt = _require_mapping(config.get("dbt"), "dbt")
    for key in ("project_dir", "profile_name", "selector"):
        if not isinstance(dbt.get(key), str) or not dbt[key]:
            raise PipelineConfigError(f"dbt.{key}가 필요합니다.")
    if not isinstance(dbt.get("vars", {}), dict):
        raise PipelineConfigError("dbt.vars는 YAML mapping이어야 합니다.")
    dbt.setdefault("vars", {})
    return config, config_path.parent


def resolve_project_path(value: str, config_dir: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = config_dir / candidate
    return candidate.resolve()
