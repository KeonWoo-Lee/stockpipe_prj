"""Flatten Spark StructType objects and split arrays into child relations."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    NullType,
    StructField,
    StructType,
)


@dataclass(frozen=True)
class RelationFrame:
    table_name: str
    dataframe: DataFrame
    array_path: str | None


class FlattenSchemaError(ValueError):
    """Raised for Spark schema shapes that cannot be represented safely."""


def _quoted_field(name: str) -> Column:
    escaped = name.replace("`", "``")
    return F.col(f"`{escaped}`")


def _table_slug(path: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_]+", "_", path.replace(".", "_"))
    value = re.sub(r"_+", "_", value).strip("_").lower()
    if not value:
        value = "array"
    if value[0].isdigit():
        value = f"n_{value}"
    if len(value) > 40:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        value = f"{value[:31]}_{digest}"
    return value


def table_name(prefix: str, array_path: str | None) -> str:
    clean_prefix = _table_slug(prefix)
    suffix = "root" if array_path is None else _table_slug(array_path)
    name = f"{clean_prefix}_{suffix}"
    if len(name) > 63:
        digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
        name = f"{name[:54]}_{digest}"
    return name


def _canonical_id_expr(field: StructField) -> Column:
    expression = _quoted_field(field.name)
    if isinstance(field.dataType, (StructType, ArrayType, MapType)):
        return F.to_json(expression)
    return expression.cast("string")


def _leaf_and_array_fields(
    struct_type: StructType,
    base_expression: Column | None,
    prefix: tuple[str, ...],
    map_policy: str,
) -> tuple[list[tuple[str, Column]], list[tuple[str, Column, DataType]]]:
    leaves: list[tuple[str, Column]] = []
    arrays: list[tuple[str, Column, DataType]] = []

    def visit(field: StructField, expression: Column, path: tuple[str, ...]) -> None:
        dtype = field.dataType
        if isinstance(dtype, StructType):
            for child in dtype.fields:
                visit(child, expression.getField(child.name), path + (child.name,))
        elif isinstance(dtype, ArrayType):
            arrays.append((".".join(path), expression, dtype.elementType))
        elif isinstance(dtype, MapType):
            if map_policy == "error":
                raise FlattenSchemaError(
                    f"MapType 필드 {'.'.join(path)}가 발견되었습니다. schema hint를 지정하거나 "
                    "map_policy를 json_string으로 설정하세요."
                )
            leaves.append((".".join(path), F.to_json(expression)))
        elif isinstance(dtype, NullType):
            leaves.append((".".join(path), expression.cast("string")))
        else:
            leaves.append((".".join(path), expression))

    for field in struct_type.fields:
        path = prefix + (field.name,)
        expression = (
            base_expression.getField(field.name)
            if base_expression is not None
            else _quoted_field(field.name)
        )
        if not prefix and field.name == "_id":
            leaves.append(("_id", expression.cast("string")))
            continue
        visit(field, expression, path)

    names = [name for name, _ in leaves]
    if len(names) != len(set(names)):
        raise FlattenSchemaError("동일한 평탄화 컬럼명이 생성되어 경로 간 충돌이 발생했습니다.")
    return leaves, arrays


def _stable_child_id(parent_id: Column, path: str, index: Column) -> Column:
    payload = F.to_json(
        F.struct(
            parent_id.alias("parent_id"),
            F.lit(path).alias("array_path"),
            index.alias("array_index"),
        )
    )
    return F.sha2(payload, 256)


def build_relations(
    dataframe: DataFrame,
    table_prefix: str,
    map_policy: str = "json_string",
) -> list[RelationFrame]:
    """Return one root relation and one relation for each array path.

    MongoDB's required ``_id`` is normalized to text and is used to derive stable
    ETL record IDs. Array-child IDs derive from parent ID, path, and position.
    """
    if not isinstance(dataframe.schema, StructType):
        raise FlattenSchemaError("MongoDB collection root must infer to a StructType.")
    reserved = {"__etl_record_id", "__etl_parent_record_id", "__etl_array_index", "__etl_source_id"}
    root_names = {field.name for field in dataframe.schema.fields}
    collision = sorted(root_names & reserved)
    if collision:
        raise FlattenSchemaError(f"원본에 예약 ETL 컬럼명이 있습니다: {', '.join(collision)}")
    id_field = next((field for field in dataframe.schema.fields if field.name == "_id"), None)
    if id_field is None:
        raise FlattenSchemaError("MongoDB 문서에 필요한 _id 필드가 없습니다.")

    source_id = _canonical_id_expr(id_field)
    root = (
        dataframe.withColumn("__etl_source_id", source_id)
        .withColumn("__etl_record_id", F.sha2(F.col("__etl_source_id"), 256))
    )
    output: list[RelationFrame] = []
    seen_names: set[str] = set()

    def emit_relation(
        base_df: DataFrame,
        object_type: DataType,
        object_expression: Column | None,
        array_path: str | None,
        is_root: bool,
    ) -> None:
        if isinstance(object_type, ArrayType):
            element_type = object_type.elementType
            if isinstance(element_type, ArrayType):
                raise FlattenSchemaError(
                    f"중첩 배열 {array_path}의 배열 요소가 다시 배열입니다. "
                    "현재 템플릿은 배열 요소가 scalar, struct 또는 map인 경우를 지원합니다."
                )
            if isinstance(element_type, StructType):
                leaves, arrays = _leaf_and_array_fields(
                    element_type, object_expression, tuple(array_path.split(".")), map_policy
                )
            elif isinstance(element_type, MapType):
                if map_policy == "error":
                    raise FlattenSchemaError(f"MapType 배열 {array_path}는 지원하지 않습니다.")
                leaves = [(f"{array_path}.value", F.to_json(object_expression))]
                arrays = []
            else:
                leaves = [(f"{array_path}.value", object_expression)]
                arrays = []
        elif isinstance(object_type, StructType):
            leaves, arrays = _leaf_and_array_fields(
                object_type, object_expression, (), map_policy
            )
        else:
            raise FlattenSchemaError("내부 오류: relation schema가 StructType/ArrayType이 아닙니다.")

        table = table_name(table_prefix, array_path)
        if table in seen_names:
            raise FlattenSchemaError(f"생성 테이블명 충돌: {table}")
        seen_names.add(table)

        meta = [F.col("__etl_record_id"), F.col("__etl_source_id")]
        if not is_root:
            meta.extend(
                [F.col("__etl_parent_record_id"), F.col("__etl_array_index")]
            )
        selected = meta + [expression.alias(name) for name, expression in leaves]
        relation_df = base_df.select(*selected)
        output.append(RelationFrame(table, relation_df, array_path))

        for child_path, array_expression, child_type in arrays:
            child_base = base_df.select(
                F.col("__etl_record_id").alias("__etl_parent_record_id"),
                F.col("__etl_source_id"),
                F.posexplode(array_expression).alias("__etl_array_index", "__etl_element"),
            )
            child_base = child_base.withColumn(
                "__etl_record_id",
                _stable_child_id(
                    F.col("__etl_parent_record_id"),
                    child_path,
                    F.col("__etl_array_index"),
                ),
            )
            emit_relation(
                child_base,
                child_type,
                F.col("__etl_element"),
                child_path,
                False,
            )

    emit_relation(root, dataframe.schema, None, None, True)
    return output
