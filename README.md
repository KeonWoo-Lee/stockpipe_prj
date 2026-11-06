# 파이프라인

## 0.1.0 ~ 0.2.0 주식 데이터 전용 데이터 파이프라인

```text
pykrx → PostgreSQL 원천 테이블 → PySpark 지표 계산 → dbt 모델 → Grafana
```

## 0.3.0 MongoDB 반정형 데이터 기반 데이터 파이프라인

```text
MongoDB collection
    → Spark DataFrame
    → 중첩 object 평탄화 및 array 자식 테이블 분리
    → PostgreSQL landing DB (PG1)
    → postgres_fdw
    → dbt staging / intermediate / marts
    → PostgreSQL 분석 DB (PG2)
    → Grafana
```

MongoDB 입력 연결, 데이터베이스/컬렉션, 평탄화 옵션, PG 접속 설정, dbt 선택자는 프로젝트 루트의 `pipeline.yml`에서 관리합니다. 접속 비밀번호 등 민감한 값은 YAML에 저장하지 않고 환경변수에서 읽습니다. 현재 예시 설정은 MongoDB Atlas 샘플의 `sample_airbnb.listingsAndReviews`를 대상으로 합니다.

## 기술 스택

| 구분 | 기술 | 역할 |
|---|---|---|
| 수집 | pykrx, MongoDB Spark Connector | 주식 데이터 및 MongoDB 문서 읽기 |
| 처리 | PySpark 3.5.3 | 주식 지표 계산, 중첩 문서 평탄화 |
| 저장 | PostgreSQL 16 | 원천/landing 데이터와 분석 결과 저장 |
| 모델링 | dbt | staging, intermediate, marts SQL 모델 실행 |
| 시각화 | Grafana 11 | 주식 지표와 데이터 마트 시각화 |
| 실행 환경 | Docker Compose, Airflow 2.9 | 서비스 실행 환경 및 기존 주식 DAG 운영 |

## 저장 및 평탄화 정책

- 중첩 object의 scalar 값은 `address.location.type`처럼 경로를 컬럼명에 포함해 같은 테이블에 펼칩니다.
- array는 각각 별도 자식 테이블로 분리합니다. 자식 테이블에는 `__etl_record_id`, `__etl_parent_record_id`, `__etl_array_index`를 두어 원본 문서 및 배열 위치를 추적합니다.
- MongoDB의 `_id`는 문자열로 보존하고 `__etl_source_id`를 기준으로 ETL 레코드 ID를 만듭니다.
- map은 기본 설정에서 JSON 문자열로 보존합니다. `map_policy: error`로 설정하면 map 타입을 만났을 때 중단합니다.
- 현재 구현은 MongoDB 입력, object 평탄화, array 자식 테이블 분리를 지원합니다. JSON/XML 파일 입력이나 Airflow DAG를 통한 신규 파이프라인 스케줄링은 아직 범위에 포함되지 않습니다.

## 설정

`pipeline.yml`에서 다음 항목을 조정합니다.

| 섹션 | 설정 내용 |
|---|---|
| `source.mongodb` | URI, 데이터베이스, 컬렉션, 필터, 스키마 추론 옵션 |
| `flatten` | object/array/map 처리 정책과 landing 테이블 접두사 |
| `postgres_landing` | Spark 결과를 적재할 PG1 접속 정보 및 쓰기 모드 |
| `postgres_analytics` | dbt 결과를 저장할 PG2 접속 정보와 스키마 |
| `postgres_fdw` | PG1 landing 테이블을 PG2에 외부 테이블로 노출하는 설정 |
| `dbt` | dbt 프로젝트, 모델 selector, 테스트 실행 여부 |

`${S2R_...}` 형태의 항목은 실행 환경변수에서 값을 가져옵니다. 저장소에 비밀번호를 추가하지 말고, 실행 환경 또는 로컬 전용 환경변수 파일을 사용하세요. 이 설정에서 PG2의 `postgres_fdw` 확장을 만들 수 있는 권한과 PG1 landing 테이블을 읽을 계정이 필요합니다. 두 DB는 같은 PostgreSQL 서버에 있어도 되지만, 서로 다른 데이터베이스라면 `postgres_fdw`가 연결을 담당합니다.

Compose는 현재 Airflow 컨테이너에 `pipeline.yml`을 읽기 전용으로 마운트하고 S2R 관련 환경변수를 전달합니다. 환경변수를 변경한 경우 컨테이너를 다시 생성해야 전달된 값이 반영됩니다.

## 실행

Docker Compose로 서비스를 준비한 뒤 Airflow 컨테이너에서 설정 검증을 먼저 실행합니다.

```bash
docker compose up -d --build airflow
docker compose exec airflow \
  python -m stockpipe.pipeline.runner \
  --config /opt/airflow/pipeline.yml \
  --validate-only
```

검증이 통과하면 실제 배치를 실행합니다.

```bash
docker compose exec airflow \
  python -m stockpipe.pipeline.runner \
  --config /opt/airflow/pipeline.yml
```

실행 순서는 MongoDB 읽기 → Spark 평탄화 → PG1 적재 → PG2의 FDW 외부 테이블 갱신 → `dbt deps` → 선택된 dbt 모델 실행 → 설정된 경우 `dbt test`입니다. `pipeline.yml`의 기본 write mode는 `overwrite`이므로 반복 실행 전 데이터 덮어쓰기 범위를 확인하세요. dbt 패키지는 `dbt/packages.yml`에 선언된 항목을 실행 시 `dbt deps`로 설치합니다.

이 배치 runner는 현재 Airflow DAG에 연결되어 있지 않습니다. Airflow 컨테이너는 실행 환경으로 이용하고 명령은 수동 실행합니다.

## 주요 파일

| 파일/디렉토리 | 역할 |
|---|---|
| `pipeline.yml` | MongoDB → Spark → PG1 → FDW → dbt → PG2 배치 설정 |
| `stockpipe/pipeline/config.py` | YAML 및 환경변수 로드, 설정 유효성 검사 |
| `stockpipe/pipeline/mongo_source.py` | Spark session 및 MongoDB collection reader 구성 |
| `stockpipe/pipeline/flatten.py` | nested object 평탄화, array별 자식 DataFrame 생성 |
| `stockpipe/pipeline/postgres.py` | PG1 JDBC 적재 및 PG2 FDW 설정 |
| `stockpipe/pipeline/dbt_runner.py` | 임시 dbt profile 생성, deps/run/test 실행 |
| `stockpipe/pipeline/runner.py` | 위 단계를 순서대로 실행하는 CLI 진입점 |
| `dbt/models/sources/s2r_airbnb_sources.yml` | PG2의 FDW landing schema를 dbt source로 정의 |
| `docker-compose.yml` | 서비스 구성, pipeline 설정 마운트, 환경변수 전달 |
| `Dockerfile` | Airflow 실행 이미지에 Java 및 프로젝트 의존성 설치 |

## 프로젝트 환경

- Docker / Docker Compose
- Python 프로젝트 의존성은 `pyproject.toml` 참고
- Spark 3.5.3, PostgreSQL JDBC 42.7.3
- MongoDB Spark Connector 10.7.0 (`_2.12` artifact)
- Java 런타임은 PySpark 및 커넥터 호환 버전으로 구성
- Grafana: `http://localhost:3000`
- Airflow: `http://localhost:8080`
