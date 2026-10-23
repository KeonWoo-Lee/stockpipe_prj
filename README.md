[README.md](https://github.com/user-attachments/files/33856672/README.md)
# StockPipe

Korean stock investment monitoring pipeline.

pykrx로 한국 주식 데이터를 수집하고, PySpark로 기술적 지표를 계산한 뒤, Grafana 대시보드로 시각화하는 E2E 데이터 파이프라인입니다.

## Architecture

```
pykrx (KRX)
    |
    v
PostgreSQL (raw_daily_prices)
    |
    v
PySpark (SMA, RSI)
    |
    v
PostgreSQL (spark_technical_indicators)
    |
    v
dbt (staging, mart)
    |
    v
Grafana (dashboard)

[ Airflow - orchestration ]
```

## Tech Stack

| Category | Tool | Role |
|----------|------|------|
| Data Collection | pykrx | KRX 일봉 OHLCV 수집 |
| Storage | PostgreSQL 16 | 원천/가공 데이터 저장 |
| Processing | PySpark 3.5.3 | 기술적 지표 계산 (SMA, RSI) |
| Transformation | dbt | SQL 기반 데이터 정제 (staging, mart) |
| Visualization | Grafana 11.0 | 주가 차트, 이동평균선, RSI 대시보드 |
| Orchestration | Airflow 2.9 | DAG 기반 파이프라인 스케줄링 |
| Infrastructure | Docker / Docker Compose | 컨테이너 기반 인프라 구성 |

## Project Structure

```
stockpipe_prj/
  pyproject.toml          # project dependency manifest
  Dockerfile              # Airflow custom image (JDK + PySpark + stockpipe)
  docker-compose.yml      # PostgreSQL, Airflow, Grafana
  stockpipe/              # data collector (pykrx -> PostgreSQL)
    config.py             #   stock codes, DB connection
    db.py                 #   SQLAlchemy engine/session
    cli.py                #   CLI entrypoint (stockpipe collect --prices)
    collectors/
      base.py             #   abstract base collector
      price.py            #   OHLCV price collector
  spark/                  # preprocessing (PySpark)
    transform.py          #   SMA (5/20/60), RSI (14) calculation
    postgresql-42.7.3.jar #   JDBC driver
  dbt/                    # SQL transformation
    models/
      staging/
        stg_daily_prices.sql
      marts/
        mart_stock_summary.sql
        mart_price_sma.sql
  dags/                   # Airflow DAG
    stockpipe_daily.py    #   collect -> spark -> dbt (weekday 09:30 KST)
  script/
    init_db.sql           # DDL (raw_daily_prices, raw_market_indicators)
```

## Pipeline Flow

1. **Collect** - pykrx로 KRX에서 종목별 일봉 데이터(OHLCV) 수집 후 PostgreSQL에 upsert
2. **Spark Preprocess** - raw_daily_prices를 PySpark로 읽어 기술적 지표 계산
   - SMA (5, 20, 60일 이동평균선)
   - RSI (14일 상대강도지수)
3. **dbt Transform** - staging/mart 레이어로 데이터 정제
4. **Visualize** - Grafana 대시보드에서 주가 차트, 이동평균선, RSI 추이 확인

## Technical Indicators

### SMA (Simple Moving Average)
- SMA 5: 5일 단순 이동평균 (단기 추세)
- SMA 20: 20일 단순 이동평균 (중기 추세)
- SMA 60: 60일 단순 이동평균 (장기 추세)

### RSI (Relative Strength Index)
- 14일 기준 상대강도지수
- RSI > 70: 과매수 구간
- RSI < 30: 과매도 구간

## Quick Start

### Prerequisites
- Docker, Docker Compose
- Python 3.11+
- Java 11+ (PySpark)

### Run

```bash
# start infrastructure
docker compose up -d

# manual execution (host)
stockpipe collect --prices          # collect price data
python3.11 spark/transform.py       # spark preprocessing
cd dbt && dbt run --profiles-dir .  # dbt transformation
```

### Airflow
- Web UI: http://localhost:8080
- DAG `stockpipe_daily` runs weekday 09:30 KST
- Flow: collect_prices -> spark_preprocess -> dbt_run

### Grafana
- Web UI: http://localhost:3000
- Data source: PostgreSQL (stockpipe)
- Tables: `spark_technical_indicators`, `mart_price_sma`

## Environment

- OS: RHEL 8.10
- Python: 3.11.7
- Java: OpenJDK 11
- Docker: Docker Compose v2
