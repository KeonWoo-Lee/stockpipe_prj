-- scripts/init_db.sql
CREATE TABLE IF NOT EXISTS raw_daily_prices (
    stock_code   VARCHAR(10)  NOT NULL,
    stock_name   VARCHAR(50)  NOT NULL,
    trade_date   DATE         NOT NULL,
    open_price   NUMERIC      NOT NULL,
    high_price   NUMERIC      NOT NULL,
    low_price    NUMERIC      NOT NULL,
    close_price  NUMERIC      NOT NULL,
    volume       BIGINT       NOT NULL,
    created_at   TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (stock_code, trade_date)
);

CREATE TABLE IF NOT EXISTS raw_market_indicators (
    indicator_date  DATE         NOT NULL,
    indicator_name  VARCHAR(50)  NOT NULL,
    indicator_value NUMERIC      NOT NULL,
    created_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (indicator_date, indicator_name)
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    id          SERIAL       PRIMARY KEY,
    check_name  VARCHAR(100) NOT NULL,
    status      VARCHAR(10)  NOT NULL,
    message     TEXT,
    checked_at  TIMESTAMP    DEFAULT NOW()
);
