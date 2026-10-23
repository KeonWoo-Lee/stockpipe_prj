
  create view "stockpipe"."public"."mart_stock_summary__dbt_tmp"
    
    
  as (
    WITH latest AS (
    SELECT
        stock_code,
        stock_name,
        trade_date,
        close_price,
        volume,
        LAG(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date) AS prev_close
    FROM "stockpipe"."public"."stg_daily_prices"
)
SELECT
    stock_code,
    stock_name,
    trade_date,
    close_price,
    volume,
    ROUND((close_price - prev_close) / prev_close * 100, 2) AS change_pct
FROM latest
WHERE trade_date = (SELECT MAX(trade_date) FROM latest)
  );