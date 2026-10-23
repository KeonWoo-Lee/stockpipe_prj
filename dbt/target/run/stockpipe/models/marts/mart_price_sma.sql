
  create view "stockpipe"."public"."mart_price_sma__dbt_tmp"
    
    
  as (
    SELECT
    stock_code,
    stock_name,
    trade_date,
    close_price,
    AVG(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_5,
    AVG(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
    AVG(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS sma_60
FROM "stockpipe"."public"."stg_daily_prices"
  );