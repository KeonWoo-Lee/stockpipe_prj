
  create view "stockpipe"."public"."stg_daily_prices__dbt_tmp"
    
    
  as (
    SELECT
    stock_code,
    stock_name,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM raw_daily_prices
  );