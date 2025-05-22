{{
    config(
        materialized="table",
        snowflake_warehouse="WALRUS",
        database="walrus",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH market_data as ({{ get_coingecko_metrics("walrus-2") }})

SELECT 
    parquet_raw:date::date AS date
    , parquet_raw:active_blobs::int AS active_blobs
    , parquet_raw:dau::int AS dau
    , parquet_raw:fees::float AS fees
    , parquet_raw:revenue::float AS revenue
    , parquet_raw:txns::int AS txns

    -- Standardized Metrics
    , COALESCE(parquet_raw:fees::float, 0) AS gross_protocol_revenue
    , COALESCE(parquet_raw:revenue::float, 0) AS service_cash_flow
    
    -- Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    -- Crypto Metrics
    , COALESCE(parquet_raw:tvl::float, 0) AS tvl
    , COALESCE(parquet_raw:tvl_net_change::float, 0) AS tvl_net_change

    --Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM {{source('PROD_LANDING', 'raw_sui_ez_walrus_metrics_parquet')}}
LEFT JOIN market_data ON parquet_raw:date::date = market_data.date