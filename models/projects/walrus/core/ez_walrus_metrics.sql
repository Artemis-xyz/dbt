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

SELECT DISTINCT
    parquet_raw:date::date AS date
    , parquet_raw:active_blobs::int AS active_blobs
    , parquet_raw:dau::int AS dau
    , (COALESCE(parquet_raw:fees::float, 0) / 1e9) AS fees_native
    , (COALESCE(parquet_raw:revenue::float, 0) / 1e9) AS revenue_native
    , (COALESCE(parquet_raw:fees::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS fees_usd
    , (COALESCE(parquet_raw:revenue::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS revenue_usd
    , parquet_raw:txns::int AS txns

    -- Standardized Metrics
    , (COALESCE(parquet_raw:fees::float, 0) / 1e9) AS gross_protocol_revenue_native
    , (COALESCE(parquet_raw:revenue::float, 0) / 1e9) AS service_cash_flow_native
    , (COALESCE(parquet_raw:fees::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS gross_protocol_revenue_usd
    , (COALESCE(parquet_raw:revenue::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS service_cash_flow_usd
    
    -- Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    -- Crypto Metrics
    , (COALESCE(parquet_raw:tvl::float, 0) / 1e9) AS tvl_native
    , (COALESCE(parquet_raw:tvl::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS tvl
    
    --Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM {{source('PROD_LANDING', 'raw_sui_ez_walrus_metrics_parquet')}}
LEFT JOIN market_data ON parquet_raw:date::date = market_data.date