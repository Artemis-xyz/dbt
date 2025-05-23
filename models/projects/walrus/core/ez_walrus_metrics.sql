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

-- Have to filter out parquet rows where tvl_net_change is not null because it caused duplicates
, clean_parquet AS (
    SELECT *
    FROM {{source('PROD_LANDING', 'raw_sui_ez_walrus_metrics_parquet')}}
    WHERE parquet_raw:tvl_net_change::float IS NULL
)

SELECT DISTINCT
    parquet_raw:date::date AS date
    , parquet_raw:active_blobs::int AS active_blobs
    , parquet_raw:dau::int AS dau
    , parquet_raw:txns::int AS txns

    -- Standardized Metrics
    , (COALESCE(parquet_raw:fees::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS gross_protocol_revenue_usd

    -- Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    -- Chain Metrics
    , (COALESCE(parquet_raw:tvl::float, 0) / 1e9) AS total_staked_native
    , (COALESCE(parquet_raw:tvl::float, 0) / 1e9) * COALESCE(market_data.price, 0) AS total_staked
    
    --Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM clean_parquet
LEFT JOIN market_data ON parquet_raw:date::date = market_data.date