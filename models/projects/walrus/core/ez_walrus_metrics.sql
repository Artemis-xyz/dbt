{{
    config(
        materialized="table",
        snowflake_warehouse="WALRUS",
        database="walrus",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH date_spine AS (
    SELECT date
    FROM {{ref('dim_date_spine')}}
    WHERE date BETWEEN (SELECT MIN(date) FROM {{ref('fact_walrus_certified_blobs_bigquery')}}) AND to_date(sysdate())
), 

tvl AS (
    SELECT 
        date,
        tvl
    FROM {{ref('fact_walrus_tvl_bigquery')}}
), 

dau AS (
    SELECT 
        date,
        dau
    FROM {{ref('fact_walrus_txns_dau_bigquery')}}
), 

txns AS (
    SELECT 
        date,
        txns
    FROM {{ref('fact_walrus_txns_dau_bigquery')}}
), 

fees AS (
    SELECT 
        date,
        fees
    FROM {{ref('fact_walrus_fees_bigquery')}}
), 

active_blobs AS (
    SELECT 
        certified_date AS date,
        count(distinct blob_id) AS active_blobs
    FROM {{ref('fact_walrus_active_blobs_bigquery')}}
    GROUP BY 1
), 

market_data as ({{ get_coingecko_metrics("walrus-2") }})

SELECT 
    date
    , coalesce(dau.dau, 0) as dau
    , coalesce(txns.txns, 0) as txns
    , coalesce(fees.fees, 0) as fees
    , coalesce(active_blobs.active_blobs, 0) as active_blobs
    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume

    --Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl, 0) - LAG(coalesce(tvl.tvl, 0)) OVER (ORDER BY date) as tvl_net_change

    --Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv

FROM date_spine
LEFT JOIN dau USING(date)
LEFT JOIN txns USING(date)
LEFT JOIN fees USING(date)
LEFT JOIN active_blobs USING(date)
LEFT JOIN market_data USING(date)
LEFT JOIN tvl USING(date)
    
    