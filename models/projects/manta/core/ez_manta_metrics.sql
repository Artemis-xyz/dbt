{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA",
        database="manta",
        schema="core",
        alias="ez_metrics"
    )
}}
with 
    price_data as ({{ get_coingecko_metrics("manta-network") }})
   , defillama_data as ({{ get_defillama_metrics("manta") }})
SELECT
    date
    , daily_txns as txns
    , dau
    , fees
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , dex_volumes as chain_dex_volume
    , tvl
    -- Cashflow Metrics
    , fees AS gross_protocol_revenue
FROM {{ ref('fact_manta_txns_daa') }}
LEFT JOIN price_data using (date)
LEFT JOIN defillama_data using (date)
