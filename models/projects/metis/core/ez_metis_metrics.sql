{{
    config(
        materialized = "table",
        snowflake_warehouse = "METIS",
        database = "METIS",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_metis_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_metis_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_metis_dau")}}
), price_data as ({{ get_coingecko_metrics("metis-token") }})

select
    coalesce(fees.date, txns.date, daus.date) as date
    , dau
    , txns
    , fees_usd as fees
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , dau as chain_dau
    , txns as chain_txns
    -- Cashflow Metrics
    , fees_usd as gross_protocol_revenue
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 
left join price_data on fees.date = price_data.date