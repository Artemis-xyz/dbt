{{
    config(
        materialized = "table",
        snowflake_warehouse = "FLARE",
        database = "FLARE",
        schema = "core",
        alias = "ez_metrics"
    )
}}

with fees as (
    select
        date,
        fees_usd
    from {{ref("fact_flare_fees")}}
),
txns as (
    select
        date,
        txns
    from {{ref("fact_flare_txns")}}
)
, daus as (
    select
        date,
        dau
    from {{ref("fact_flare_dau")}}
)
, dex_volumes as (
    select
        date,
        daily_volume as dex_volumes
    from {{ref("fact_flare_daily_dex_volumes")}}
)
select
    coalesce(fees.date, txns.date, daus.date) as date
    , dau
    , txns
    , fees_usd as fees
    -- Standardized Metrics
    -- Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    -- Cashflow metrics
    , fees_usd AS gross_protocol_revenue
    -- Chain Metrics
    , dex_volumes
from fees
left join txns on fees.date = txns.date
left join daus on fees.date = daus.date 
left join dex_volumes on fees.date = dex_volumes.date