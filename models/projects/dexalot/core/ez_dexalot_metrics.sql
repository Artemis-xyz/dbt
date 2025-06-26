{{
    config(
        materialized="table",
        snowflake_warehouse="DEXALOT",
        database="DEXALOT",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_dexalot_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("dexalot") }})
select
    f.date
    , chain
    , dau
    , txns
    , fees_native
    , fees_native * price as fees
    , fees / txns as avg_txn_fee
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    -- Cash Flow Metrics
    , fees as chain_fees
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join price_data on f.date = price_data.date
where f.date < to_date(sysdate())
