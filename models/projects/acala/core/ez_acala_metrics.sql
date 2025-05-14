--depends_on: {{ ref("fact_acala_rolling_active_addresses") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ACALA",
        database="acala",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as (
        select
            date, chain, daa, txns, fees_native, fees_usd as fees, fees_native * .2 as revenue_native, fees_usd * .2 as revenue
        from {{ ref("fact_acala_fundamental_metrics") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("acala") }}),
    price_data as ({{ get_coingecko_metrics("acala") }})
select
    fundamental_data.date
    , fundamental_data.chain
    , daa as dau
    , txns
    , fees_native
    , fees
    , fees / txns as avg_txn_fee
    , revenue
    , wau
    , mau
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    -- Cashflow metrics
    , fees as chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue_native AS burned_cash_flow_native
    , revenue AS burned_cash_flow
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join price_data on fundamental_data.date = price_data.date
where fundamental_data.date < to_date(sysdate())
