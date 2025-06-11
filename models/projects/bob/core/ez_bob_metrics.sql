{{
    config(
        materialized="table",
        snowflake_warehouse="BOB",
        database="bob",
        schema="core",
        alias="ez_metrics",
    )
}}
with 
    fundamental_data as (
        select
            date 
            , txns
            , daa
            , fees_native
            , fees
            , cost
            , cost_native
            , revenue
            , revenue_native
        from {{ ref("fact_bob_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics('bob-build-on-bitcoin') }})
select
    f.date
    , txns
    , daa as dau
    , fees_native
    , fees
    , cost
    , cost_native
    -- we leave revenue and revenue_native untouched as there isn't much information about bob
    , revenue
    , revenue_native
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_fee_allocation
    , cost_native as l1_fee_allocation_native
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join price_data using(f.date)
where f.date  < to_date(sysdate())
