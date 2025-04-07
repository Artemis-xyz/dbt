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
    , revenue
    , revenue_native
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    -- Cash Flow Metrics
    , fees as gross_protocol_revenue
    , fees_native as gross_protocol_revenue_native
    , cost as l1_cash_flow
    , cost_native as l1_cash_flow_native
    , revenue as foundation_cash_flow
    , revenue_native as foundation_cash_flow_native
from fundamental_data f
left join price_data using(f.date)
where f.date  < to_date(sysdate())
