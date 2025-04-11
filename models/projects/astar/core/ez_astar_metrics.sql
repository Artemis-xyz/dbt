{{
    config(
        materialized="table",
        snowflake_warehouse="ASTAR",
        database="astar",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd
        from {{ ref("fact_astar_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics("astar") }})
select
    f.date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees
    -- Standardized Metrics
    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    -- Cash Flow Metrics
    , fees_native as gross_protocol_revenue_native
    , fees as gross_protocol_revenue
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
from fundamental_data f
left join price_data using(f.date)
where f.date < to_date(sysdate())
