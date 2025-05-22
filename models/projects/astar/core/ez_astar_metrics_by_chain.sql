{{
    config(
        materialized="table",
        snowflake_warehouse="ASTAR",
        database="astar",
        schema="core",
        alias="ez_metrics_by_chain",
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
    , 'astar' as chain
    -- Standardized Metrics
    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    -- Cash Flow Metrics
    , coalesce(fees_native, 0) as ecosystem_revenue_native
    , coalesce(fees_usd, 0) as ecosystem_revenue
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
from fundamental_data f
left join price_data using(f.date)
where f.date < to_date(sysdate())
