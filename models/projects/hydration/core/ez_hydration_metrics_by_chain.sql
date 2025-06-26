{{
    config(
        materialized="table",
        snowflake_warehouse="HYDRATION",
        database="hydration",
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
        from {{ ref("fact_hydration_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics('hydradx') }})
select
    f.date
    , 'hydration' as chain
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    -- Cash Flow Metrics
    , coalesce(fees_usd, 0) as fees
    , coalesce(fees_native, 0) as fees_native
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join price_data using(f.date)
where f.date < to_date(sysdate())
