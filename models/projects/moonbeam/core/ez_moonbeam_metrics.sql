{{
    config(
        materialized="table",
        snowflake_warehouse="MOONBEAM",
        database="moonbeam",
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
        from {{ ref("fact_moonbeam_fundamental_metrics") }}
    ),
    price_data as ({{ get_coingecko_metrics('moonbeam') }})
select
    f.date
    , txns
    , daa as dau
    , coalesce(fees_native, 0) as fees_native
    , coalesce(fees_usd, 0) as fees
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
    , token_turnover_circulating
    , token_turnover_fdv
from fundamental_data f
left join price_data using(f.date)
where f.date < to_date(sysdate())
