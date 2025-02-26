{{
    config(
        materialized="table",
        snowflake_warehouse="UNICHAIN",
        database="unichain",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
     price_data as ({{ get_coingecko_metrics('uniswap') }})
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
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    token_volume
from {{ ref("fact_unichain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
where f.date  < to_date(sysdate())
