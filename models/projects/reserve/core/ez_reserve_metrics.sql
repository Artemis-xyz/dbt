{{
    config(
        materialized="table",
        database = 'RESERVE',
        schema = 'core',
        snowflake_warehouse = 'RESERVE',
        alias = 'ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2021-10-01' and to_date(sysdate())
)
, dau as (
    select
        date
        , dau
    from {{ ref("fact_reserve_dau") }}
)
, tvl as (
    select
        date
        , tvl
    from {{ ref("fact_reserve_tvl") }}
)
, market_data as (
    {{ get_coingecko_metrics('reserve-rights-token') }}
)

select
    date

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- Stablecoin Metrics
    , coalesce(dau, 0) as stablecoin_dau

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl

    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv
    
from date_spine
left join tvl using (date)
left join dau using (date)
left join market_data using (date)