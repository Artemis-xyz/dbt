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
, rtoken_market_cap as (
    select
        date
        , rtokens_mc
    from {{ ref("fact_reserve_rtoken_market_cap") }}
)
, forward_filled_data as (
    select
        ds.date
        , dau
        , tvl
        , rtokens_mc
        , price
        , market_cap
        , fdmc
        , token_volume
        
        , token_turnover_circulating
        , token_turnover_fdv

        -- Fill forward market cap and price
        , last_value(market_data.market_cap ignore nulls) over (order by ds.date) as market_cap_filled
        , last_value(market_data.price ignore nulls) over (order by ds.date) as price_filled

    from date_spine ds
    left join tvl using (date)
    left join dau using (date)
    left join market_data using (date)
    left join rtoken_market_cap using (date)
)

, protocol_revenue as (
    select
        date
        , sum(ecosystem_revenue) as ecosystem_revenue
    from {{ ref("fact_reserve_protocol_revenue") }}
    group by date
)

select
    date
    , dau

    -- Standardized Metrics
    , coalesce(ecosystem_revenue, 0) as ecosystem_revenue

    -- Token Metrics
    , coalesce(price, 0) as price
    , coalesce(market_cap, 0) as market_cap
    , coalesce(fdmc, 0) as fdmc
    , coalesce(token_volume, 0) as token_volume

    -- Stablecoin Metrics
    , coalesce(dau, 0) as stablecoin_dau

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl
    , coalesce(rtokens_mc, 0) as rtokens_mc
    -- Turnover Metrics
    , coalesce(token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(token_turnover_fdv, 0) as token_turnover_fdv

    -- Supply Metrics
    , market_cap_filled - lag(market_cap_filled) over (order by date) as net_supply_change_native
    , market_cap_filled / price_filled as circulating_supply_native

from forward_filled_data
left join protocol_revenue using (date)