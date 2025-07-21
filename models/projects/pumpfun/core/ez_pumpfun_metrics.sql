{{
    config(
        materialized='table',
        snowflake_warehouse='ANALYTICS_XL',
        database='PUMPFUN',
        schema='core',
        alias='ez_metrics',
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and (to_date(sysdate()) - 1)
)

, pumpswap_metrics as (
    select
        date,
        'pumpswap' as version,
        spot_dau,
        spot_txns,
        spot_volume,
        spot_fees as spot_protocol_fees,
        spot_lp_fees
    from {{ ref('fact_pumpswap_metrics') }}
    where date >= '2025-03-20'
)
, pumpfun_metrics as (
    select
        date,
        launchpad_dau,
        launchpad_txns,
        launchpad_volume,
        launchpad_fees
    from {{ ref('fact_pumpfun_metrics') }}
)


, market_metrics as ( {{ get_coingecko_metrics("pump-fun") }})
-- Final combined query with one row per day
select
    date_spine.date

    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc
    
    --Usage Metrics
    , pumpfun_metrics.launchpad_dau
    , pumpfun_metrics.launchpad_txns
    , pumpfun_metrics.launchpad_volume
    , pumpswap_metrics.spot_dau
    , pumpswap_metrics.spot_txns
    , pumpswap_metrics.spot_volume
    
    --Cashflow Metrics
    , pumpswap_metrics.spot_protocol_fees
    , pumpswap_metrics.spot_lp_fees
    , pumpswap_metrics.spot_protocol_fees + pumpswap_metrics.spot_lp_fees as spot_fees
    , pumpfun_metrics.launchpad_fees
    , pumpswap_metrics.spot_protocol_fees + pumpswap_metrics.spot_lp_fees + pumpfun_metrics.launchpad_fees as fees

    -- Financial Statement Metrics
    , 0 as revenue

    --Token Turnover Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

from date_spine
left join pumpfun_metrics using(date)
left join pumpswap_metrics using(date)
left join market_metrics using(date)
where date_spine.date < to_date(sysdate())
order by date desc