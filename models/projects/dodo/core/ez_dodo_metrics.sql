{{
    config(
        materialized="table",
        snowflake_warehouse="DODO",
        database="dodo",
        schema="core",
        alias="ez_metrics",
    )
}}

with swap_metrics as (
    select
        block_timestamp::date as date,
        count(distinct tx_hash) as txns,
        count(distinct swapper) as dau,
        sum(amount_usd) as volume_usd,
    from {{ref('fact_dodo_swap_metrics')}}
    group by date
)
, token_incentives as (
    select
        block_timestamp::date as date,
        sum(amount_usd) as token_incentives
    from {{ref('fact_dodo_token_incentives')}}
    group by date
)
, date_spine as (
    select * 
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from swap_metrics) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics("dodo") }}
)

select
    date_spine.date

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    
    -- Usage Metrics
    , swap_metrics.txns as spot_txns
    , swap_metrics.dau as spot_dau
    -- , swap_metrics.volume_usd as spot_volume

    -- Cashflow Metrics
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

from date_spine
left join market_metrics using (date)
left join swap_metrics using (date)
left join token_incentives using (date)
where date_spine.date <= to_date(sysdate())
    