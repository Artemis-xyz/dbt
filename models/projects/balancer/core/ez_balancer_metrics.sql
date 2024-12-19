{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)

, all_tvl as (
    SELECT
        date,
        sum(tvl_native) as tvl_native,
        sum(tvl_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1
)
, token_holders as (
    SELECT
        date,
        token_holder_count
    FROM {{ ref('fact_balancer_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('balancer') }}
)

select
    date_spine.date,
    all_tvl.tvl_native,
    all_tvl.tvl_usd,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume,
    token_holders.token_holder_count
from date_spine
left join all_tvl on date_spine.date = all_tvl.date
left join token_holders on date_spine.date = token_holders.date
left join market_data on date_spine.date = market_data.date