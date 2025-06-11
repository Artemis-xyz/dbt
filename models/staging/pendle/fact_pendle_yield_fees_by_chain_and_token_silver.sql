{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with agg as (
    select *, 'arbitrum' as chain from {{ref('fact_pendle_arbitrum_yield_fees_silver')}}
    union all
    select *, 'ethereum' as chain from {{ref('fact_pendle_ethereum_yield_fees_silver')}}
    union all
    select *, 'optimism' as chain from {{ref('fact_pendle_optimism_yield_fees_silver')}}
)
SELECT
    date
    , chain
    , token
    , sum(yield_fee_usd) as yield_fees_usd
    , sum(yield_fee_native) as yield_fees_native
FROM agg
where date < to_date(sysdate())
GROUP BY 1, 2, 3