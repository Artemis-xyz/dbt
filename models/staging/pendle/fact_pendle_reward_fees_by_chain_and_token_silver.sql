{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with agg as (
    select *, 'arbitrum' as chain from {{ref('fact_pendle_ethereum_reward_fees')}}
    union all
    select *, 'ethereum' as chain from {{ref('fact_pendle_ethereum_reward_fees')}}
    union all
    select *, 'optimism' as chain from {{ref('fact_pendle_ethereum_reward_fees')}}
)
SELECT
    block_timestamp::date as date
    , chain
    , symbol as token
    , sum(fee) as reward_fees_usd
    , sum(fee_native) as reward_fees_native
FROM agg
where block_timestamp::date < to_date(sysdate())
GROUP BY 1, 2, 3