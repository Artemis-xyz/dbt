{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

with agg as (
    select * from {{ref('fact_pendle_arbitrum_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_ethereum_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_optimism_fees_silver')}}
)
SELECT
    date
    , chain
    , symbol
    , sum(fee_usd) as fees_usd
    , sum(fee_native) as fees_native
FROM agg
where date < to_date(sysdate())
GROUP BY 1, 2, 3