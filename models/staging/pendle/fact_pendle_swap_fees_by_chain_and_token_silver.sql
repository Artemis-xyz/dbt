{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
    )
}}

with agg as (
    select * from {{ref('fact_pendle_arbitrum_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_base_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_bsc_fees_silver')}}
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
    , sum(volume_usd) as volume_usd
    , sum(volume_native) as volume_native
    , sum(revenue_usd) as revenue_usd
    , sum(revenue_native) as revenue_native
    , sum(supply_side_fees_usd) as supply_side_fees_usd
    , sum(supply_side_fees_native) as supply_side_fees_native
FROM agg
where date < to_date(sysdate())
GROUP BY 1, 2, 3