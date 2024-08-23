{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE"
    )
}}

with agg as (
    select * from {{ref('fact_pendle_arbitrum_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_ethereum_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_optimism_fees_silver')}}
    union all
    select * from {{ref('fact_pendle_bsc_fees_silver')}}
)
SELECT
    date
    , chain
    , symbol
    , sum(fee_usd) as fees
    , fees * 0.8 as revenue
    , fees * 0.2 as supply_side_fees
FROM agg
where date < to_date(sysdate())
GROUP BY 1, 2