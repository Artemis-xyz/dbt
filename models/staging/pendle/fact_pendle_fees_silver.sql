{{
    config(
        materialized = 'table'
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
    , sum(fees) as fees
    , fees * 0.8 as revenue
    , fees * 0.2 as supply_side_fees
FROM agg
GROUP BY 1, 2
where date < current_date()