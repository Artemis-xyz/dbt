{{
    config(
        materialized = 'table'
    )
}}

with agg as (
    select * from {{ref('fact_pendle_arbitrum_daus_txns_silver')}}
    union all
    select * from {{ref('fact_pendle_ethereum_daus_txns_silver')}}
    union all
    select * from {{ref('fact_pendle_optimism_daus_txns_silver')}}
    union all
    select * from {{ref('fact_pendle_bsc_daus_txns_silver')}}
)
SELECT
    date
    , chain
    , sum(daus) as daus
    , sum(txns) as daily_txns
FROM agg
GROUP BY 1, 2
where date < current_date()