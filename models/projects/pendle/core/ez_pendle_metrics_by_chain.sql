{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    fees as (
        SELECT
            date
            , chain
            , fees
            , supply_side_fees
            , revenue
        FROM
            {{ ref('fact_pendle_fees') }}
    )
    , daus_txns as (
        SELECT
            date
            , chain
            , daus
            , daily_txns
        FROM
            {{ ref('fact_pendle_daus_txns') }}
    )
    , token_incentives_cte as (
        SELECT
            date
            , chain
            , token_incentives as token_incentives
        FROM
            {{ref('fact_pendle_token_incentives_by_chain')}}
    )
    , tokenholder_count as (
        select * from {{ref('fact_pendle_token_holders')}}
    )


SELECT
    f.date
    , chain
    , f.fees
    , f.supply_side_fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , f.revenue as protocol_revenue
    , d.daus
    , d.daily_txns
    , token_incentives
FROM fees f
LEFT JOIN daus_txns d using(date, chain)
LEFT JOIN token_incentives_cte using(date, chain)