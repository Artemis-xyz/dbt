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
    swap_fees as (
        SELECT
            date
            , chain
            , SUM(fees) as fees
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(revenue) as revenue
        FROM
            {{ ref('fact_pendle_swap_fees') }}
        GROUP BY 1, 2
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
            , token_incentives
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
    , coalesce(token_incentives, 0) as token_incentives
    , 0 as operating_expenses
    , token_incentives + operating_expenses as total_expenses
    , protocol_revenue - total_expenses as protocol_earnings
    , d.daus as dau
    , d.daily_txns
FROM fees f
LEFT JOIN daus_txns d using(date, chain)
LEFT JOIN token_incentives_cte using(date, chain)