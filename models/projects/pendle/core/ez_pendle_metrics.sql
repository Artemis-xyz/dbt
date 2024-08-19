{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fees as (
        SELECT
            date
            , fees
            , SUM(supply_side_fees) as supply_side_fees
            , SUM(revenue) as revenue
        FROM
            {{ ref('fact_pendle_fees') }}
        GROUP BY 1
    )
    , daus_txns as (
        SELECT
            date
            , SUM(daus) as daus
            , SUM(daily_txns) as daily_txns
        FROM
            {{ ref('fact_pendle_daus_txns') }}
        GROUP BY 1
    )
    , price_data_cte as(
        get_coingecko_metrics('pendle')
    )
    , tokenholder_count as (
        token_holders('ethereum', '0x808507121B80c02388fAd14726482e061B8da827', '2021-04-27')
    )

SELECT
    f.date
    , f.fees
    , f.supply_side_fees as primary_supply_side_revenue
    , 0 as secondary_supply_side_revenue
    , f.revenue as protocol_revenue
    , d.daus
    , d.daily_txns
    , p.fdmc
    , p.market_cap
    -- , t.token_turnover_fdv
    -- , t.token_turnover_mcap
    -- , t.trading_volume
    , token_holder_count
FROM fees f
LEFT JOIN daus_txns d using(date)
LEFT JOIN price_data_cte p using(date)
-- LEFT JOIN fdv_and_turnover using(date)
LEFT JOIN tokenholder_count t using(date)