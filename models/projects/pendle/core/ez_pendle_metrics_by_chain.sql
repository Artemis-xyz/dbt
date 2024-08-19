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
    -- , price_data_cte as(
    --     get_coingecko_metrics('pendle')
    -- )
    , tokenholder_count as (
        token_holders('ethereum', '0x808507121B80c02388fAd14726482e061B8da827', '2021-04-27')
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
    -- , p.fdmc
    -- , p.market_cap
    -- , p.token_turnover_fdv
    -- , p.token_turnover_mcap
    -- , p.trading_volume
    , token_holder_count
FROM fees f
LEFT JOIN daus_txns d using(date, chain)
-- LEFT JOIN price_data_cte p using(date, chain)
-- LEFT JOIN fdv_and_turnover using(date, chain)
LEFT JOIN tokenholder_count t using(date, chain)