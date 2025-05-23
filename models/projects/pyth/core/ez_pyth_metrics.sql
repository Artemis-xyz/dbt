{{
    config(
        materialized='table',
        snowflake_warehouse='PYTH',
        database='pyth',
        schema='core',
        alias='ez_metrics',
    )
}}

with 
    dau_txns as (
        SELECT * FROM {{ ref('fact_pyth_txns_dau') }}
    )
    , date_spine as (
        SELECT
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date between (SELECT min(date) FROM dau_txns) and to_date(sysdate())
    ),
    market_metrics as (
        {{ get_coingecko_metrics("pyth-network") }}
    )
    , dfl_tvs as (
        SELECT
            date,
            dfl_tvs
        FROM {{ ref('fact_pyth_dfl_tvs') }}
    )
    , supply_data as (
        SELECT
            date,
            premine_unlocks_native,
            net_supply_change_native,
            circulating_supply_native
        FROM {{ ref('fact_pyth_supply_data') }}
    )

SELECT
    date_spine.date

    --Old Metrics needed for compatibility
    , dau_txns.dau
    , dau_txns.txns
    , '0' as fees

    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Usage Metrics
    , dau_txns.txns as oracle_txns
    , dau_txns.dau as oracle_dau
    , dfl_tvs.dfl_tvs as tvs

    --Cash Flow Metrics
    , 0 as oracle_fees
    , 0 as ecosystem_revenue

    --Supply Metrics
    , supply_data.premine_unlocks_native as premine_unlocks_native
    , supply_data.net_supply_change_native as net_supply_change_native
    , supply_data.circulating_supply_native as circulating_supply_native

    --Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
FROM date_spine
LEFT JOIN dau_txns ON date_spine.date = dau_txns.date
LEFT JOIN market_metrics ON date_spine.date = market_metrics.date
LEFT JOIN dfl_tvs ON date_spine.date = dfl_tvs.date
LEFT JOIN supply_data ON date_spine.date = supply_data.date