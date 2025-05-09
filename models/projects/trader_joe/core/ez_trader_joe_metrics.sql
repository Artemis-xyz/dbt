{{
    config(
        materialized="view",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics",
    )
}}

with market_data as (
    {{ get_coingecko_metrics("joe") }}
)
, protocol_data as (
    SELECT
        date
        , app
        , category
        , sum(trading_volume) as trading_volume
        , sum(trading_fees) as trading_fees
        , sum(unique_traders) as unique_traders
        , sum(number_of_swaps) as number_of_swaps
        , sum(gas_cost_usd) as gas_cost_usd

        -- Standardized Metrics
        , sum(spot_dau) as spot_dau
        , sum(spot_txns) as spot_txns
        , sum(spot_volume) as spot_volume
        , sum(tvl) as tvl
        , sum(trading_fees) as trading_fees
        , sum(gross_protocol_revenue) as gross_protocol_revenue
        , sum(gas_cost_native) as gas_cost_native
        , sum(gas_cost) as gas_cost

    FROM {{ ref("ez_trader_joe_metrics_by_chain") }}
    GROUP BY 1, 2, 3
)

, supply_data as (
    select
        date
        , premine_unlocks_native
        , gross_emissions_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
    from {{ ref("fact_trader_joe_supply_data") }}
)
, token_incentives as (
    select
        date
        , sum(amount_usd) as token_incentives_usd
    from {{ ref("fact_trader_joe_token_incentives") }}
    group by date
)
SELECT
    date(date) as date
    , app
    , category
    , trading_volume
    , trading_fees
    , unique_traders
    , number_of_swaps
    , gas_cost_usd

    -- Standardized Metrics

    -- Supply Metrics
    , premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native

    -- Token Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage/Sector Metrics
    , spot_dau
    , spot_txns
    , spot_volume
    , tvl

    -- Money Metrics
    , trading_fees as spot_fees
    , gross_protocol_revenue
    , token_incentives_usd
    , gas_cost_native
    , gas_cost

    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
FROM protocol_data
LEFT JOIN market_data using(date)
LEFT JOIN supply_data using(date)
LEFT JOIN token_incentives using(date)
WHERE date < to_date(sysdate())
