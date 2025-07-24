{{
    config(
        materialized="incremental",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with protocol_data as (
    select
        date
        , app
        , category
        , sum(trading_volume) as trading_volume
        , sum(unique_traders) as unique_traders
        , sum(number_of_swaps) as number_of_swaps
        , sum(gas_cost_usd) as gas_cost_usd

        -- Standardized Metrics
        , sum(spot_dau) as spot_dau
        , sum(spot_fees) as spot_fees
        , sum(spot_txns) as spot_txns
        , sum(spot_volume) as spot_volume
        , sum(tvl) as tvl
        , sum(trading_fees) as trading_fees
        , sum(fees) as fees
        , sum(gas_cost_native) as gas_cost_native
        , sum(gas_cost) as gas_cost

    from {{ ref("ez_trader_joe_metrics_by_chain") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    group by 1, 2, 3
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
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, token_incentives as (
    select
        date
        , sum(amount_usd) as token_incentives
    from {{ ref("fact_trader_joe_token_incentives") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    group by date
)
, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from protocol_data) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics("joe") }}
)

select
    date_spine.date
    , protocol_data.app
    , protocol_data.category

    -- Old Metrics needed for compatibility
    , protocol_data.trading_volume
    , protocol_data.trading_fees
    , protocol_data.unique_traders
    , protocol_data.number_of_swaps
    , protocol_data.gas_cost_usd

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , protocol_data.spot_dau
    , protocol_data.spot_txns
    , protocol_data.spot_volume
    , protocol_data.tvl

    -- Cashflow Metrics
    , protocol_data.spot_fees
    , protocol_data.spot_fees as fees 
    , supply_data.burns_native as revenue
    , revenue - token_incentives.token_incentives as earnings
    , token_incentives.token_incentives
    , protocol_data.gas_cost_native
    , protocol_data.gas_cost

    -- LFJ Token Supply Data
    , supply_data.premine_unlocks_native
    , supply_data.gross_emissions_native
    , supply_data.burns_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native

    -- Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join protocol_data using(date)
left join market_metrics using(date)
left join token_incentives using(date)
left join supply_data using(date)
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
