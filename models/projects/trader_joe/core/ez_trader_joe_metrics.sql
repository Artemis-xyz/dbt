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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with protocol_data as (
    select
        date
        , app
        , category
        
        , sum(coalesce(unique_traders, 0)) as unique_traders
        , sum(coalesce(number_of_swaps, 0)) as number_of_swaps
        , sum(coalesce(gas_cost_usd, 0)) as gas_cost_usd

        -- Standardized Metrics
        , sum(coalesce(spot_dau, 0)) as spot_dau
        , sum(coalesce(spot_fees, 0)) as spot_fees
        , sum(coalesce(spot_txns, 0)) as spot_txns
        , sum(coalesce(spot_volume, 0)) as spot_volume
        , sum(coalesce(tvl, 0)) as tvl
        , sum(coalesce(trading_fees, 0)) as trading_fees
        , sum(coalesce(fees, 0)) as fees
        , sum(coalesce(gas_cost_native, 0)) as gas_cost_native
        , sum(coalesce(gas_cost, 0)) as gas_cost

    from {{ ref("ez_trader_joe_metrics_by_chain") }}
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
)
, token_incentives as (
    select
        date
        , sum(coalesce(amount_usd, 0)) as token_incentives
    from {{ ref("fact_trader_joe_token_incentives") }}
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
    , 'trader_joe' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , protocol_data.spot_dau
    , protocol_data.spot_dau as dau
    , protocol_data.spot_txns
    , protocol_data.spot_txns as txns
    , protocol_data.spot_volume
    , protocol_data.tvl
    , protocol_data.trading_volume as volume

    -- Fee Data
    , protocol_data.spot_fees
    , protocol_data.spot_fees as fees

    -- Financial Statement
    , supply_data.burns_native as revenue
    , token_incentives.token_incentives
    , revenue - token_incentives.token_incentives as earnings

    -- LFJ Token Supply Data
    , supply_data.premine_unlocks_native
    , supply_data.gross_emissions_native
    , supply_data.burns_native
    , supply_data.circulating_supply_native

    -- Token Turnover/Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Bespoke Metrics
    , protocol_data.gas_cost_native
    , protocol_data.gas_cost

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join protocol_data using(date)
left join market_metrics using(date)
left join token_incentives using(date)
left join supply_data using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
