{{
    config(
        materialized="incremental",
        snowflake_warehouse="SONIC",
        database="sonic",
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

with 
    sonic_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_sonic_daily_dex_volumes") }}
    )
    , fundamentals as (
        SELECT
            date,
            fees,
            txns,
            dau
        FROM {{ ref("fact_sonic_fundamental_metrics") }}
    )
    , supply_data as (
        select
            date,
            emissions_native,
            premine_unlocks_native,
            net_supply_change_native,
            circulating_supply_native
        from {{ ref("fact_sonic_supply_data") }}
    )
    , price_data as ({{ get_coingecko_metrics("sonic-3") }})
select
    fundamentals.date
    , fundamentals.fees
    , fundamentals.txns
    , fundamentals.dau
    , sonic_dex_volumes.dex_volumes
    , sonic_dex_volumes.adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    -- Chain Usage Metrics
    , fundamentals.dau AS chain_dau
    , fundamentals.txns AS chain_txns
    , sonic_dex_volumes.dex_volumes AS chain_spot_volume
    -- Cashflow metrics
    , fundamentals.fees AS ecosystem_revenue
    -- Supply Metrics
    , supply_data.emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native
    -- Token Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
    , price_data.token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamentals
left join sonic_dex_volumes on fundamentals.date = sonic_dex_volumes.date
left join price_data on fundamentals.date = price_data.date
left join supply_data on fundamentals.date = supply_data.date
where true
{{ ez_metrics_incremental('fundamentals.date', backfill_date) }}
and fundamentals.date < to_date(sysdate())
