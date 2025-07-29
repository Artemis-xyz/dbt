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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2024-12-01' AND date < to_date(sysdate())
    )
    , sonic_dex_volumes AS (
        SELECT date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        FROM {{ ref("fact_sonic_daily_dex_volumes") }}
    )
    , fundamentals AS (
        SELECT
            date,
            fees,
            txns,
            dau
        FROM {{ ref("fact_sonic_fundamental_metrics") }}
    )
    , supply_data AS (
        SELECT
            date,
            emissions_native,
            premine_unlocks_native,
            net_supply_change_native,
            circulating_supply_native
        FROM {{ ref("fact_sonic_supply_data") }}
    )
    , price_data AS ({{ get_coingecko_metrics("sonic-3") }})

SELECT
    fundamentals.date
    , 'sonic' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Usage Data
    , fundamentals.dau AS chain_dau
    , fundamentals.dau
    , fundamentals.txns AS chain_txns
    , fundamentals.txns
    , sonic_dex_volumes.dex_volumes AS chain_spot_volume
    , sonic_dex_volumes.adjusted_dex_volumes AS chain_spot_volume_adjusted

    -- Fee Data
    , fundamentals.fees

    -- Supply Data
    , supply_data.emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.circulating_supply_native

    -- Turnover Data
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN fundamentals USING (date)
LEFT JOIN sonic_dex_volumes USING (date)
LEFT JOIN price_data USING (date)
LEFT JOIN supply_data USING (date)
WHERE true
{{ ez_metrics_incremental('fundamentals.date', backfill_date) }}
AND fundamentals.date < to_date(sysdate())
