{{
    config(
        materialized="incremental",
        snowflake_warehouse="CORN",
        database="corn",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with corn_dex_volumes as (
    select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_corn_daily_dex_volumes") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, corn_market_data as (
    {{ get_coingecko_metrics('corn-3') }}
)

select
    date
    , dex_volumes
    , adjusted_dex_volumes
    -- Standardized Metrics
    , dex_volumes as chain_spot_volume

    -- Market Metrics
    , cmd.price
    , cmd.market_cap
    , cmd.fdmc
    , cmd.token_turnover_circulating
    , cmd.token_turnover_fdv
    , cmd.token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from corn_dex_volumes
left join corn_market_data cmd using (date)
{{ ez_metrics_incremental("date", backfill_date) }}