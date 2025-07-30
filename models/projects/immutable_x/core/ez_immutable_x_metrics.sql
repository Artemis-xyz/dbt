{{
    config(
        materialized="incremental",
        snowflake_warehouse="IMMUTABLE_X",
        database="immutable_x",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    nft_metrics as ({{ get_nft_metrics("immutable_x") }}),
    price_data as ({{ get_coingecko_metrics("immutable-x") }})
select
    date
    , 'immutable_x' as artemis_id

    -- Market Data Metrics
    , price
    , market_cap as mc
    , fdmc

    --Usage Data
    , nft_trading_volume AS chain_nft_trading_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from nft_metrics
left join price_data using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
