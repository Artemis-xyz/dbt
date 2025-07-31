{{
    config(
        materialized="incremental",
        snowflake_warehouse="AEVO",
        database="aevo",
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
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_aevo_trading_volume") }}
        group by date
    )
    , price as ({{ get_coingecko_metrics("aevo-exchange") }})
select
    date
    , 'aevo' as artemis_id

    --Market Data
    , price_data.price
    , price_data.market_cap as mc
    , price_data.fdmc
    , price_data.token_volume

    --Usage Data
    , trading_volume_data.trading_volume as perp_volume

    --Token Turnover/Other Data
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from trading_volume_data
left join price using(date)
where true 
{{ ez_metrics_incremental("date", backfill_date) }}
and date < to_date(sysdate())
