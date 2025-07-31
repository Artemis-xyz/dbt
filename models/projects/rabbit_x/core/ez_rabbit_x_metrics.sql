{{
    config(
        materialized="incremental",
        snowflake_warehouse="RABBIT_X",
        database="rabbit_x",
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

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_rabbitx_trading_volume") }}
        group by date
    )
    , unique_traders_data as (
        select date, sum(unique_traders) as unique_traders
        from {{ ref("fact_rabbitx_unique_traders") }}
        group by date
    )
    , market_metrics as ({{ get_coingecko_metrics("rabbitx") }})
select
    date
    , 'rabbit_x' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , unique_traders_data.unique_traders as perp_dau
    , unique_traders_data.unique_traders as dau
    , trading_volume_data.trading_volume as perp_volume

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from trading_volume_data
left join unique_traders_data using(date)
left join market_metrics using(date)
where true
{{ ez_metrics_incremental('trading_volume_data.date', backfill_date) }}
and trading_volume_data.date < to_date(sysdate())
