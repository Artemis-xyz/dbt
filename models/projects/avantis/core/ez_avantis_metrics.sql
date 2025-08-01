{{
    config(
        materialized="incremental",
        snowflake_warehouse="AVANTIS",
        database="avantis",
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
        from {{ ref("fact_avantis_trading_volume_silver") }}
        group by date
    )
    , unique_traders_data as (
        select date, sum(unique_traders) as unique_traders
        from {{ ref("fact_avantis_unique_traders_silver") }}
        group by date
    )
select
    date
    , 'avantis' as app
    , 'DeFi' as category
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from trading_volume_data
left join unique_traders_data using(date)
where true
{{ ez_metrics_incremental("date", backfill_date) }}
and date < to_date(sysdate())
