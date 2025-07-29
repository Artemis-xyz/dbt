{{
    config(
        materialized="incremental",
        snowflake_warehouse="GRASS",
        database="grass",
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
    data_collected as (
        select
            date,
            data_collected_tb
        from {{ ref("fact_grass_data_scraped") }}
    )
, market_data as (
    {{ get_coingecko_metrics('grass')}}
)

select
    market_data.date
    , data_collected.data_collected_tb
    -- Token Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume
    -- Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from market_data
left join data_collected on market_data.date = data_collected.date
where true
{{ ez_metrics_incremental('market_data.date', backfill_date) }}
and market_data.date < to_date(sysdate())