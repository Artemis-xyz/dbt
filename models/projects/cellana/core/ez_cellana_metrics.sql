{{
    config(
        materialized='incremental',
        snowflake_warehouse='CELLANA',
        database='CELLANA',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with cellana_tvl as (
    {{ get_defillama_protocol_tvl('cellana') }}
)
, market_metrics as (
    {{ get_coingecko_metrics('cellena-finance') }}
)

select
    cellana_tvl.date
    , 'cellana' as artemis_id
    , 'Defillama' as source

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , cellana_tvl.tvl as tvl

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from cellana_tvl
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('cellana_tvl.date', backfill_date) }}
and cellana_tvl.date < to_date(sysdate())