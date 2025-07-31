{{
    config(
        materialized='incremental',
        snowflake_warehouse='DEEPBOOK',
        database='DEEPBOOK',
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

with deepbook_tvl as (
    {{ get_defillama_protocol_tvl('deepbook') }}
)
, market_metrics as (
    {{ get_coingecko_metrics('deep') }}
)

select
    deepbook_tvl.date
    , 'deepbook' as artemis_id
    , 'Defillama' as source

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , deepbook_tvl.tvl

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from deepbook_tvl
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('deepbook_tvl.date', backfill_date) }}
and deepbook_tvl.date < to_date(sysdate())