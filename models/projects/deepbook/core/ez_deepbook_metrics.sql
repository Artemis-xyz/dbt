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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with deepbook_tvl as (
    {{ get_defillama_protocol_tvl('deepbook') }}
)
, deepbook_market_data as (
    {{ get_coingecko_metrics('deep') }}
)

select
    deepbook_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , deepbook_tvl.tvl

    -- Market Metrics
    , dmd.price
    , dmd.market_cap
    , dmd.fdmc
    , dmd.token_turnover_circulating
    , dmd.token_turnover_fdv
    , dmd.token_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from deepbook_tvl
left join deepbook_market_data dmd using (date)
{{ ez_metrics_incremental('deepbook_tvl.date', backfill_date) }}
    and deepbook_tvl.date < to_date(sysdate())