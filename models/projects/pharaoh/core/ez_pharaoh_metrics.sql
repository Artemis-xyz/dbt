{{
    config(
        materialized='incremental',
        snowflake_warehouse='PHARAOH',
        database='PHARAOH',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var('backfill_columns', []),
        merge_exclude_columns=['created_on'] if not var('backfill_columns', []) else none,
        full_refresh=false,
        tags=['ez_metrics']
    )
}}

{% set backfill_date = var('backfill_date', None) %}

with pharaoh_tvl as (
    {{ get_defillama_protocol_tvl('pharaoh') }}
)
, pharaoh_market_data as (
    {{ get_coingecko_metrics('pharaoh') }}
)

select
    pharaoh_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , pharaoh_tvl.tvl

    -- Market Metrics
    , pmd.price
    , pmd.market_cap
    , pmd.fdmc
    , pmd.token_turnover_circulating
    , pmd.token_turnover_fdv
    , pmd.token_volume

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from pharaoh_tvl
left join pharaoh_market_data pmd using (date)
where true
{{ ez_metrics_incremental('pharaoh_tvl.date', backfill_date) }}
and pharaoh_tvl.date < to_date(sysdate())