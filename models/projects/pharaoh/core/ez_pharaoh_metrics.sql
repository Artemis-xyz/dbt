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
    , 'pharaoh' as artemis_id
    , 'Defillama' as source

    --Market Data
    , pmd.price
    , pmd.market_cap as mc
    , pmd.fdmc
    , pmd.token_volume

    --Usage Data
    , pharaoh_tvl.tvl

    --Token Turnover/Other Data
    , pmd.token_turnover_circulating
    , pmd.token_turnover_fdv

    -- timestamp columns
    , to_timestamp_ntz(current_timestamp()) as created_on
    , to_timestamp_ntz(current_timestamp()) as modified_on
from pharaoh_tvl
left join pharaoh_market_data pmd using (date)
where true
{{ ez_metrics_incremental('pharaoh_tvl.date', backfill_date) }}
and pharaoh_tvl.date < to_date(sysdate())