{{
    config(
        materialized='incremental',
        snowflake_warehouse='VELODROME',
        database='VELODROME',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var('backfill_columns', []),
        merge_exclude_columns=['created_on'] | reject('in', var('backfill_columns', [])) | list,
        full_refresh=false,
        tags=['ez_metrics']
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with velodrome_tvl as (
    {{ get_defillama_protocol_tvl('velodrome') }}
)
, velodrome_market_data as (
    {{ get_coingecko_metrics('velodrome-finance') }}
)

select
    velodrome_tvl.date,
    'Defillama' as source,

    -- Standardized Metrics
    velodrome_tvl.tvl,
    velodrome_market_data.price,
    velodrome_market_data.market_cap,
    velodrome_market_data.fdmc,
    velodrome_market_data.token_turnover_circulating,
    velodrome_market_data.token_turnover_fdv,
    velodrome_market_data.token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from velodrome_tvl
left join velodrome_market_data using (date) 
{{ ez_metrics_incremental('velodrome_tvl.date', backfill_date) }}
and velodrome_tvl.date < to_date(sysdate())