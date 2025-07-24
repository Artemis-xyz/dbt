{{
    config(
        materialized='incremental',
        snowflake_warehouse='SABER',
        database='SABER',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with saber_tvl as (
    {{ get_defillama_protocol_tvl('saber') }}
),
saber_market_data as (
    {{ get_coingecko_metrics('saber') }}
)

select
    saber_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , saber_tvl.tvl

    -- Market Metrics
    , smd.price
    , smd.market_cap
    , smd.fdmc
    , smd.token_turnover_circulating
    , smd.token_turnover_fdv
    , smd.token_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on  
from saber_tvl
left join saber_market_data smd using (date)
{{ ez_metrics_incremental('saber_tvl.date', backfill_date) }}
and saber_tvl.date < to_date(sysdate())