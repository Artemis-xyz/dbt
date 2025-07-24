{{
    config(
        materialized='incremental',
        snowflake_warehouse='THORCHAIN',
        database='THORCHAIN',
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

with thorchain_tvl as (
    {{ get_defillama_protocol_tvl('thorchain') }}
)

, market_metrics as (
    {{get_coingecko_metrics('thorchain')}}
)

select
    tt.date
    , 'Defillama' as source

    -- Standardized Metrics
    , tt.tvl

    -- Market Metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
    , mm.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from thorchain_tvl tt
left join market_metrics mm using (date)
{{ ez_metrics_incremental('tt.date', backfill_date) }}
and tt.date < to_date(sysdate())
and tt.name = 'thorchain' -- macro above returns data for 'Thorchain Lending' too, so we filter by name