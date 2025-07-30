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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
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
    thorchain_tvl.date
    , 'thorchain' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    -- Usage Data
    , thorchain_tvl.tvl as tvl

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from thorchain_tvl
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('tt.date', backfill_date) }}
and thorchain_tvl.date < to_date(sysdate())
and thorchain_tvl.name = 'thorchain' -- macro above returns data for 'Thorchain Lending' too, so we filter by name