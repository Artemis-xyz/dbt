{{
    config(
        materialized='incremental',
        snowflake_warehouse='LIQUIDSWAP',
        database='LIQUIDSWAP',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with liquidswap_tvl as (
    {{ get_defillama_protocol_tvl('liquidswap') }}
)
, market_metrics as (
    {{ get_coingecko_metrics('pontem-liquidswap') }}
)

select
    liquidswap_tvl.date
    , 'liquidswap' as artemis_id

    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , liquidswap_tvl.tvl as tvl
    , dex_volumes as spot_volume
    
    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from liquidswap_tvl
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('liquidswap_tvl.date', backfill_date) }}
and liquidswap_tvl.date < to_date(sysdate())