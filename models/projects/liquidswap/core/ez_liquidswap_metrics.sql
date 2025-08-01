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
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with liquidswap_tvl as (
    {{ get_defillama_protocol_tvl('liquidswap') }}
)
, liquidswap_market_data as (
    {{ get_coingecko_metrics('pontem-liquidswap') }}
)

select
    liquidswap_tvl.date
    , 'Defillama' as source
    -- Standardized Metrics
    , liquidswap_tvl.tvl
    -- Market Metrics
    , lmd.price
    , lmd.market_cap
    , lmd.fdmc
    , lmd.token_turnover_circulating
    , lmd.token_turnover_fdv
    , lmd.token_volume
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from liquidswap_tvl
left join liquidswap_market_data lmd using (date)
where true
{{ ez_metrics_incremental('liquidswap_tvl.date', backfill_date) }}
and liquidswap_tvl.date < to_date(sysdate())