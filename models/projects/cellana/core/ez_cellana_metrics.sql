{{
    config(
        materialized='incremental',
        snowflake_warehouse='CELLANA',
        database='CELLANA',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with cellana_tvl as (
    {{ get_defillama_protocol_tvl('cellana') }}
)
, cellana_market_data as (
    {{ get_coingecko_metrics('cellena-finance') }}
)

select
    cellana_tvl.date
    , 'Defillama' as source
    -- Standardized Metrics
    , cellana_tvl.tvl
    -- Market Metrics
    , cmd.price
    , cmd.market_cap
    , cmd.fdmc
    , cmd.token_turnover_circulating
    , cmd.token_turnover_fdv
    , cmd.token_volume
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from cellana_tvl
left join cellana_market_data cmd using (date)
where true
{{ ez_metrics_incremental('cellana_tvl.date', backfill_date) }}
and cellana_tvl.date < to_date(sysdate())