{{
    config(
        materialized='incremental',
        snowflake_warehouse='ALEX',
        database='ALEX',
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

with alex_tvl as (
    {{ get_defillama_protocol_tvl('alex') }}
)
, alex_market_data as (
    {{ get_coingecko_metrics('alexgo') }}
)

select
    alex_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , alex_tvl.tvl

    -- Market Metrics
    , alex_market_data.price
    , alex_market_data.market_cap
    , alex_market_data.fdmc
    , alex_market_data.token_turnover_circulating
    , alex_market_data.token_turnover_fdv
    , alex_market_data.token_volume

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from alex_tvl
left join alex_market_data using (date)
where true
{{ ez_metrics_incremental("alex_tvl.date", backfill_date) }}
and alex_tvl.date < to_date(sysdate())