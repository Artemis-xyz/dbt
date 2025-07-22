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
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

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
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from cellana_tvl
left join cellana_market_data cmd using (date)
{{ ez_metrics_incremental('cellana_tvl.date', backfill_date) }}
    and cellana_tvl.date < to_date(sysdate())