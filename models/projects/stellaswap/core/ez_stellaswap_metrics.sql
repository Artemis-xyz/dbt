{{
    config(
        materialized='incremental',
        snowflake_warehouse='STELLASWAP',
        database='STELLASWAP',
        schema='core',
        alias='ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with stellaswap_tvl as (
    {{ get_defillama_protocol_tvl('stellaswap') }}
)
, stellaswap_market_data as (
    {{ get_coingecko_metrics('stellaswap') }}
)

select
    stellaswap_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , stellaswap_market_data.price
    , stellaswap_market_data.token_volume
    , stellaswap_market_data.market_cap
    , stellaswap_market_data.fdmc

    , stellaswap_tvl.tvl

    , stellaswap_market_data.token_turnover_circulating
    , stellaswap_market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from stellaswap_tvl
left join stellaswap_market_data using (date)
where true
{{ ez_metrics_incremental('stellaswap_tvl.date', backfill_date) }}
and stellaswap_tvl.date < to_date(sysdate())