{{
    config(
        materialized="incremental",
        snowflake_warehouse="APEX",
        database="apex",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_apex_trading_volume") }}
        {{ ez_metrics_incremental("date", backfill_date) }}
        group by date
    )
    , price as ({{ get_coingecko_metrics("apex-token-2") }})
select
    date
    , 'apex' as app
    , 'DeFi' as category
    , trading_volume
    -- standardize metrics
    , trading_volume as perp_volume
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from trading_volume_data
left join price using(date)
where date < to_date(sysdate())
