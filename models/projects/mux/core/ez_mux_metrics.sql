{{
    config(
        materialized="incremental",
        snowflake_warehouse="MUX",
        database="mux",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    mux_data as (
        select 
            date
            , sum(trading_volume) as trading_volume
            , sum(unique_traders) as unique_traders
        from {{ ref("fact_mux_trading_volume_unique_traders") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        and chain is not null
        group by 1
    )
    , price as ({{ get_coingecko_metrics("mcdex") }})
    , token_incentives as (
        select
            date,
            sum(token_incentives) as token_incentives
        from {{ ref("fact_mux_token_incentives") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
    )
select
    date
    , 'mux' as app
    , 'DeFi' as category
    -- Usage Metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    -- Market Data
    , price.price
    , price.market_cap
    , price.fdmc
    , price.token_turnover_circulating
    , price.token_turnover_fdv
    , price.token_volume
    -- Cashflow Metrics
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from mux_data
left join price using(date)
left join token_incentives using(date)
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
