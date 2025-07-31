{{
    config(
        materialized="incremental",
        snowflake_warehouse="HYDRATION",
        database="hydration",
        schema="core",
        alias="ez_metrics",
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

with
    fundamental_data as (
        select
            date
            , coalesce(txns, 0) as txns
            , coalesce(daa, 0) as dau
            , coalesce(fees_native, 0) as fees_native
            , coalesce(fees_usd, 0) as fees
        from {{ ref("fact_hydration_fundamental_metrics") }}
    ),
    market_metrics as ({{ get_coingecko_metrics('hydradx') }})
select
    fundamental_data.date
    , 'hydration' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , fundamental_data.daa as chain_dau
    , fundamental_data.daa as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns

    -- Fee Data
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as fees
    
    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join market_metrics using(date)
where true 
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
