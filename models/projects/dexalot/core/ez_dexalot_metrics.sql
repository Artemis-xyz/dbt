{{
    config(
        materialized="incremental",
        snowflake_warehouse="DEXALOT",
        database="DEXALOT",
        schema="core",
        alias="ez_metrics",
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

with
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date >= '2025-05-27' and date < to_date(sysdate())
    )
    , fundamental_data as (
        select
            date, chain, dau, txns, fees_native
        from {{ ref("fact_dexalot_fundamental_metrics") }}
    )
    , market_data as ({{ get_coingecko_metrics("dexalot") }})
select
    f.date
    , 'dexalot' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , fundamental_data.dau AS chain_dau
    , fundamental_data.dau
    , fundamental_data.txns AS chain_txns
    , fundamental_data.txns

    -- Fee Data
    , fundamental_data.fees_native
    , fundamental_data.fees_native * market_data.price AS fees


    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine 
LEFT JOIN fundamental_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true 
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
AND f.date < to_date(sysdate())
