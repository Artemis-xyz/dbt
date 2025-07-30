{{
    config(
        materialized="incremental",
        snowflake_warehouse="BOB",
        database="bob",
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

WITH 
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2024-04-11'
        AND date < to_date(sysdate())
    )
    , fundamental_data as (
        SELECT
            date 
            , txns
            , daa
            , fees_native
            , fees
            , cost
            , cost_native
            , revenue
            , revenue_native
        FROM {{ ref("fact_bob_fundamental_metrics") }}
    )
    , market_data as ({{ get_coingecko_metrics('bob-build-on-bitcoin') }})
select
    date_spine.date
    , 'bob' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Metrics
    , fundamental_data.dau as chain_dau
    , fundamental_data.daa as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns

    -- Cash Flow Metrics
    , fundamental_data.fees_native
    , fundamental_data.fees
    , fundamental_data.cost as l1_fee_allocation

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
{{ ez_metrics_incremental("date", backfill_date) }}
AND date < to_date(sysdate())
