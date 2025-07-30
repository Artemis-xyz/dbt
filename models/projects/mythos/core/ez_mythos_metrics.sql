{{
    config(
        materialized="incremental",
        snowflake_warehouse="MYTHOS",
        database="mythos",
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
        WHERE date >= '2024-03-20'
        AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT
            date, 
            txns,
            daa, 
            fees_native, 
            fees_usd AS fees 
        FROM {{ ref("fact_mythos_fundamental_metrics") }}
    )
    , market_data AS ({{ get_coingecko_metrics('mythos') }})

SELECT
    date_spine.date
    , 'mythos' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , daa AS chain_dau
    , daa AS dau
    , txns AS chain_txns
    , txns

    -- Fee Data
    , fundamental_data.fees_native 
    , fundamental_data.fees 

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
