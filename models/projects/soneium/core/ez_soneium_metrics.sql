{{
    config(
        materialized="incremental",
        snowflake_warehouse="SONEIUM",
        database="soneium",
        schema="core",
        alias="ez_metrics",
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

WITH 
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2024-12-02'
        AND date < to_date(sysdate())
    )
    , fundamental_metrics AS (SELECT * FROM {{ ref("fact_soneium_fundamental_metrics") }})

SELECT
    date_spine.date
    , 'soneium' AS artemis_id

    -- Standardized Metrics

    -- Usage Data
    , dau as chain_dau
    , daa as dau
    , txns as chain_txns
    , txns

    -- Fee Data
    , fees_native
    , fees

    -- timestamp columns    
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN fundamental_metrics USING (date)
WHERE true
{{ ez_metrics_incremental("date", backfill_date) }}
AND date < to_date(sysdate())
