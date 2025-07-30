{{
    config(
        materialized="incremental",
        snowflake_warehouse="CCTP",
        database="cctp",
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
        WHERE date >= '2023-04-18'
        AND date < to_date(sysdate())
    )
    , bridge_volume AS (
        SELECT date
            , bridge_volume
        FROM {{ ref("fact_cctp_bridge_volume") }}
        WHERE chain IS NULL
    )
    , bridge_dau AS (
        SELECT date
            , bridge_dau
        FROM {{ ref("fact_cctp_bridge_dau") }}
    )

SELECT
    bridge_volume.date AS date
    , 'cctp' AS artemis_id

    -- Standardized Metrics

    -- Usage Data
    , bridge_dau.bridge_dau
    , bridge_dau.bridge_dau AS dau
    , bridge_volume.bridge_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN bridge_volume USING (date)
LEFT JOIN bridge_dau USING (date)
WHERE true
{{ ez_metrics_incremental("bridge_volume.date", backfill_date) }}
AND bridge_volume.date < to_date(sysdate())
