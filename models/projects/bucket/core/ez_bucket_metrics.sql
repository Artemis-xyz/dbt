{{
    config(
        materialized="incremental",
        snowflake_warehouse="BUCKET",
        database="bucket",
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

WITH 
    defillama_tvl AS (
        SELECT
            date, 
            SUM(tvl) AS tvl
        FROM {{ ref("fact_defillama_protocol_tvls") }}
        WHERE defillama_protocol_id = 3206 OR defillama_protocol_id = 5530
            -- This includes Bucket CDP and Bucket Farm
        {{ ez_metrics_incremental('date', backfill_date) }}
        GROUP BY 1
    )

    , date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= (SELECT MIN(date) FROM defillama_tvl)
          AND date < to_date(sysdate())
    )

    , defillama_tvl_forwardfill AS (
        SELECT
            d.date,
            LAST_VALUE(t.tvl IGNORE NULLS) OVER (
                ORDER BY d.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS tvl
        FROM date_spine d
        LEFT JOIN defillama_tvl t ON d.date = t.date
    )   

SELECT
    d.date, 
    'bucket' AS app,
    'DeFi' AS category,
    COALESCE(d.tvl, 0) AS tvl
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM defillama_tvl_forwardfill d
{{ ez_metrics_incremental('d.date', backfill_date) }}