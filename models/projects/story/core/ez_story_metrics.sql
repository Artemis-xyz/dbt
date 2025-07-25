{{
    config(
        materialized="incremental",
        snowflake_warehouse="STORY",
        database="story",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH issued_supply_metrics AS (
    SELECT 
        date,
        total_supply + cumulative_ip_burned AS max_supply_native,
        total_supply AS total_supply_native,
        total_ip_burned AS native_burns,
        revenue,
        issued_supply AS issued_supply_native,
        circulating_supply AS circulating_supply_native
    FROM {{ ref('fact_story_issued_supply_and_float') }}
)

SELECT
    f.date,
    f.txns,
    f.daa AS dau,
    f.fees_native,
    f.fees,
    i.revenue,
    i.max_supply_native,
    i.total_supply_native,
    i.native_burns,
    i.issued_supply_native,
    i.circulating_supply_native,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM {{ ref("fact_story_fundamental_metrics") }} f
LEFT JOIN issued_supply_metrics i
    ON DATE(f.date) = i.date
WHERE TRUE
{% if is_incremental() and backfill_date is not none %}
  AND f.date >= {{ backfill_date }}
{% endif %}
AND f.date < TO_DATE(SYSDATE())
