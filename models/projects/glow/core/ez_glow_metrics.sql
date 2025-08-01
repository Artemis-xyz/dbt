{{
    config(
        materialized="incremental",
        snowflake_warehouse="GLOW",
        database="glow",
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

with revenue as (
    SELECT
        date,
        fees
    FROM {{ ref("fact_glow_compute_revenue") }}
)
,  date_spine as (
    SELECT date
    FROM {{ ref("dim_date_spine") }}
    WHERE date between (SELECT MIN(date) from revenue) and to_date(sysdate())
)

SELECT
    date_spine.date,
    coalesce(revenue.fees, 0) as fees,
    -- timestamp columns
    sysdate() as created_on,
    sysdate() as modified_on
FROM date_spine
LEFT JOIN revenue USING(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())