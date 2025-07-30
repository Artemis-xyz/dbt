{{
    config(
        materialized="incremental",
        snowflake_warehouse="BRAINTRUST",
        database="braintrust",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
    )
}}

with revenue as (
    SELECT
        date,
        revenue
    FROM {{ ref("fact_braintrust_revenue") }}
)
, market_metrics as (
    {{ get_coingecko_metrics("braintrust") }}
)
, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between (SELECT min(date) from market_metrics) and to_date(sysdate())
)
SELECT
    date_spine.date,

    -- Standardized Metrics
    -- Market Data
    market_metrics.price,
    market_metrics.market_cap,
    market_metrics.fdmc,
    market_metrics.token_turnover_circulating,
    market_metrics.token_turnover_fdv,

    -- Financial Metrics
    coalesce(revenue.revenue, 0) as revenue

    -- Timetamp Columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
LEFT JOIN revenue USING(date)
LEFT JOIN market_metrics USING(date)
WHERE TRUE
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}