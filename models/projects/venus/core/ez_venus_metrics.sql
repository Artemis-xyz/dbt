{{
    config(
        materialized="incremental",
        snowflake_warehouse="venus",
        database="venus",
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
        WHERE date >= '2020-11-24'
        AND date < to_date(sysdate())
    )
    , venus_by_chain AS (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_venus_v4_lending_bsc_gold"),
                ],
            )
        }}
    )
    , venus_metrics AS (
        SELECT
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        FROM venus_by_chain
        GROUP BY 1
    )

    , token_incentives AS (
        SELECT
            date
            , token_incentives
        FROM {{ ref("fact_venus_token_incentives") }}
    )
    , market_data AS ({{ get_coingecko_metrics("venus") }})

SELECT
    venus_metrics.date
    , 'venus' as artemis_id

    -- Standardized Metrics

    -- Market Data 
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , venus_metrics.daily_borrows_usd as lending_loans
    , venus_metrics.daily_supply_usd as lending_deposits

    -- Financial Statements 
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN venus_metrics USING (date)
LEFT JOIN token_incentives USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental("venus_metrics.date", backfill_date) }}
AND venus_metrics.date < to_date(sysdate())