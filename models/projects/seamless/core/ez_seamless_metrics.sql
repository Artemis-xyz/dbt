{{
    config(
        materialized="incremental",
        snowflake_warehouse="SEAMLESSPROTOCOL",
        database="seamlessprotocol",
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
        WHERE date >= '2023-08-31'
        AND date < to_date(sysdate())
    )
    , seamless_by_chain AS (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_seamless_protocol_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
    , seamless_metrics AS (
        SELECT
            date
            , SUM(daily_borrows_usd) AS daily_borrows_usd
            , SUM(daily_supply_usd) AS daily_supply_usd
        FROM seamless_by_chain
        GROUP BY 1
    )

    , market_data AS ({{ get_coingecko_metrics("seamless-protocol") }})

SELECT
    seamless_metrics.date
    , 'seamlessprotocol' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , seamless_metrics.daily_borrows_usd AS lending_loans
    , seamless_metrics.daily_supply_usd AS lending_deposits

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN seamless_metrics USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental("seamless_metrics.date", backfill_date) }}
AND seamless_metrics.date < to_date(sysdate())