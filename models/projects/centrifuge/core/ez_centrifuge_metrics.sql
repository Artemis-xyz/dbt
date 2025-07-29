{{
    config(
        materialized="incremental",
        snowflake_warehouse="CENTRIFUGE",
        database="centrifuge",
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
    dim_date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2022-03-12'
          AND date < to_date(sysdate())
    )
    , fundamental_data AS (
        SELECT
            date
            , txns
            , daa
            , fees_native
            , fees_usd
        FROM {{ ref("fact_centrifuge_fundamental_metrics") }}
    )
    , market_data AS (
        {{ get_coingecko_metrics("centrifuge") }}
    )
SELECT
    date
    , 'centrifuge' AS artemis_id 

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume 

    -- Usage Data
    , daa AS chain_dau
    , daa AS dau
    , txns AS chain_txns
    , txns AS txns

    -- Fee Data
    , fees_native
    , fees_usd
    , fees_usd AS l1_fee_allocation

    -- Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM fundamental_data
LEFT JOIN market_data USING(date)
WHERE true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
AND fundamental_data.date < to_date(sysdate())
