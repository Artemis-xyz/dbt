{{
    config(
        materialized="incremental",
        snowflake_warehouse="ALGORAND",
        database="algorand",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data AS (
        SELECT 
            * EXCLUDE date,
            TO_TIMESTAMP_NTZ(date) AS date
        FROM {{ source('PROD_LANDING', 'ez_algorand_metrics') }}
    ),

    price as ({{ get_coingecko_metrics("algorand") }})

SELECT
    DATE(DATE_TRUNC('DAY', fundamental_data.date)) AS date
    ,'algorand' as chain
    , dau
    , txns
    , fees_native
    , fees_native * price AS fees
    , CASE 
        WHEN date > '2024-12-31' THEN 0.5 * fees
        ELSE fees
    END AS revenue
    , rewards_algo
    , rewards_algo * price AS rewards_usd
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Usage Metrics
    , dau AS chain_dau
    , txns AS chain_txns
    -- Cashflow Metrics
    , fees_native * price AS chain_fees
    , fees_native * price AS ecosystem_revenue
    , fees_native AS ecosystem_revenue_native
    , rewards_algo * price AS validator_fee_allocation_usd
    , rewards_algo AS validator_fee_allocation_native
    -- Bespoke metrics
    , unique_eoas
    , unique_senders
    , unique_receivers
    , new_eoas
    , unique_pairs
    , unique_eoa_pairs
    , unique_tokens
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM fundamental_data
LEFT JOIN price USING (date)
WHERE true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())