{{
    config(
        materialized="table",
        snowflake_warehouse="ALGORAND",
        database="algorand",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data AS (SELECT * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date FROM {{ source('PROD_LANDING', 'ez_algorand_metrics') }}),
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
FROM fundamental_data
LEFT JOIN price USING (date)
