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
    prices AS ({{ get_coingecko_price_with_latest("algorand") }})
SELECT
    fundamental_data.date,
    'algorand' as chain,
    fundamental_data.txns,
    fundamental_data.fees_native,
    fundamental_data.fees_native * prices.price as fees,
    fundamental_data.rewards_algo,
    fundamental_data.rewards_algo * prices.price as rewards_usd,
    fundamental_data.dau,
    fundamental_data.unique_eoas,
    fundamental_data.unique_senders,
    fundamental_data.unique_receivers,
    fundamental_data.new_eoas,
    fundamental_data.unique_pairs,
    fundamental_data.unique_eoa_pairs,
    fundamental_data.unique_tokens
FROM fundamental_data
LEFT JOIN prices USING (date)
