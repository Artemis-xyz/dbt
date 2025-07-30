{{config(
    materialized = 'table',
    snowflake_warehouse = 'ALGORAND'
)}}

SELECT
    parquet_raw:date::date AS date
    , parquet_raw:dau::int AS dau
    , parquet_raw:fees_native::float AS fees_native
    , parquet_raw:new_eoas::int AS new_eoas
    , parquet_raw:rewards_algo::float AS rewards_algo
    , parquet_raw:txns::int AS txns
    , parquet_raw:unique_eoa_pairs::int AS unique_eoa_pairs
    , parquet_raw:unique_eoas::int AS unique_eoas
    , parquet_raw:unique_pairs::int AS unique_pairs
    , parquet_raw:unique_receivers::int AS unique_receivers
    , parquet_raw:unique_senders::int AS unique_senders
    , parquet_raw:unique_tokens::int AS unique_tokens
FROM {{ source('PROD_LANDING', 'raw_algorand_ez_algorand_metrics_parquet') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY parquet_raw:date::date DESC) = 1