{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    parquet_raw:date::date AS date,  
    parquet_raw:transaction_digest::string AS transaction_digest,     
    parquet_raw:a_to_b::boolean AS a_to_b,
    parquet_raw:pool::string AS pool_address,     
    parquet_raw:fee_amount::float AS fee_amount,     
    parquet_raw:fee_symbol::string AS fee_symbol,     
    parquet_raw:amount_a_swapped::float AS amount_a_swapped,     
    parquet_raw:vault_a_amount::float AS vault_a_amount,     
    parquet_raw:symbol_a::string AS symbol_a,   
    parquet_raw:amount_b_swapped::float AS amount_b_swapped,     
    parquet_raw:vault_b_amount::float AS vault_b_amount,     
    parquet_raw:symbol_b::string AS symbol_b,    
FROM {{ source('PROD_LANDING', 'raw_sui_fact_bluefin_dex_swaps_parquet') }}