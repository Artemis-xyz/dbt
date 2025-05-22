{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

/*
WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

SELECT
    parquet_raw:date::date AS date,  
    parquet_raw:transaction_digest::string AS transaction_digest,     
    parquet_raw:a_to_b::boolean AS a_to_b,
    parquet_raw:pool::string AS pool_address,     
    parquet_raw:fee_amount::float AS fee_amount,     
    parquet_raw:fee_symbol::string AS fee_symbol,    
    coingecko_prices_fee.price AS price_fee,
    parquet_raw:amount_a_swapped::float AS amount_a_swapped,     
    parquet_raw:vault_a_amount::float AS vault_a_amount,     
    parquet_raw:symbol_a::string AS symbol_a,   
    coingecko_prices_a.price AS price_a,
    parquet_raw:amount_b_swapped::float AS amount_b_swapped,     
    parquet_raw:vault_b_amount::float AS vault_b_amount,     
    parquet_raw:symbol_b::string AS symbol_b,    
    coingecko_prices_b.price AS price_b,
FROM {{ source('PROD_LANDING', 'raw_sui_fact_bluefin_dex_swaps_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.symbol) = lower(parquet_raw:symbol_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.symbol) = lower(parquet_raw:symbol_b::string)
LEFT JOIN coingecko_prices AS coingecko_prices_fee
    ON coingecko_prices_fee.date = parquet_raw:date::date
    AND lower(coingecko_prices_fee.symbol) = lower(parquet_raw:fee_symbol::string)
*/ 

-- Run this check to see if the coingecko prices are correct

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

SELECT DISTINCT
    parquet_raw:symbol_a::string AS parquet_symbol_a,   
    parquet_raw:symbol_b::string AS parquet_symbol_b, 
    parquet_raw:token_address_a::string AS parquet_token_address_a,
    parquet_raw:token_address_b::string AS parquet_token_address_b, 
    coingecko_prices_a.price AS coingecko_price_a,
    coingecko_prices_b.price AS coingecko_price_b, 
    coingecko_prices_a.contract_address AS coingecko_contract_address_a,
    coingecko_prices_b.contract_address AS coingecko_contract_address_b, 
    coingecko_prices_a.symbol AS coingecko_symbol_a,
    coingecko_prices_b.symbol AS coingecko_symbol_b
FROM {{ source('PROD_LANDING', 'raw_sui_fact_bluefin_dex_swaps_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower(parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower(parquet_raw:token_address_b::string)