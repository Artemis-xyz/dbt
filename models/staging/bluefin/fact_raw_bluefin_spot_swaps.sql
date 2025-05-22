{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)


SELECT
    parquet_raw:date::date AS date
    , parquet_raw:timestamp_ms::timestamp AS timestamp
    , parquet_raw:transaction_digest::string AS transaction_digest
    , parquet_raw:pool::string AS pool_address
    , parquet_raw:sender::string AS sender

    , coingecko_prices_fee.symbol AS fee_symbol
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS fee_amount_native
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS fee_amount_usd
    , parquet_raw:protocol_fee_share_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS revenue_native
    , parquet_raw:protocol_fee_share_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS revenue_usd

    , coingecko_prices_a.symbol AS symbol_a
    , parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) AS amount_a_swapped_native
    , parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price AS amount_a_swapped_usd
    , parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) AS vault_a_amount_native
    , parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price AS vault_a_amount_usd

    , coingecko_prices_b.symbol AS symbol_b
    , parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) AS amount_b_swapped_native
    , parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price AS amount_b_swapped_usd
    , parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) AS vault_b_amount_native
    , parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price AS vault_b_amount_usd
FROM {{ source('PROD_LANDING', 'raw_sui_fact_bluefin_dex_swaps_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower(parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_fee
    ON coingecko_prices_fee.date = parquet_raw:date::date
    AND lower(coingecko_prices_fee.contract_address) = lower(parquet_raw:fee_token_address::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower(parquet_raw:token_address_b::string)

/*
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
*/