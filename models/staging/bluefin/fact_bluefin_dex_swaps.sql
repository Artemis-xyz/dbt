{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH coingecko_prices AS (
    WITH raw_coingecko_prices AS (
        {{get_multiple_coingecko_price_with_latest('sui')}}
    )

    SELECT *
    FROM raw_coingecko_prices
    WHERE contract_address != '0x53b7015c996f22c026fa320cff2110002771e55dd36307221c2a0f473107869b::blue::BLUE'
)

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
