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
FROM {{ source('PROD_LANDING', 'raw_sui_fact_cetus_dex_swaps_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower(parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower(parquet_raw:token_address_b::string)