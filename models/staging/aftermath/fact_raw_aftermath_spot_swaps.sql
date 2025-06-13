{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

SELECT
    parquet_raw:date::date AS date
    , parquet_raw:transaction_digest::string AS transaction_digest
    , parquet_raw:pool_address::string AS pool_address
    , parquet_raw:sender::string AS sender

    , coingecko_prices_fee.symbol AS fee_symbol
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS fee_amount_native
    , CASE
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_b::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_a.price
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_a::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_a::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_a.price > 1.2 THEN parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_b::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_b.price > 1.2 THEN parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_a.price
        ELSE parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price 
    END AS fee_amount_usd
    , parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS protocol_fee_share_amount_native
    , CASE  
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_b::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_a.price
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_a::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_a::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_a.price > 1.2 THEN parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_b::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_b.price > 1.2 THEN parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_a.price
        ELSE parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price 
    END AS protocol_fee_share_amount_usd
    , coingecko_prices_a.symbol AS symbol_a
    , coingecko_prices_a.price AS price_a
    , coingecko_prices_a.contract_address AS coingecko_token_address_a
    , parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) AS amount_a_swapped_native
    , CASE
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_a::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_a::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_a.price > 1.2 THEN parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_b.price
        ELSE parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price 
    END AS amount_a_swapped_usd
    , (parquet_raw:vault_a_amount_raw::float / 1e18) / POW(10, coingecko_prices_a.decimals) AS vault_a_amount_native
    , CASE
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_a::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN (parquet_raw:vault_a_amount_raw::float / 1e18) / POW(10, coingecko_prices_a.decimals) * coingecko_prices_b.price
        WHEN LOWER(parquet_raw:token_address_a::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_a.price > 1.2 THEN (parquet_raw:vault_a_amount_raw::float / 1e18) / POW(10, coingecko_prices_a.decimals) * coingecko_prices_b.price
        ELSE (parquet_raw:vault_a_amount_raw::float / 1e18) / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price 
    END AS vault_a_amount_usd
    , coingecko_prices_b.symbol AS symbol_b
    , coingecko_prices_b.price AS price_b
    , coingecko_prices_b.contract_address AS coingecko_token_address_b
    , parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) AS amount_b_swapped_native
    , CASE
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_b::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_a.price
        WHEN LOWER(parquet_raw:token_address_b::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_b.price > 1.2 THEN parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_a.price
        ELSE parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price 
    END AS amount_b_swapped_usd
    , (parquet_raw:vault_b_amount_raw::float / 1e18) / POW(10, coingecko_prices_b.decimals) AS vault_b_amount_native
    , CASE
        WHEN parquet_raw:date::date = '2024-11-12' AND LOWER(parquet_raw:token_address_b::string) = LOWER('0xf325ce1300e8dac124071d3152c5c5ee6174914f8bc2161e88329cf579246efc::afsui::AFSUI') THEN (parquet_raw:vault_b_amount_raw::float / 1e18) / POW(10, coingecko_prices_b.decimals) * coingecko_prices_a.price
        WHEN LOWER(parquet_raw:token_address_b::string) = LOWER('0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN') AND coingecko_prices_b.price > 1.2 THEN (parquet_raw:vault_b_amount_raw::float / 1e18) / POW(10, coingecko_prices_b.decimals) * coingecko_prices_a.price
        ELSE (parquet_raw:vault_b_amount_raw::float / 1e18) / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price 
    END AS vault_b_amount_usd, 
FROM {{ source('PROD_LANDING', 'raw_sui_fact_aftermath_dex_swaps_gold_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower(parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_fee
    ON coingecko_prices_fee.date = parquet_raw:date::date
    AND lower(coingecko_prices_fee.contract_address) = lower(parquet_raw:fee_token_address::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower(parquet_raw:token_address_b::string)
QUALIFY ROW_NUMBER() OVER (PARTITION BY parquet_raw:transaction_digest::string ORDER BY coingecko_prices_a.price DESC, coingecko_prices_b.price DESC) = 1