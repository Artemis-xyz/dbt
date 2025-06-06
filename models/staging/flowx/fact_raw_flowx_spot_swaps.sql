{{config(
    materialized = 'table',
    database = 'flowx'
)}}

/*
{
  "a_to_b": true,
  "amount_a_swapped_raw": 6.021619000000000e+06,
  "amount_b_swapped_raw": 1.765459140000000e+08,
  "epoch": 450,
  "sender": "0xe3c3335efe12dfcb750f9b94697ebffe1483aa5409ab37cd0e658ec4d711615a",
  "timestamp_ms": 1720214454867,
  "token_address_a": "0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
  "token_address_b": "d9f9b0b4f35276eecd1eea6985bfabe2a2bbd5575f9adb9162ccbdb4ddebde7f::smove::SMOVE",
  "transaction_digest": "7wnBhXYue2sw29bDZYCqvy4pAiMX5SZfKT8iQVoNJygp",
  "unique_id": "7wnBhXYue2sw29bDZYCqvy4pAiMX5SZfKT8iQVoNJygp-{\"amount_x_in\":\"6021619\",\"amount_x_out\":\"0\",\"amount_y_in\":\"0\",\"amount_y_out\":\"176545914\",\"coin_x\":\"0000000000000000000000000000000000000000000000000000000000000002::sui::SUI\",\"coin_y\":\"d9f9b0b4f35276eecd1eea6985bfabe2a2bbd5575f9adb9162ccbdb4ddebde7f::smove::SMOVE\",\"user\":\"0xe3c3335efe12dfcb750f9b94697ebffe1483aa5409ab37cd0e658ec4d711615a\"}"
}
*/

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)


SELECT
    dex_swaps.parquet_raw:date::date AS date
    , dex_swaps.parquet_raw:timestamp_ms::timestamp AS timestamp
    , dex_swaps.parquet_raw:transaction_digest::string AS transaction_digest
    , dex_swaps.parquet_raw:sender::string AS sender

    , coingecko_prices_a.symbol AS symbol_a
    , dex_swaps.parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) AS amount_a_swapped_native
    , dex_swaps.parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price AS amount_a_swapped_usd

    , coingecko_prices_b.symbol AS symbol_b
    , dex_swaps.parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) AS amount_b_swapped_native
    , dex_swaps.parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price AS amount_b_swapped_usd

FROM {{ source('PROD_LANDING', 'raw_sui_fact_flowx_dex_swaps_parquet') }} AS dex_swaps
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = dex_swaps.parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower('0x' || dex_swaps.parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = dex_swaps.parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower('0x' || dex_swaps.parquet_raw:token_address_b::string)