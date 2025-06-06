{{config(
    materialized = 'table',
    database = 'flowx'
)}}

/*
{
  "date": "2024-01-24",
  "epoch": 287,
  "pool_address": "0x5b41997b06d7461e565b2675c0a968a497ff9e3de7f5b4a2cb82ec270449ff5f",
  "swap_fee_percent": 3.000000000000000e-03,
  "token_address_a": "47f389127ad7bfdd5b64dd532ba5e29495466c208b7ba2cc6a10a0a3a4610f3e::btcat::BTCAT",
  "token_address_b": "9a399e218c15b0d1e546ebe95521ee327a30ca9b129b378cfa422aefec53f758::brcsui::BRCSUI",
  "unique_id": "GSAauPWQr5ub9n15YyTXv8upoq1XzVxPQ2tc45yTSoP9-0x5b41997b06d7461e565b2675c0a968a497ff9e3de7f5b4a2cb82ec270449ff5f",
  "vault_a_amount_raw": 6.045875520660600e+13,
  "vault_b_amount_raw": 1.655187748143892e+15,
  "version": "v2"
}
*/

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

SELECT 
    parquet_raw:date::date AS date
    , parquet_raw:epoch::number AS epoch
    , parquet_raw:pool_address::string AS pool_address
    , COALESCE(parquet_raw:swap_fee_percent::float, 0) AS swap_fee_percent
    , parquet_raw:token_address_a::string AS token_address_a
    , parquet_raw:token_address_b::string AS token_address_b
    , parquet_raw:vault_a_amount_raw::float AS vault_a_amount_raw
    , parquet_raw:vault_b_amount_raw::float AS vault_b_amount_raw
    , parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) AS vault_a_amount_native
    , parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) AS vault_b_amount_native
    , parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price AS vault_a_amount_usd
    , parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price AS vault_b_amount_usd
FROM {{ source('PROD_LANDING', 'raw_sui_dim_flowx_pools_parquet') }}
LEFT JOIN coingecko_prices AS coingecko_prices_a
    ON coingecko_prices_a.date = parquet_raw:date::date
    AND lower(coingecko_prices_a.contract_address) = lower('0x' || parquet_raw:token_address_a::string)
LEFT JOIN coingecko_prices AS coingecko_prices_b
    ON coingecko_prices_b.date = parquet_raw:date::date
    AND lower(coingecko_prices_b.contract_address) = lower('0x' || parquet_raw:token_address_b::string)