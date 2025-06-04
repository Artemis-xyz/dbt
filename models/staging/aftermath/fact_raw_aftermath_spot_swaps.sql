{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

/*
{
  "amount_a_swapped_raw": "75630",
  "amount_b_swapped_raw": "9",
  "date": "2025-06-02",
  "fee_percentage": 1.000000000000000e-03,
  "pool_address": "0x30ddd58a67dd3a0bf2f948d60e67cdf9e93022cc26288e6d7a0b1830c46255c7",
  "protocol_fee_percentage": 5.000000000000000e-05,
  "sender": "0xf913a4a3ec2ee0aa6504f9926f14df971d112196ceb8ff55b4d0fdd2fdc24e47",
  "token_address_a": "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
  "token_address_b": "0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN",
  "transaction_digest": "H6YxFWic9koBpU84ct9JH9cKCKZkfhoEmg5eVpMCCXcT",
  "unique_id": "H6YxFWic9koBpU84ct9JH9cKCKZkfhoEmg5eVpMCCXcT-{\"amounts_in\":[\"9\"],\"amounts_out\":[\"75630\"],\"issuer\":\"0xf913a4a3ec2ee0aa6504f9926f14df971d112196ceb8ff55b4d0fdd2fdc24e47\",\"pool_id\":\"0x30ddd58a67dd3a0bf2f948d60e67cdf9e93022cc26288e6d7a0b1830c46255c7\",\"referrer\":null,\"reserves\":[\"17369104\",\"61\",\"477965263\",\"131693103\",\"318658\",\"32046319114\",\"58511\",\"437412\"],\"types_in\":[\"027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN\"],\"types_out\":[\"deeb7a4662eec9f2f3def03fb937a663dddaa2e215b8078a284d026b7946c270::deep::DEEP\"]}",
  "vault_a_amount_raw": 2.391024800000000e+25,
  "vault_b_amount_raw": 5.300000000000000e+19
}
*/

SELECT
    parquet_raw:date::date AS date
    , parquet_raw:transaction_digest::string AS transaction_digest
    , parquet_raw:pool_address::string AS pool_address
    , parquet_raw:sender::string AS sender

    , coingecko_prices_fee.symbol AS fee_symbol
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS fee_amount_native
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS fee_amount_usd
    , parquet_raw:protocol_fee_share_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS protocol_fee_share_amount_native
    , parquet_raw:protocol_fee_share_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS protocol_fee_share_amount_usd

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