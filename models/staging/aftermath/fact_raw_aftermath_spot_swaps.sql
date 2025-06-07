{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

WITH coingecko_prices AS (
    {{get_multiple_coingecko_price_with_latest('sui')}}
)

/*
  "amount_a_swapped_raw": 2.633519534700000e+10,
  "amount_b_swapped_raw": 1.586478172700000e+10,
  "date": "2023-08-04",
  "epoch": 113,
  "fee_amount_raw": 7.900558604100001e+07,
  "fee_token_address": "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
  "pool_address": "0xdeacf7ab460385d4bcb567f183f916367f7d43666a2c72323013822eb3c57026",
  "protocol_fee_amount_raw": 1.316759767350000e+06,
  "sender": "0x48b6cc0c7afa46f107b45ae2e5774e900df4ceb890bf278b26b91cbef0191fee",
  "token_address_a": "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
  "token_address_b": "0xce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::buck::BUCK",
  "transaction_digest": "Dzu14WCPfXU1RmqmYbUJv783vAyuma1vS81XAVVev5gT",
  "unique_id": "Dzu14WCPfXU1RmqmYbUJv783vAyuma1vS81XAVVev5gT-{\"amounts_in\":[\"26335195347\"],\"amounts_out\":[\"15864781727\"],\"issuer\":\"0x48b6cc0c7afa46f107b45ae2e5774e900df4ceb890bf278b26b91cbef0191fee\",\"pool_id\":\"0xdeacf7ab460385d4bcb567f183f916367f7d43666a2c72323013822eb3c57026\",\"referrer\":null,\"types_in\":[\"0000000000000000000000000000000000000000000000000000000000000002::sui::SUI\"],\"types_out\":[\"ce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::buck::BUCK\"]}",
  "vault_a_amount_raw": 1.846813493393920e+32,
  "vault_b_amount_raw": 1.102952356217940e+32
*/

SELECT
    parquet_raw:date::date AS date
    , parquet_raw:transaction_digest::string AS transaction_digest
    , parquet_raw:pool_address::string AS pool_address
    , parquet_raw:sender::string AS sender

    , coingecko_prices_fee.symbol AS fee_symbol
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS fee_amount_native
    , parquet_raw:fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS fee_amount_usd
    , parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) AS protocol_fee_share_amount_native
    , parquet_raw:protocol_fee_amount_raw::float / POW(10, coingecko_prices_fee.decimals) * coingecko_prices_fee.price AS protocol_fee_share_amount_usd

    , coingecko_prices_a.symbol AS symbol_a
    , coingecko_prices_a.contract_address AS coingecko_token_address_a
    , CASE 
        WHEN parquet_raw:token_address_a::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) 
    END AS amount_a_swapped_native
    , CASE 
        WHEN parquet_raw:token_address_a::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:amount_a_swapped_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price 
    END AS amount_a_swapped_usd
    , CASE 
        WHEN parquet_raw:token_address_a::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) 
    END AS vault_a_amount_native
    , CASE 
        WHEN parquet_raw:token_address_a::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:vault_a_amount_raw::float / POW(10, coingecko_prices_a.decimals) * coingecko_prices_a.price 
    END AS vault_a_amount_usd

    , coingecko_prices_b.symbol AS symbol_b
    , coingecko_prices_b.contract_address AS coingecko_token_address_b
    , CASE 
        WHEN parquet_raw:token_address_b::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) 
    END AS amount_b_swapped_native
    , CASE 
        WHEN parquet_raw:token_address_b::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:amount_b_swapped_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price 
    END AS amount_b_swapped_usd
    , CASE 
        WHEN parquet_raw:token_address_b::string = '0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN' THEN NULL
        ELSE parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) 
    END AS vault_b_amount_native
    , parquet_raw:vault_b_amount_raw::float / POW(10, coingecko_prices_b.decimals) * coingecko_prices_b.price AS vault_b_amount_usd
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