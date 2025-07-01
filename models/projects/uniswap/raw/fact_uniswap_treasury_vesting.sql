{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
    )
}}


SELECT
    block_timestamp::date as date,
    sum(amount_native) as treasury_vested_native,
    sum(amount) as treasury_vested
FROM
    {{ ref('fact_ethereum_token_transfers') }}
WHERE contract_address = lower('0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984')
AND from_address in (
    lower('0x4750c43867ef5f89869132eccf19b9b6c4286e1a')
    , lower('0xe3953d9d317b834592ab58ab2c7a6ad22b54075d')
    , lower('0x4b4e140d1f131fdad6fb59c13af796fd194e4135')
    , lower('0x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
    )
GROUP BY 1  