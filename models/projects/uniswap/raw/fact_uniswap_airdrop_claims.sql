{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

SELECT
    block_timestamp::date as date,
    sum(amount_native) as airdrop_amount_claimed_native,
    sum(amount) as airdrop_amount_claimed
FROM
    {{ ref('fact_ethereum_token_transfers') }}
WHERE contract_address = lower('0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984')
AND from_address = lower('0x090D4613473dEE047c3f2706764f49E0821D256e')
GROUP BY 1