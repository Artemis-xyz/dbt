{{ config(
    materialized="table",
    snowflake_warehouse="ROCKETPOOL"
    )
}}

SELECT
    date(block_timestamp) as date,
    sum(amount) as token_incentives_native,
    sum(amount_usd) as token_incentives_usd
FROM
    ethereum_flipside.core.ez_token_transfers
WHERE
    contract_address = lower('0xd33526068d116ce69f19a9ee46f0bd304f21a51f')
    AND from_address in (
        -- lower('0x0000000000000000000000000000000000000000'),
        lower('0xd33526068d116ce69f19a9ee46f0bd304f21a51f')
    )
    AND origin_function_signature = '0x5d3e8ffa'
GROUP BY 1