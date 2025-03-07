{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_dao_emissions",
    )
}}

SELECT
    block_timestamp::date as date,
    'DAO Emissions' as event_type,
    sum(amount_precise) as amount,
    'https://arbiscan.io/address/0xf3fc178157fb3c87548baa86f9d24ba38e649b58' as source
FROM arbitrum_flipside.core.ez_token_transfers
WHERE from_address = lower('0xF3FC178157fb3c87548bAA86F9d24BA38E649B58')
AND contract_address = lower('0x912CE59144191C1204E64559FE8253a0e49E6548')
GROUP BY 1