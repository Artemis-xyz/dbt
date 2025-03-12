{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_foundation_unlocks",
    )
}}

 SELECT
    block_timestamp::date as date,
    'Arbitrum Foundation Unlocks' as event_type,
    sum(amount_precise) as amount,
    'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#vesting-and-lockup-details' as source
FROM
arbitrum_flipside.core.ez_token_transfers
WHERE from_address = lower('0x15533b77981cDa0F85c4F9a485237DF4285D6844')
and contract_address = lower('0x912CE59144191C1204E64559FE8253a0e49E6548')
GROUP BY 1