{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_foundation_unlocks",
    )
}}

SELECT * FROM (VALUES
    (
        '2023-03-21',
        'Arbitrum Foundation Initial Unlock',
        0.4 * 1e8, 
        'https://arbiscan.io/tokentxns?a=0xc24e24383120669512a336fb3b5b19afb4cc2a56'
    ),
    (
        '2023-03-24', 
        'Arbitrum Foundation Initial Unlock',
        0.1 * 1e8, 
        'https://arbiscan.io/tokentxns?a=0xc24e24383120669512a336fb3b5b19afb4cc2a56'
    ),
    (
        '2023-03-27', 
        'Arbitrum Foundation Initial Unlock',
        0.500009 * 1e6, 
        'https://arbiscan.io/tokentxns?a=0xc24e24383120669512a336fb3b5b19afb4cc2a56'
    )
) AS t (date, event_type, amount, source)

UNION ALL

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