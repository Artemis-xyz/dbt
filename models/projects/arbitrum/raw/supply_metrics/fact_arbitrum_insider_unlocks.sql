{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="fact_arbitrum_insider_unlocks",
    )
}}

SELECT date, event_type, amount, source FROM (
    -- Initial 0% unlocked at TGE
    -- Investor unlocks (44.47% = 4.447 billion tokens)
    -- 1-year cliff then 3-year vesting
    SELECT 
        '2024-03-23'::DATE AS date,
        'Team/Investor Unlock' as event_type,
        (4.447 * 1e9) * 0.25 AS amount, -- 25% after 1 year cliff
        'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#vesting-and-lockup-details' as source
    
    UNION ALL
    
    SELECT 
        DATEADD(month, seq4(), '2024-03-23'::DATE) AS date,
        'Team/Investor Unlock' as event_type,
        (4.447 * 1e9) * 0.75 / 36 AS amount, -- Remaining 75% over 36 months
        'https://docs.arbitrum.foundation/airdrop-eligibility-distribution#vesting-and-lockup-details' as source
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 36))
)