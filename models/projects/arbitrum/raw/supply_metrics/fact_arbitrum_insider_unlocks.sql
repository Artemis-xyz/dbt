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
    
    -- Team/Foundation/DAO unlocks (gradual vesting over 4 years)
    -- Total allocation: 42.78% = 4.278 billion tokens
    SELECT 
        DATEADD(month, seq4(), '2023-03-23'::DATE) AS date,
        'Team/Foundation/DAO Unlock' as event_type,
        (4.278 * 1e9) / 48 AS amount, -- 48 months = 4 years
        'https://docs.arbitrum.foundation/token-distribution' as source
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 48))
    
    UNION ALL
    
    -- Investor unlocks (17.53% = 1.753 billion tokens)
    -- 1-year cliff then 3-year vesting
    SELECT 
        '2024-03-23'::DATE AS date,
        'Investor Unlock' as event_type,
        (1.753 * 1e9) * 0.25 AS amount, -- 25% after 1 year cliff
        'https://docs.arbitrum.foundation/token-distribution' as source
    
    UNION ALL
    
    SELECT 
        DATEADD(month, seq4(), '2024-03-23'::DATE) AS date,
        'Investor Unlock' as event_type,
        (1.753 * 1e9) * 0.75 / 36 AS amount, -- Remaining 75% over 36 months
        'https://docs.arbitrum.foundation/token-distribution' as source
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 36))
)