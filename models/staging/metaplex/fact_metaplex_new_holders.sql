{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

WITH metaplex_programs AS (
    SELECT 
        program_id::TEXT AS program_id, 
        program_name
    FROM (
        VALUES
            ('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY', 'Bubblegum'),
            ('CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d', 'Core'),
            ('CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR', 'Candy Machine v3'),
            ('cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ', 'Candy Machine v2'),
            ('cndyAnrLdqq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ', 'Candy Machine v1'),
            ('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s', 'Token Metadata')
    ) AS t(program_id, program_name)
),

metaplex_mints AS (
    SELECT DISTINCT 
        m.mint, 
        m.program_id
    FROM 
        {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_mints') }} AS m
    WHERE 
        m.program_id IN (SELECT program_id FROM metaplex_programs)
),

minter_activity AS (
    SELECT 
        DATE_TRUNC('DAY', m.block_timestamp) AS activity_date, 
        m.purchaser AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_mints') }} AS m
    WHERE 
        m.program_id IN (SELECT program_id FROM metaplex_programs)
),

decoded_instruction_activity AS (
    SELECT 
        DATE_TRUNC('DAY', di.block_timestamp) AS activity_date, 
        account.value:pubkey::TEXT AS wallet,
        ROW_NUMBER() OVER (
            PARTITION BY di.block_timestamp, account.value:pubkey::TEXT 
            ORDER BY 
                CASE WHEN account.value:name::TEXT = 'leafOwner' THEN 1 
                     WHEN account.value:name::TEXT = 'newLeafOwner' THEN 2 
                END
        ) AS rank
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }} AS di,
        LATERAL FLATTEN(input => di.decoded_instruction:accounts) AS account
    WHERE 
        di.program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'
        AND account.value:name::TEXT IN ('leafOwner', 'newLeafOwner')
        AND LOWER(event_type) IN ('mintv1', 'minttocollectionv1')
),

filtered_decoded_instruction_activity AS (
    SELECT 
        activity_date,
        wallet
    FROM 
        decoded_instruction_activity
    WHERE 
        rank = 1  -- leafOwner if present, newLeafOwner otherwise
),

unique_daily_holders AS (
    SELECT wallet, MIN(activity_date) AS date
    FROM (
        SELECT activity_date, wallet FROM minter_activity
        UNION ALL
        SELECT activity_date, wallet FROM filtered_decoded_instruction_activity
    ) AS combined_activities
    GROUP BY wallet
)


SELECT 
    date,
    COUNT(wallet) AS daily_new_holders  
FROM 
    unique_daily_holders
GROUP BY 
    date
ORDER BY 
    date DESC;
