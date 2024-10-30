{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

WITH minter_activity AS (
    SELECT 
        DATE_TRUNC('DAY', m.block_timestamp) AS activity_date, 
        m.purchaser AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_mints') }} AS m
    WHERE 
        m.program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
),

seller_activity AS (
    SELECT 
        DATE_TRUNC('DAY', s.block_timestamp) AS activity_date, 
        s.seller AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_sales') }} AS s
    JOIN 
        {{ ref('fact_metaplex_mints') }} AS m ON s.mint = m.mint
),

buyer_activity AS (
    SELECT 
        DATE_TRUNC('DAY', s.block_timestamp) AS activity_date, 
        s.purchaser AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_sales') }} AS s
    JOIN 
        {{ ref('fact_metaplex_mints') }} AS m ON s.mint = m.mint
),

recipient_activity AS (
    SELECT 
        DATE_TRUNC('DAY', t.block_timestamp) AS activity_date, 
        t.tx_to AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} AS t
    WHERE 
        EXISTS (SELECT 1 FROM {{ ref('fact_metaplex_mints') }} AS m WHERE m.mint = t.mint)
),

event_activity AS (
    SELECT 
        DATE_TRUNC('DAY', fe.block_timestamp) AS activity_date, 
        signer.value AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_events') }} AS fe,
        LATERAL FLATTEN(input => fe.signers) AS signer
    WHERE 
        fe.program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
),

transaction_activity AS (
    SELECT 
        DATE_TRUNC('DAY', ft.block_timestamp) AS activity_date, 
        signer.value AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_transactions') }} AS ft
    JOIN 
        {{ source('SOLANA_FLIPSIDE', 'fact_events') }} AS fe ON ft.tx_id = fe.tx_id,
        LATERAL FLATTEN(input => ft.signers) AS signer
    WHERE 
        fe.program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
),

decoded_instruction_activity AS (
    SELECT 
        DATE_TRUNC('DAY', di.block_timestamp) AS activity_date, 
        account.value:pubkey::TEXT AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }} AS di,
        LATERAL FLATTEN(input => di.decoded_instruction:accounts) AS account
    WHERE 
        di.program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
        AND account.value:name::TEXT IN ('leafOwner', 'payer', 'treeAuthority', 'leafDelegate', 'treeDelegate', 'collectionAuthority')
),


unique_daily_activity AS (
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        minter_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        seller_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        buyer_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        recipient_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        event_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        transaction_activity 
    UNION ALL
    SELECT 
        activity_date AS day, 
        wallet 
    FROM 
        decoded_instruction_activity
),


cumulative_data AS (
    SELECT 
        day as date, 
        wallet
    FROM 
        unique_daily_activity
)


SELECT 
    date,
    COUNT(DISTINCT wallet) AS daily_active_users
FROM 
    cumulative_data
GROUP BY 
    date
ORDER BY 
    date DESC;
