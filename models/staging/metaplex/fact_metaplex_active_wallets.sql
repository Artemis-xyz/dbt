{{ config(
    materialized="incremental",
    snowflake_warehouse="METAPLEX",
    unique_key=["date"]
) }}

WITH all_activity AS (
    -- Minter activity
    SELECT 
        DATE_TRUNC('DAY', m.block_timestamp) AS date, 
        m.purchaser AS wallet
    FROM 
        {{ ref('fact_filtered_metaplex_solana_nft_mints') }} AS m
    WHERE LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        and block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- Seller activity
    SELECT 
        DATE_TRUNC('DAY', s.block_timestamp) AS date, 
        seller AS wallet
    FROM 
        {{ ref('fact_metaplex_sales') }} AS s
    WHERE LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        and block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    
    UNION ALL

    -- Buyer activity
    SELECT 
        DATE_TRUNC('DAY', s.block_timestamp) AS date, 
        purchaser AS wallet
    FROM 
        {{ ref('fact_metaplex_sales') }} AS s
    WHERE LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        and block_timestamp >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}

    UNION ALL       

    -- Recipient activity
    SELECT 
        DATE_TRUNC('DAY', t.block_timestamp) AS date, 
        t.tx_to AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} AS t
    WHERE 
        EXISTS (SELECT 1 FROM {{ ref('fact_metaplex_mints') }} AS m WHERE m.mint = t.mint and LOWER(m.program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'))
    {% if is_incremental() %}
        AND t.block_timestamp > (SELECT MAX(date) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- Event activity
    SELECT 
        DATE_TRUNC('DAY', fe.block_timestamp) AS date, 
        signer.value AS wallet
    FROM 
        {{ ref('fact_filtered_metaplex_solana_events') }} AS fe,
        LATERAL FLATTEN(input => fe.signers) AS signer
    WHERE LOWER(fe.program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        and fe.block_timestamp > (SELECT MAX(date) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- Transaction activity
    SELECT 
        DATE_TRUNC('DAY', ft.block_timestamp) AS date, 
        signer.value AS wallet
    FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_transactions') }} AS ft
    JOIN 
        {{ ref('fact_filtered_metaplex_solana_events') }} AS fe ON ft.tx_id = fe.tx_id,
        LATERAL FLATTEN(input => ft.signers) AS signer
    WHERE LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        and ft.block_timestamp > (SELECT MAX(date) FROM {{ this }})
    {% endif %}

    UNION ALL

    -- Decoded instruction activity
    SELECT 
        DATE_TRUNC('DAY', di.block_timestamp) AS date, 
        account.value:pubkey::TEXT AS wallet
    FROM 
        {{ ref('fact_filtered_metaplex_solana_instructions') }} AS di,
        LATERAL FLATTEN(input => di.decoded_instruction:accounts) AS account
    WHERE 
        account.value:name::TEXT IN ('leafOwner', 'payer', 'treeAuthority', 'leafDelegate', 'treeDelegate', 'collectionAuthority')
        and LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
    {% if is_incremental() %}
        AND di.block_timestamp > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
)


SELECT 
    date,
    COUNT(DISTINCT wallet) AS daily_active_users
FROM 
    all_activity
WHERE date < to_date(sysdate())
GROUP BY 
    date
ORDER BY 
    date DESC