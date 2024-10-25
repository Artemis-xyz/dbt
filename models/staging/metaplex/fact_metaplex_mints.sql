{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

SELECT 
    tx_id,
    DATE(block_timestamp) AS event_date,
    program_id,
    mint,
    mint_price,
    mint_currency,
    is_compressed
FROM 
    solana_flipside.nft.fact_nft_mints
WHERE 
    succeeded = TRUE
    AND program_id IN (
        'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',  -- Bubblegum
        'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR',  -- Candy Machine v3
        'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ',   -- Candy Machine v2
        'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ',   -- Candy Machine v1
        'Guard1JwRhJkVH6XZhzoYxeBVQe872VH6QggF4BWmS9g',  -- Candy Guard
        'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'    -- Token Metadata
    )
