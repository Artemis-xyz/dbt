{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'METAPLEX',
        unique_key = 'block_timestamp'
    )
}}


SELECT 
    MARKETPLACE
    , BLOCK_TIMESTAMP
    , BLOCK_ID
    , s.TX_ID
    , SUCCEEDED
    , s.PROGRAM_ID
    , BUYER_ADDRESS AS PURCHASER
    , SELLER_ADDRESS AS SELLER
    , s.MINT
    , PRICE AS SALES_AMOUNT
    , TREE_AUTHORITY
    , MERKLE_TREE
    , LEAF_INDEX
    , s.IS_COMPRESSED
    , EZ_NFT_SALES_ID AS FACT_NFT_SALES_ID
    , INSERTED_TIMESTAMP
    , MODIFIED_TIMESTAMP
FROM 
    {{ source('SOLANA_FLIPSIDE_NFT', 'ez_nft_sales') }} AS s
JOIN 
    {{ ref('fact_metaplex_mints') }} AS m ON s.mint = m.mint
{% if is_incremental() %}
    WHERE block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}
