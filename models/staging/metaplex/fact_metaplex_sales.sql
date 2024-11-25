{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'METAPLEX'
    )
}}


SELECT 
    s.*
FROM 
    {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_sales') }} AS s
JOIN 
    {{ ref('fact_metaplex_mints') }} AS m ON s.mint = m.mint
{% if is_incremental() %}
    WHERE block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}
