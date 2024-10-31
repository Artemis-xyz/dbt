
 -- could not use block_id, tx_id as unique key because of duplicates
{{
    config(
        materialized="incremental",
        snowflake_warehouse="METAPLEX",
        unique_key= ["FACT_NFT_MINTS_ID"]
    )
}}

SELECT 
    *
FROM 
    {{ source('SOLANA_FLIPSIDE_NFT', 'fact_nft_mints') }}
WHERE 
    program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
{% if is_incremental() %}
    AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}
