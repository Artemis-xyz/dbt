{{
    config(
        materialized="incremental",
        snowflake_warehouse="METAPLEX",
        unique_key= ["block_id", "tx_id", "index"]
    )
}}

SELECT 
    *
FROM 
    {{ source('SOLANA_FLIPSIDE', 'fact_decoded_instructions') }} AS di
WHERE 
    di.program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
{% if is_incremental() %}
    AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}
