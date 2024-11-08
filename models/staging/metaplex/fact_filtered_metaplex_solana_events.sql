{{
    config(
        materialized="incremental",
        snowflake_warehouse="METAPLEX",
        unique_key= ["block_id", "tx_id", "index"]
    )
}}

SELECT e.* FROM {{ source('SOLANA_FLIPSIDE', 'fact_events') }} e
JOIN {{ ref('fact_metaplex_programs') }} mp ON e.program_id = mp.program_id
AND succeeded = TRUE
{% if is_incremental() %}
    AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}
