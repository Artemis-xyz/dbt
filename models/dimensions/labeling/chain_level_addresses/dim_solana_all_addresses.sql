{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT 
        signer, 
        programs_used, 
        modified_timestamp AS last_updated
    FROM 
        solana_flipside.core.ez_signers
    {% if is_incremental() %}
        WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
), flattened_program_ids AS (
    SELECT 
        value::STRING AS program_ids,
        last_updated
    FROM 
        source_data, 
        LATERAL FLATTEN(input => programs_used)
), union_data AS (
    SELECT 
        trim(program_ids) AS address, 
        'program_id' AS transaction_trace_type,
        CASE
            WHEN trim(program_ids) IS NOT NULL THEN 'smart_contract'
            ELSE NULL
        END AS address_type,
        MAX(last_updated) OVER (PARTITION BY program_ids) AS last_updated
    FROM 
        flattened_program_ids
    UNION ALL
    SELECT 
        trim(signer) AS address, 
        'signer_id' AS transaction_trace_type, 
        NULL AS address_type,
        MAX(last_updated) OVER (PARTITION BY signer) AS last_updated
    FROM 
        source_data
)
SELECT 
    address,
    ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
    COALESCE(MAX(address_type), NULL) AS address_type,
    'solana' AS chain,
    MAX(last_updated) AS last_updated
FROM union_data
WHERE address IS NOT NULL
GROUP BY address 