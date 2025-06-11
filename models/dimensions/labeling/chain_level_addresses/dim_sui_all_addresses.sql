{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT sender, package, block_timestamp as last_updated
    FROM sui.prod_raw.ez_transactions
    {% if is_incremental() %}
        WHERE block_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT 
        trim(sender) AS address, 
        'sender_id' AS transaction_trace_type, 
        NULL AS address_type,
        last_updated 
    FROM source_data
    UNION ALL
    SELECT 
        trim(package) AS address, 
        'package_id' AS transaction_trace_type, 
        CASE
            WHEN trim(package) IS NOT NULL THEN 'smart_contract'
            ELSE NULL
        END AS address_type,
        last_updated 
    FROM source_data
)
SELECT 
    address,
    ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
    COALESCE(MAX(address_type), NULL) AS address_type,
    'sui' AS chain,
    MAX(last_updated) AS last_updated
FROM union_data
WHERE address IS NOT NULL
GROUP BY address