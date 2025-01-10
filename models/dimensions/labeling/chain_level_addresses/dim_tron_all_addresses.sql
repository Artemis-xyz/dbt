{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT from_address, to_address, receipt_contract_address, _updated_at as last_updated
    FROM tron_allium.raw.transactions
    {% if is_incremental() %}
        WHERE _updated_at > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(from_address) AS address, 'from_address' AS transaction_trace_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(to_address) AS address, 'to_address' AS transaction_trace_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(receipt_contract_address) AS address, 'receipt_contract_address' AS address_type, last_updated FROM source_data 
)
SELECT 
    address,
    ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
    NULL AS address_type,
    'tron' AS chain,
    MAX(last_updated) AS last_updated
FROM union_data
WHERE address IS NOT NULL
GROUP BY address 