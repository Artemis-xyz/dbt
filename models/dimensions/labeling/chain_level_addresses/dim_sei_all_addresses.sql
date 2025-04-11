{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT from_address, contract_address, block_timestamp as last_updated
    FROM pc_dbt_db.prod.fact_sei_transactions_v2 -- Hardcoded to prevent dbt cycle complaints
    {% if is_incremental() %}
        WHERE block_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(from_address) AS address, 'from_address' AS transaction_trace_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(contract_address) AS address, 'contract_address' AS transaction_trace_type, last_updated FROM source_data
)
SELECT 
    address,
    ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
    NULL AS address_type,
    'sei' AS chain,
    MAX(last_updated) AS last_updated
FROM union_data
WHERE address IS NOT NULL
GROUP BY address 