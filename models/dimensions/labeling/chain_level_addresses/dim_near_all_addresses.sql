{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT tx_receiver, tx_signer, modified_timestamp as last_updated
    FROM near_flipside.core.fact_transactions
    {% if is_incremental() %}
        WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(tx_receiver) AS address, 'tx_receiver' AS transaction_trace_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(tx_signer) AS address, 'tx_signer' AS transaction_trace_type, last_updated FROM source_data
)
SELECT 
    address,
    ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
    NULL AS address_type,
    'near' AS chain,
    MAX(last_updated) AS last_updated
FROM union_data
WHERE address IS NOT NULL
GROUP BY address 