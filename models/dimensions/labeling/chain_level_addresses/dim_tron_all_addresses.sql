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
        WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(from_address) AS address, 'from_address' AS address_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(to_address) AS address, 'to_address' AS address_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(receipt_contract_address) AS address, 'receipt_contract_address' AS address_type, last_updated FROM source_data 
)
SELECT DISTINCT 
    address, 
    address_type,
    'tron' as chain,
    MAX(last_updated) OVER () AS last_updated
FROM union_data
WHERE address IS NOT NULL