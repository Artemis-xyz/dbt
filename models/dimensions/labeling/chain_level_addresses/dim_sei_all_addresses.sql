{{
    config(
        materialized="incremental",
        unique_key=["address"],
        incremental_strategy="merge",
    )
}}

WITH source_data AS (
    SELECT from_address, contract_address, inserted_timestamp as last_updated
    FROM sei.prod_raw.ez_transactions
    {% if is_incremental() %}
        WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(from_address) AS address, 'from_address' AS address_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(contract_address) AS address, 'contract_address' AS address_type, last_updated FROM source_data
)
SELECT DISTINCT 
    address, 
    address_type,
    'sei' as chain,
    MAX(last_updated) OVER () AS last_updated
FROM union_data
WHERE address IS NOT NULL