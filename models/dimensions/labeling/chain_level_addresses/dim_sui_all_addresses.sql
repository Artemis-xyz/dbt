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
        WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),
union_data AS (
    SELECT trim(sender) AS address, 'sender_id' AS address_type, last_updated FROM source_data
    UNION ALL
    SELECT trim(package) AS address, 'package_id' AS address_type, last_updated FROM source_data
)
SELECT DISTINCT 
    address, 
    address_type,
    'sui' as chain,
    MAX(last_updated) OVER () AS last_updated
FROM union_data
WHERE address IS NOT NULL