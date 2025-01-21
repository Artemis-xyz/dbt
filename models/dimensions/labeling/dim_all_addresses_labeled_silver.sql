{{
    config(
        materialized="table"
    )
}}

WITH addresses_with_namespace AS (
    SELECT 
        address, 
        namespace,
        chain
    FROM {{ ref("dim_all_addresses_gold") }}
    WHERE namespace IS NOT NULL    
)
SELECT
    a.address,
    a.namespace,
    n.artemis_application_id,
    a.chain
FROM addresses_with_namespace a
LEFT JOIN pc_dbt_db.prod.dim_namespace_to_application n
    ON a.namespace = n.namespace
WHERE n.artemis_application_id IS NOT NULL