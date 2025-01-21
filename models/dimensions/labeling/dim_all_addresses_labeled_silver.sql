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
), labeled_automatic_table AS (
    SELECT
        a.address,
        a.namespace AS name,
        n.artemis_application_id,
        NULL AS category,
        NULL AS sub_category,
        a.chain
    FROM addresses_with_namespace a
    LEFT JOIN pc_dbt_db.prod.dim_namespace_to_application n
        ON a.namespace = n.namespace
    WHERE n.artemis_application_id IS NOT NULL
)
SELECT
    COALESCE(dmla.address, lat.address) AS address,
    COALESCE(dmla.name, lat.name) AS name,
    COALESCE(dmla.artemis_application_id, lat.artemis_application_id) AS artemis_application_id,
    COALESCE(dmla.category, lat.category) AS category,
    COALESCE(dmla.sub_category, lat.sub_category) AS sub_category,
    COALESCE(dmla.chain, lat.chain) AS chain
FROM labeled_automatic_table lat
FULL OUTER JOIN pc_dbt_db.prod.dim_manual_labeled_addresses dmla
    ON lat.address = dmla.address