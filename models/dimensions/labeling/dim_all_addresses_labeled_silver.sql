{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
    )
}}

WITH addresses_with_namespace_and_category AS (
    SELECT 
        address, 
        namespace,
        raw_external_category,
        raw_external_sub_category,
        chain,
        last_updated
    FROM {{ ref("dim_all_addresses_gold") }}
    WHERE namespace IS NOT NULL  
    {% if is_incremental() %}
        AND last_updated > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}  
), labeled_automatic_table AS (
    SELECT
        a.address,
        a.namespace AS name,
        n.artemis_application_id,
        c.artemis_category_id,
        c.artemis_sub_category_id,
        a.chain,
        a.last_updated
    FROM addresses_with_namespace_and_category a
    LEFT JOIN {{ source("PYTHON_LOGIC", "dim_namespace_to_application") }} n
        ON a.namespace = n.namespace
    LEFT JOIN {{ source("PYTHON_LOGIC", "automatic_categories_map_seed") }} c
        ON a.raw_external_category = c.raw_external_category AND a.raw_external_sub_category = c.raw_external_sub_category
    WHERE n.artemis_application_id IS NOT NULL 
), final AS (
    SELECT
        COALESCE(dmla.address, lat.address) AS address,
        COALESCE(dmla.name, lat.name) AS name,
        COALESCE(dmla.artemis_application_id, lat.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.artemis_category_id, lat.artemis_category_id) AS artemis_category_id,
        COALESCE(dmla.artemis_sub_category_id, lat.artemis_sub_category_id) AS artemis_sub_category_id,
        COALESCE(dmla.chain, lat.chain) AS chain,
        COALESCE(dmla.last_updated, lat.last_updated) AS last_updated
    FROM labeled_automatic_table lat
    FULL OUTER JOIN {{ source("PYTHON_LOGIC", "dim_manual_labeled_addresses") }} dmla
        ON lat.address = dmla.address
) 
SELECT
    address,
    name,
    INITCAP(REPLACE(name, '_', ' ')) as friendly_name,
    artemis_application_id,
    artemis_category_id,
    artemis_sub_category_id,
    chain,
    last_updated
FROM final