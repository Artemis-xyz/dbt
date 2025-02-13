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
        chain,
        last_updated
    FROM {{ ref("dim_all_addresses_gold") }}
    WHERE namespace IS NOT NULL  
    {% if is_incremental() %}
        AND last_updated > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}  
), mapped_addresses AS (
    select 
        a.address,
        a.namespace,
        n.artemis_application_id,
        a.chain,
        NULL AS is_token,
        NULL AS is_fungible,
        a.last_updated
    FROM addresses_with_namespace_and_category a
    LEFT JOIN PC_DBT_DB.PROD.dim_namespace_to_application n
        ON a.namespace = n.namespace
    WHERE n.artemis_application_id IS NOT NULL
),
labeled_automatic_table AS (
    SELECT
        COALESCE(dmla.address, a.address) AS address,
        COALESCE(dmla.name, a.namespace) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        COALESCE(dmla.is_token, NULL) AS is_token,
        COALESCE(dmla.is_fungible, NULL) AS is_fungible,
        COALESCE(dmla.last_updated, a.last_updated) AS last_updated
    FROM mapped_addresses a
    FULL OUTER JOIN PC_DBT_DB.PROD.dim_manual_labeled_addresses dmla
        ON a.address = dmla.address
)
SELECT
    lat.address,
    lat.name,
    INITCAP(REPLACE(lat.name, '_', ' ')) as friendly_name,
    lat.artemis_application_id,
    ag.artemis_category_id,
    ag.artemis_sub_category_id,
    lat.chain,
    lat.is_token,
    lat.is_fungible,
    lat.last_updated
FROM labeled_automatic_table lat
LEFT JOIN {{ ref("dim_all_apps_gold") }} ag
ON lat.artemis_application_id = ag.artemis_application_id