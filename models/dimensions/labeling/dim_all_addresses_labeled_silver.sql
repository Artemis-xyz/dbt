{{
    config(
        materialized="table",
        on_schema_change="append_new_columns"
    )
}}

WITH addresses_with_namespace_and_category AS (
    SELECT 
        address, 
        namespace,
        chain,
        address_type,
        last_updated
    FROM {{ ref("dim_all_addresses_gold") }}
    WHERE namespace IS NOT NULL  
), mapped_addresses AS (
    select 
        a.address,
        a.namespace,
        n.artemis_application_id,
        a.chain,
        a.address_type,
        NULL AS is_token,
        NULL AS is_fungible,
        a.last_updated
    FROM addresses_with_namespace_and_category a
    LEFT JOIN PC_DBT_DB.PROD.dim_namespace_to_application n
        ON a.namespace = n.namespace
    WHERE n.artemis_application_id IS NOT NULL
),
deduped_manual_labeled_addresses AS (
    SELECT *
    FROM {{ source("PYTHON_LOGIC", "dim_manual_labeled_addresses") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated DESC) = 1
),
labeled_automatic_table AS (
    SELECT
        COALESCE(LOWER(dmla.address), LOWER(a.address)) AS address,
        COALESCE(dmla.name, a.namespace) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        a.address_type AS address_type,
        COALESCE(dmla.is_token, NULL) AS is_token,
        COALESCE(dmla.is_fungible, NULL) AS is_fungible,
        COALESCE(dmla.type, NULL) AS type,
        COALESCE(dmla.last_updated, a.last_updated) AS last_updated,
    FROM mapped_addresses a
    FULL OUTER JOIN deduped_manual_labeled_addresses dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
)
SELECT DISTINCT
    lat.address,
    lat.name,
    ag.app_name as friendly_name,
    lat.artemis_application_id,
    ag.artemis_category_id,
    ag.artemis_sub_category_id,
    lat.chain,
    lat.address_type,
    lat.is_token,
    lat.is_fungible,
    lat.type,
    lat.last_updated
FROM labeled_automatic_table lat
LEFT JOIN {{ ref("dim_all_apps_gold") }} ag
ON lat.artemis_application_id = ag.artemis_application_id