-- This table is incremental to account for deleted contracts (from applications)
{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
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
    {% if is_incremental() %}
        AND last_updated > (SELECT DATEADD('day', -5, MAX(last_updated)) FROM {{ this }})
    {% endif %}
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
    {% if is_incremental() %}
        LEFT JOIN {{ this }} existing
            ON existing.address = a.address
            AND existing.chain = a.chain
    {% endif %}
    WHERE n.artemis_application_id IS NOT NULL
    {% if is_incremental() %}
        AND existing.address IS NULL
    {% endif %}
),
deduped_bulk_manual_labeled_addresses AS (
    SELECT *
    FROM {{ ref("dim_manual_labeled_addresses") }}
    {% if is_incremental() %}
        WHERE last_updated > (SELECT DATEADD('day', -5, MAX(last_updated)) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated DESC) = 1
),
labeled_automatic_table AS (
    SELECT
        COALESCE(dmla.address, a.address) AS address,
        COALESCE(dmla.name, a.namespace) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        a.address_type AS address_type,
        COALESCE(dmla.is_token, NULL) AS is_token,
        COALESCE(dmla.is_fungible, NULL) AS is_fungible,
        COALESCE(dmla.type, NULL) AS type,
        COALESCE(dmla.last_updated, a.last_updated) AS last_updated,
    FROM mapped_addresses a
    FULL OUTER JOIN deduped_bulk_manual_labeled_addresses dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
),
deduped_added_manual_labeled_addresses AS (
    SELECT *
    FROM {{ source("PYTHON_LOGIC", "dim_frontend_manual_contracts") }}
    WHERE action = 'ADD'
    {% if is_incremental() %}
        AND last_updated_timestamp > (SELECT DATEADD('day', -5, MAX(last_updated)) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated_timestamp DESC) = 1
),
deduped_deleted_manual_labeled_addresses AS (
    SELECT *
    FROM {{ source("PYTHON_LOGIC", "dim_frontend_manual_contracts") }}
    WHERE action = 'DELETE'
    {% if is_incremental() %}
        AND last_updated_timestamp > (SELECT DATEADD('day', -5, MAX(last_updated)) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated_timestamp DESC) = 1
),
full_labeled_automatic_table AS (
    SELECT
        COALESCE(dmla.address, a.address) AS address,
        COALESCE(dmla.name, a.name) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        a.address_type,
        COALESCE(a.is_token, dmla.is_token) AS is_token,
        COALESCE(a.is_fungible, dmla.is_fungible) AS is_fungible,
        COALESCE(a.type, dmla.type) AS type,
        COALESCE(dmla.last_updated_timestamp, a.last_updated) AS last_updated,
        dmla.last_updated_by
    FROM labeled_automatic_table a
    FULL OUTER JOIN deduped_added_manual_labeled_addresses dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
), 
final_added_table AS (
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
        lat.last_updated_by,
        lat.last_updated
    FROM full_labeled_automatic_table lat
    LEFT JOIN {{ ref("dim_all_apps_gold") }} ag
    ON lat.artemis_application_id = ag.artemis_application_id
) 
SELECT 
    CASE WHEN substr(fat.address, 1, 2) = '0x' THEN LOWER(fat.address) ELSE fat.address END AS address,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.name 
    END AS name,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.friendly_name 
    END AS friendly_name,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.artemis_application_id 
    END AS artemis_application_id,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.artemis_category_id 
    END AS artemis_category_id,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.artemis_sub_category_id 
    END AS artemis_sub_category_id,
    fat.chain,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.address_type 
    END AS address_type,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.is_token 
    END AS is_token,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.is_fungible 
    END AS is_fungible,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.type 
    END AS type,
    CASE 
        WHEN del.address IS NOT NULL THEN NULL 
        ELSE fat.last_updated_by 
    END AS last_updated_by,
    fat.last_updated
FROM final_added_table fat
LEFT JOIN deduped_deleted_manual_labeled_addresses del
ON LOWER(fat.address) = LOWER(del.address) AND fat.chain = del.chain