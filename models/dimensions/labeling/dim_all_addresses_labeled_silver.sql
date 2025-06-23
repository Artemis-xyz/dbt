{{
    config(
        materialized="table",
        snowflake_warehouse="LABELING",
        unique_key=["address", "chain"],
    )
}}

-- Source priority: frontend/terminal -> global -> manual -> external

WITH 
externally_labeled_addresses AS (
    select 
        address,
        namespace,
        artemis_application_id,
        chain,
        address_type,
        is_token,
        is_fungible,
        last_updated
    FROM {{ref("dim_all_externally_labeled_addresses")}}
),
bulk_manual_labeled_addresses AS (
    SELECT 
        address
        , name
        , artemis_application_id
        , chain
        , is_token
        , is_fungible
        , type
        , last_updated
    FROM {{ ref("dim_manual_labeled_addresses") }}
),
external_and_bulk_automatic_table AS (
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
    FROM externally_labeled_addresses a
    FULL OUTER JOIN bulk_manual_labeled_addresses dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
),
labeled_automatic_table AS (
    SELECT
        COALESCE(dmla.address, a.address) AS address,
        COALESCE(dmla.name, a.name) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        a.address_type,
        COALESCE(a.is_token, dmla.is_token) AS is_token,
        COALESCE(a.is_fungible, dmla.is_fungible) AS is_fungible,
        COALESCE(a.type, dmla.type) AS type,
        COALESCE(dmla.last_updated, a.last_updated) AS last_updated
    FROM external_and_bulk_automatic_table a
    FULL OUTER JOIN {{ ref("dim_global_labeled_addresses")}} dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
),
deduped_added_frontend_manual_labeled_addresses AS (
    SELECT 
        address
        , name
        , artemis_application_id
        , chain
        , last_updated_by
        , last_updated
        , action
    FROM {{ ref('dim_all_frontend_labeled_addresses')}}
    where action = 'ADD'
),

full_labeled_table AS (
    SELECT
        COALESCE(dmla.address, a.address) AS address,
        COALESCE(dmla.name, a.name) AS name,
        COALESCE(dmla.artemis_application_id, a.artemis_application_id) AS artemis_application_id,
        COALESCE(dmla.chain, a.chain) AS chain,
        address_type,
        is_token,
        is_fungible,
        type,
        COALESCE(dmla.last_updated, a.last_updated) AS last_updated,
        dmla.last_updated_by
    FROM labeled_automatic_table a
    FULL OUTER JOIN deduped_added_frontend_manual_labeled_addresses dmla
        ON LOWER(a.address) = LOWER(dmla.address) AND a.chain = dmla.chain
), 
duduped_app_and_address_labeled_table AS (
    SELECT
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
    FROM full_labeled_table lat
    LEFT JOIN {{ ref("dim_all_apps_gold") }} ag
    ON lat.artemis_application_id = ag.artemis_application_id
),

deduped_deleted_frontend_manual_labeled_addresses AS (
    SELECT 
        address
        , name
        , artemis_application_id
        , chain
        , last_updated_by
        , last_updated
        , action
    FROM {{ ref('dim_all_frontend_labeled_addresses')}}
    where action = 'DELETE'
)

SELECT 
    CASE WHEN substr(coalesce(del.address, fat.address), 1, 2) = '0x' THEN LOWER(coalesce(del.address, fat.address)) ELSE coalesce(del.address, fat.address) END AS address,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.name 
    END AS name,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.friendly_name 
    END AS friendly_name,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.artemis_application_id 
    END AS artemis_application_id,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.artemis_category_id 
    END AS artemis_category_id,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.artemis_sub_category_id 
    END AS artemis_sub_category_id,
    coalesce(fat.chain, del.chain) as chain,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.address_type 
    END AS address_type,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.is_token 
    END AS is_token,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.is_fungible 
    END AS is_fungible,
    CASE 
        WHEN del.address IS NOT NULL AND del.last_updated > coalesce(fat.last_updated, '1970-01-01') THEN NULL 
        ELSE fat.type 
    END AS type,
    coalesce(fat.last_updated_by, del.last_updated_by) as last_updated_by,
    coalesce(fat.last_updated, del.last_updated) as last_updated
FROM duduped_app_and_address_labeled_table fat
FULL OUTER JOIN deduped_deleted_frontend_manual_labeled_addresses del
    ON LOWER(fat.address) = LOWER(del.address) AND fat.chain = del.chain