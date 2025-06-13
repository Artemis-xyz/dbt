-- Depends on {{ source('PYTHON_LOGIC', 'namespace_to_application')}}
{{ config(materialized="table", snowflake_warehouse="LABELING") }}

with
labeled_name_metadata AS (
    SELECT address, namespace, chain, last_updated, 1 AS priority
    FROM {{ ref("dim_legacy_sigma_tagged_contracts") }}
    UNION ALL
    SELECT address, namespace, chain, last_updated, 2 AS priority
    FROM {{ ref("dim_dune_contracts") }}
    UNION ALL
    SELECT address, namespace, chain, last_updated, 3 AS priority
    FROM {{ ref("dim_sui_contracts") }}
    UNION ALL
    SELECT address, namespace, chain, last_updated, 4 AS priority
    FROM {{ ref("dim_flipside_contracts") }}
)
, deduped_externally_labeled_addresses AS (
    SELECT 
        address,
        namespace,
        chain,
        coalesce(last_updated, '1970-01-01'::TIMESTAMP_NTZ) as last_updated
    FROM labeled_name_metadata
    WHERE namespace IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(address), chain ORDER BY priority ASC) = 1
)
select 
    a.address,
    a.namespace,
    n.artemis_application_id,
    a.chain,
    NULL as address_type,
    NULL AS is_token,
    NULL AS is_fungible,
    a.last_updated
FROM deduped_externally_labeled_addresses a
LEFT JOIN PC_DBT_DB.PROD.dim_namespace_to_application n
    ON a.namespace = n.namespace
WHERE n.artemis_application_id IS NOT NULL