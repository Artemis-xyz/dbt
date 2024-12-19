{{
    config(
        materialized="incremental",
        unique_key=["address", "chain", "source"],
        incremental_strategy="merge",
    )
}}

-- There will inevitably be addres + chain duplicates, but we will take care of this downstream when we don't care about the source anymore
with unioned_table as (
    SELECT address, chain, 'sigma' AS source, OBJECT_CONSTRUCT(
        'name', name
    ) AS metadata,
    last_updated
    FROM {{ source("MANUAL_STATIC_TABLES", "dim_legacy_sigma_tagged_contracts") }}
    UNION ALL
    SELECT address, chain, 'dune' AS source, OBJECT_CONSTRUCT(
        'name', name
    ) AS metadata,
    last_updated
    FROM {{ ref("dim_dune_contracts") }}
    UNION ALL
    SELECT address, chain, 'sui' AS source, OBJECT_CONSTRUCT(
        'name', name, 'icon', icon
    ) AS metadata,
    last_updated
    FROM {{ ref("dim_sui_contracts") }}
    UNION ALL
    SELECT address, chain, 'flipside' AS source, OBJECT_CONSTRUCT(
        'name', name
    ) AS metadata,
    last_updated
    FROM {{ ref("dim_flipside_contracts") }}
) select * from unioned_table 
{% if is_incremental() %}
    where last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}