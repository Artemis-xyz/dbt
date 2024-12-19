{{
    config(
        materialized="incremental",
        unique_key=["application", "source"],
        incremental_strategy="merge",
    )
}}

-- there are technically two more tables:
-- dim_new_apps_post_sigma and dim_apps_post_sigma
-- but they can be handled later
with unioned_table as (
    SELECT namespace as application, 'dune' AS source, last_updated
    FROM {{ ref("dim_dune_namespaces") }} where namespace is not null
    UNION
    SELECT namespace as application, 'sui' AS source, last_updated
    FROM {{ ref("dim_sui_namespaces") }} where namespace is not null
    UNION
    SELECT namespace as application, 'flipside' AS source, last_updated
    FROM {{ ref("dim_flipside_namespaces") }} where namespace is not null
) 
select * from unioned_table
{% if is_incremental() %}
    where last_updated > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}