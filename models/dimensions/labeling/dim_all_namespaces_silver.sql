{{
    config(
        materialized="incremental",
        unique_key=["namespace"],
        incremental_strategy="merge",
    )
}}

with dune_namespaces as (
    select 
        namespace, 
        'dune' as source,
        max(last_updated) as last_updated 
    from pc_dbt_db.prod.dim_dune_contracts 
    where namespace is not null 
    {% if is_incremental() %}
        and last_updated > (SELECT MAX(last_updated) FROM {{ this }} WHERE source = 'dune')
    {% endif %}
    group by namespace
), sui_namespaces as (
    select 
        namespace, 
        'sui' as source,
        max(last_updated) as last_updated 
    from pc_dbt_db.prod.dim_sui_contracts 
    where namespace is not null 
    {% if is_incremental() %}
        and last_updated > (SELECT MAX(last_updated) FROM {{ this }} WHERE source = 'sui')
    {% endif %}
    group by namespace
), flipside_namespaces as (
    select 
        namespace, 
        'flipside' as source,
        max(last_updated) as last_updated 
    from pc_dbt_db.prod.dim_flipside_contracts 
    where namespace is not null 
    {% if is_incremental() %}
        and last_updated > (SELECT MAX(last_updated) FROM {{ this }} WHERE source = 'flipside')
    {% endif %}
    group by namespace
), unioned as (
    select * from dune_namespaces 
    union all 
    select * from sui_namespaces 
    union all 
    select * from flipside_namespaces
) 
select 
    namespace, 
    source,
    max(last_updated) as last_updated
from unioned 
group by namespace, source