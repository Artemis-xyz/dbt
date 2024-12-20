{{config(materialized="incremental", unique_key=["address"])}}
with 
sui_contracts_sigma_over as (
    select 
        package_id as address,
        max(package_name) as name, 
        max(coalesce(overwrite.overwrite_namespace, overwrite_w_exisiting_namespace, namespace)) as namespace,
        max(friendly_name) as friendly_name,
        max(project_img) as icon,
        max(category) as category,
        max(sub_category) as sub_category,
        'sui' as chain,
        max(timestamp) as last_updated
    from {{ ref("fact_sui_contracts_silver") }} as sui_contracts full join {{ source("SIGMA", "sui_overwrite_namespace") }} overwrite
    on sui_contracts.namespace = overwrite.sui_namespace
    group by package_id
) 
select 
    address,
    name, 
    namespace,
    friendly_name,
    icon,
    category,
    sub_category,
    chain,
    last_updated
from sui_contracts_sigma_over
where address is not null
