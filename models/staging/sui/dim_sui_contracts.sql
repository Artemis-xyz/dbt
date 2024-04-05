{{config(materialized="incremental", unique_key=["package_id", "version"])}}
with 
sui_contracts_sigma_over as (
    select 
        package_id as address,
        package_name as name, 
        coalesce(overwrite.overwrite_namespace, overwrite_w_exisiting_namespace, namespace) as namespace,
        friendly_name,
        project_img as icon,
        category,
        sub_category,
        'sui' as chain
    from {{ ref("fact_sui_contracts_silver") }} as sui_contracts full join {{ source("SIGMA", "sui_overwrite_namespace") }} overwrite
    on sui_contracts.namespace = overwrite.sui_namespace
) 
select 
    address,
    name, 
    namespace,
    friendly_name,
    icon,
    category,
    sub_category,
    chain
from sui_contracts_sigma_over
where address is not null
