{{config(materialized="incremental", unique_key=["namespace"])}}

select 
    namespace,
    max(friendly_name) as friendly_name,
    max(sub_category) as sub_category,
    max(category) as category,
    max(icon) as icon,
    max(last_updated) as last_updated
from {{ ref("dim_sui_namespaces") }} as sui_contracts
group by namespace
