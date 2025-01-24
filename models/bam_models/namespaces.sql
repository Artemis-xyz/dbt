{{ config(materialized="table") }}

with
    namespaces as (
        select distinct namespace, friendly_name, category, chain
        from {{ ref("all_chains_gas_dau_txns_by_namespace") }}
        order by 3, 2, 1
    )
select
    np.namespace,
    np.friendly_name,
    case when np.category = 'Unlabeled' then null else np.category end as category,
    np.chain
from namespaces as np
left join {{ ref("dim_all_apps_gold") }} as apps on np.namespace = apps.artemis_application_id
where (visibility <> false)
