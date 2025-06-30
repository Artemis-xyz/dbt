{{ config(materialized="table", snowflake_warehouse="LABELING") }}


with all_address_changes as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("dim_manual_labeled_addresses_changelog"),
                ref("dim_all_frontend_labeled_addresses_changelog"),
                ref("dim_global_labeled_addresses"), 
                ref("dim_all_externally_labeled_addresses"),
            ],
        )
    }}
)

select 
    action,
    address,
    name,
    artemis_application_id,
    chain,
    is_token,
    is_fungible,
    type,
    last_updated,
    last_updated_by,
    CASE
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.dim_all_frontend_labeled_addresses_changelog' THEN 1
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.dim_global_labeled_addresses' THEN 2
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.dim_manual_labeled_addresses_changelog' THEN 3
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.dim_all_externally_labeled_addresses' THEN 4
    END as priority,
    _dbt_source_relation
from all_address_changes
order by address, chain, priority asc, last_updated desc