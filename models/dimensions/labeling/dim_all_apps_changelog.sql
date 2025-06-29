{{ config(materialized="table", snowflake_warehouse="LABELING") }}


with all_address_changes as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("dim_all_frontend_labeled_applications_changelog"),
                ref("all_apps_2025_05_07_seed"),
            ],
        )
    }}
)

select 
    artemis_application_id,
    artemis_category_id,
    artemis_sub_category_id,
    artemis_id,
    coingecko_id,
    ecosystem_id,
    defillama_protocol_id,
    visibility,
    symbol,
    icon,
    app_name,
    description,
    website_url,
    github_url,
    x_handle,
    discord_handle,
    developer_name,
    developer_email,
    developer_x_handle,
    last_updated_by,
    last_updated_timestamp,
    CASE 
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.dim_all_frontend_labeled_applications_changelog' THEN 1
        WHEN lower(_dbt_source_relation) = 'pc_dbt_db.prod.all_apps_2025_05_07_seed' THEN 2
    END as priority,
    _dbt_source_relation
from all_address_changes
order by artemis_application_id, priority asc, last_updated_timestamp desc