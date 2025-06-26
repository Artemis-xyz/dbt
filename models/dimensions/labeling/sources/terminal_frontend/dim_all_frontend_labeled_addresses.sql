{{ config(materialized="table", snowflake_warehouse="LABELING") }}

select
    action,
    address,
    artemis_application_id,
    chain,
    last_updated_by,
    last_updated,
    name
from {{ ref("dim_all_frontend_labeled_addresses_changelog") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated desc) = 1