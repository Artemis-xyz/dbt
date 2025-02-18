{{ config(materialized="table") }}

select 
    artemis_application_id,
    app_name,
    artemis_category_id,
    artemis_sub_category_id,
    artemis_id,
    coingecko_id,
    ecosystem_id,
    defillama_protocol_id,
    visibility,
    symbol as app_symbol,
    icon as app_icon,
    description,
    website_url,
    github_url,
    x_handle,
    discord_handle,
    developer_name,
    developer_email,
    developer_x_handle
from {{ ref("dim_all_apps_gold") }}