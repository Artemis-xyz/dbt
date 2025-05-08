{{config(materialized="table", snowflake_warehouse="LABELING")}}

SELECT 
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
    last_updated_timestamp
FROM {{ ref("dim_all_apps_silver") }}
