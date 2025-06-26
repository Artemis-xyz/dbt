{{ config(materialized="table", snowflake_warehouse="LABELING") }}
with
application_labels as (
    SELECT
        COALESCE(ma.artemis_application_id, sil.artemis_application_id) as artemis_application_id,
        COALESCE(ma.artemis_category_id, sil.artemis_category_id) as artemis_category_id,
        COALESCE(ma.artemis_sub_category_id, sil.artemis_sub_category_id) as artemis_sub_category_id,
        COALESCE(ma.artemis_id, sil.artemis_id) as artemis_id,
        COALESCE(ma.coingecko_id, sil.coingecko_id) as coingecko_id,
        COALESCE(ma.ecosystem_id, sil.ecosystem_id) as ecosystem_id,
        COALESCE(ma.defillama_protocol_id, sil.defillama_protocol_id) as defillama_protocol_id,
        COALESCE(ma.visibility, sil.visibility) as visibility,
        COALESCE(ma.symbol, token.token_symbol, sil.symbol) as symbol,
        COALESCE(ma.icon, token.token_image_small, sil.icon) as icon,
        COALESCE(ma.app_name, sil.app_name) as app_name,
        COALESCE(ma.description, sil.description) as description,
        COALESCE(ma.website_url, sil.website_url) as website_url,
        COALESCE(ma.github_url, sil.github_url) as github_url,
        COALESCE(ma.x_handle, sil.x_handle) as x_handle,
        COALESCE(ma.discord_handle, sil.discord_handle) as discord_handle,
        COALESCE(ma.developer_name, sil.developer_name) as developer_name,
        COALESCE(ma.developer_email, sil.developer_email) as developer_email,
        COALESCE(ma.developer_x_handle, sil.developer_x_handle) as developer_x_handle,
        COALESCE(ma.last_updated_by, sil.last_updated_by) as last_updated_by,
        COALESCE(ma.last_updated_timestamp, sil.last_updated_timestamp, '2025-03-01'::TIMESTAMP_NTZ) as last_updated_timestamp
    FROM {{ ref("all_apps_2025_05_07_seed") }} sil
    LEFT JOIN {{ ref('dim_coingecko_tokens')}} token
    ON sil.coingecko_id = token.coingecko_token_id
    FULL OUTER JOIN 
        {{ ref("dim_all_frontend_labeled_applications") }} ma
    ON sil.artemis_application_id = ma.artemis_application_id
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
    last_updated_timestamp
from application_labels
qualify row_number() over (partition by artemis_application_id order by last_updated_timestamp desc) = 1

