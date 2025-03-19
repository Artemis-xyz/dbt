{{
    config(
        materialized="table"
    )
}}

WITH dim_frontend_manual_applications AS (
    SELECT * FROM {{ source("PYTHON_LOGIC", "dim_frontend_manual_applications") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY artemis_application_id ORDER BY last_updated_timestamp DESC) = 1
)

SELECT
    COALESCE(ma.artemis_application_id, sil.artemis_application_id) as artemis_application_id,
    COALESCE(ma.artemis_category_id, sil.artemis_category_id) as artemis_category_id,
    COALESCE(ma.artemis_sub_category_id, sil.artemis_sub_category_id) as artemis_sub_category_id,
    COALESCE(ma.artemis_id, sil.artemis_id) as artemis_id,
    COALESCE(ma.coingecko_id, sil.coingecko_id) as coingecko_id,
    COALESCE(ma.ecosystem_id, sil.ecosystem_id) as ecosystem_id,
    COALESCE(ma.defillama_protocol_id, sil.defillama_protocol_id) as defillama_protocol_id,
    COALESCE(ma.visibility, sil.visibility) as visibility,
    COALESCE(ma.app_symbol, token.token_symbol, sil.symbol) as symbol,
    COALESCE(ma.app_icon, token.token_image_small, sil.icon) as icon,
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
    CASE 
        WHEN ma.last_updated_timestamp IS NOT NULL THEN CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        ELSE sil.last_updated_timestamp 
    END as last_updated_timestamp
FROM
    {{ this }} sil
LEFT JOIN
    dim_coingecko_tokens token
ON sil.coingecko_id = token.coingecko_token_id
FULL OUTER JOIN 
    dim_frontend_manual_applications ma
ON sil.artemis_application_id = ma.artemis_application_id
