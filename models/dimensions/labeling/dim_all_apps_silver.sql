{{
    config(
        materialized="table"
    )
}}

SELECT
    sil.artemis_application_id,
    sil.artemis_category_id,
    sil.artemis_sub_category_id,
    sil.artemis_id,
    sil.coingecko_id,
    sil.ecosystem_id,
    sil.defillama_protocol_id,
    sil.visibility,
    coalesce(token.token_symbol, sil.symbol) as symbol,
    coalesce(token.token_image_small, sil.icon) as icon,
    sil.app_name,
    sil.description,
    sil.website_url,
    sil.github_url,
    sil.x_handle,
    sil.discord_handle,
    sil.developer_name,
    sil.developer_email,
    sil.developer_x_handle
FROM
    {{ this }} sil
LEFT JOIN
    dim_coingecko_tokens token
ON sil.coingecko_id = token.coingecko_token_id
