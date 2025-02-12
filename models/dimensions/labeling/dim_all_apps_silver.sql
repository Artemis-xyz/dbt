{{
    config(
        materialized="table"
    )
}}

WITH new_apps AS (
    SELECT 
        DISTINCT artemis_application_id 
    FROM {{ source("PYTHON_LOGIC", "dim_namespace_to_application") }}
    WHERE artemis_application_id IS NOT NULL
)
SELECT
    COALESCE(na.artemis_application_id, sil.artemis_application_id) AS artemis_application_id,
    sil.artemis_category_id,
    sil.artemis_sub_category_id,
    sil.artemis_id,
    sil.coingecko_id,
    sil.ecosystem_id,
    sil.defillama_protocol_id,
    sil.visibility,
    coalesce(token.token_symbol, sil.symbol) as symbol,
    coalesce(token.token_image_small, sil.icon) as icon,
FROM
    {{ this }} sil
FULL OUTER JOIN 
    new_apps na
ON 
    na.artemis_application_id = sil.artemis_application_id
LEFT JOIN
    dim_coingecko_tokens token
ON sil.coingecko_id = token.coingecko_token_id
