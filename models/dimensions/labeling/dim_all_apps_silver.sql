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
), updated_categories AS (
    SELECT
        artemis_application_id,
        ARRAY_DISTINCT(ARRAY_AGG(artemis_category_id)) AS artemis_category_id
    FROM
        {{ ref("dim_all_addresses_labeled_gold") }}
    GROUP BY
        artemis_application_id
), augmented_applications AS (
    SELECT
        COALESCE(na.artemis_application_id, sil.artemis_application_id) AS artemis_application_id,
        sil.artemis_category_id,
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
)
SELECT
    aa.artemis_application_id,
    ARRAY_DISTINCT(ARRAY_CAT(aa.artemis_category_id, uc.artemis_category_id)) AS artemis_category_id,
    aa.artemis_id,
    aa.coingecko_id,
    aa.ecosystem_id,
    aa.defillama_protocol_id,
    aa.visibility,
    aa.symbol,
    aa.icon
FROM augmented_applications aa
LEFT JOIN updated_categories uc
    ON aa.artemis_application_id = uc.artemis_application_id