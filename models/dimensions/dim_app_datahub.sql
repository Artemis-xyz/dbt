{{ config(materialized="table") }}

SELECT 
    t3.artemis_application_id,
    t3.app_name,
    t3.artemis_category_id,
    t3.artemis_sub_category_id,
    t3.artemis_id,
    t3.coingecko_id,
    t3.ecosystem_id,
    t3.defillama_protocol_id,
    t3.visibility,
    t3.symbol AS app_symbol,
    t3.icon AS app_icon,
    t3.description,
    t3.website_url,
    t3.github_url,
    t3.x_handle,
    t3.discord_handle,
    t3.developer_name,
    t3.developer_email,
    t3.developer_x_handle,
    min_dates.earliest_deployment
FROM {{ ref("dim_all_apps_gold") }} t3
LEFT JOIN (
    SELECT 
        namespace, 
        CAST(MIN(date) AS DATE) AS earliest_deployment
    FROM {{ ref("all_chains_gas_dau_txns_by_contract_v2") }}
    GROUP BY namespace
) min_dates ON t3.artemis_application_id = min_dates.namespace