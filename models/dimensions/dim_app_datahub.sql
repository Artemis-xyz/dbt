{{ config(materialized="table") }}


WITH filtered_labeled_addresses AS (
    SELECT 
        lg.* 
    FROM {{ ref('dim_all_addresses_labeled_gold') }} lg 
    INNER JOIN {{ ref('all_chains_gas_dau_txns_by_contract_v2') }} ac 
    ON lg.address = ac.contract_address
), aggregated_chains AS (
    SELECT 
        artemis_application_id, 
        ARRAY_AGG(DISTINCT chain) AS deployed_on
    FROM filtered_labeled_addresses
    GROUP BY artemis_application_id
)
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
    min_dates.earliest_deployment,
    ac.deployed_on
FROM {{ ref("dim_all_apps_gold") }} t3
LEFT JOIN (
    SELECT 
        namespace, 
        CAST(MIN(date) AS DATE) AS earliest_deployment
    FROM {{ ref("all_chains_gas_dau_txns_by_contract_v2") }}
    GROUP BY namespace
) min_dates ON t3.artemis_application_id = min_dates.namespace
LEFT JOIN aggregated_chains ac 
ON ac.artemis_application_id = t3.artemis_application_id