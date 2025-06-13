{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
        snowflake_warehouse='BAM_TRANSACTION_XLG'
    )
}}

-- Combine all raw address sources with incremental filtering
WITH raw_addresses AS (
    {% set sources = [
        {"table": "dim_arbitrum_all_addresses", "chain": "arbitrum"},
        {"table": "dim_avalanche_all_addresses", "chain": "avalanche"},
        {"table": "dim_base_all_addresses", "chain": "base"},
        {"table": "dim_bsc_all_addresses", "chain": "bsc"},
        {"table": "dim_ethereum_all_addresses", "chain": "ethereum"},
        {"table": "dim_injective_all_addresses", "chain": "injective"},
        {"table": "dim_stellar_all_addresses", "chain": "stellar"},
        {"table": "dim_near_all_addresses", "chain": "near"},
        {"table": "dim_optimism_all_addresses", "chain": "optimism"},
        {"table": "dim_polygon_all_addresses", "chain": "polygon"},
        {"table": "dim_sei_all_addresses", "chain": "sei"},
        {"table": "dim_solana_all_addresses", "chain": "solana"},
        {"table": "dim_sui_all_addresses", "chain": "sui"},
        {"table": "dim_tron_all_addresses", "chain": "tron"},
        {"table": "dim_mantle_all_addresses", "chain": "mantle"}
    ] %}

    {% for sourc in sources %}
        {% if sourc.chain in ['injective', 'stellar']  %}
            SELECT address AS address, transaction_trace_type, address_type::STRING, chain, last_updated
            FROM  {{ source("PROD_LANDING", sourc.table) }}
        {% else %}
            SELECT address AS address, transaction_trace_type, address_type, chain, last_updated FROM {{ ref(sourc.table) }}
        {% endif %}

        {% if is_incremental() %}
            WHERE last_updated > COALESCE(
                (SELECT MAX(last_updated) FROM {{ this }} WHERE chain = '{{ sourc.chain }}'),
                '1970-01-01'::TIMESTAMP_NTZ
            )
        {% endif %}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
-- This contains name + icon metadata grabbed from labels
labeled_name_metadata AS (
    SELECT address, namespace, NULL AS raw_external_category, NULL AS raw_external_sub_category, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated, 1 AS priority
    FROM {{ ref("dim_legacy_sigma_tagged_contracts") }}
    UNION ALL
    SELECT address, namespace, NULL AS raw_external_category, NULL AS raw_external_sub_category, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated, 2 AS priority
    FROM {{ ref("dim_dune_contracts") }}
    UNION ALL
    SELECT address, namespace, LOWER(category) AS raw_external_category, sub_category AS raw_external_sub_category, chain, OBJECT_CONSTRUCT('name', name, 'icon', icon) AS metadata, last_updated, 3 AS priority
    FROM {{ ref("dim_sui_contracts") }}
    UNION ALL
    SELECT address, namespace, LOWER(category) AS raw_external_category, sub_category AS raw_external_sub_category, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated, 4 AS priority
    FROM {{ ref("dim_flipside_contracts") }}
),
-- This contains deduped labeled_name_metadata
deduped_labeled_name_metadata AS (
    SELECT 
        address,
        namespace,
        LOWER(REPLACE(raw_external_category, ' ', '_')) AS raw_external_category,
        LOWER(REPLACE(raw_external_sub_category, ' ', '_')) AS raw_external_sub_category,
        chain,
        metadata,
        last_updated
    FROM labeled_name_metadata
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(address), chain ORDER BY priority ASC) = 1
),
-- This updates metadata with specific contract/wallet type if labeled
deduped_labeled_name_metadata_with_types AS (
    SELECT 
        address,
        namespace,
        raw_external_category,
        raw_external_sub_category,
        chain,
        CASE 
            WHEN raw_external_sub_category IN ('nf_token_contract', 'staking_contract', 'token_contract', 'general_contract', 'swap_contract', 'aggregator_contract', 'mint_contract', 'airdrop_contract') THEN OBJECT_INSERT(metadata, 'smart_contract_type', raw_external_sub_category, FALSE)
            WHEN raw_external_sub_category = 'token_address' THEN OBJECT_INSERT(metadata, 'smart_contract_type', 'token_contract', FALSE)
            WHEN raw_external_sub_category = 'token_distribution' THEN OBJECT_INSERT(metadata, 'smart_contract_type', 'distribution_contract', FALSE)
            WHEN raw_external_sub_category = 'swap_router' THEN OBJECT_INSERT(metadata, 'smart_contract_type', 'swap_contract', FALSE)
            WHEN raw_external_sub_category IN ('fee_wallet', 'hot_wallet', 'cold_wallet', 'deposit_wallet') THEN OBJECT_INSERT(metadata, 'eoa_type', raw_external_sub_category, FALSE)
            WHEN raw_external_sub_category = 'donation_address' THEN OBJECT_INSERT(metadata, 'eoa_type', 'donation_wallet', FALSE)
            WHEN raw_external_sub_category = 'contract_deployer' THEN OBJECT_INSERT(metadata, 'eoa_type', 'deployer_wallet', FALSE)
            ELSE metadata
        END AS metadata,
        last_updated
    FROM deduped_labeled_name_metadata
),
-- This aggreates all_chains_gas_dau_txns_by_contract by distinct address + chain
distinct_all_chains AS (
    SELECT
        contract_address AS address,
        chain,
        SUM(total_gas) AS total_gas,
        SUM(total_gas_usd) AS total_gas_usd,
        SUM(transactions) AS total_transactions,
        ROUND(AVG(dau), 2) AS average_dau
    FROM pc_dbt_db.prod.all_chains_gas_dau_txns_by_contract_v2
    GROUP BY contract_address, chain
),
deduped_manual_labeled_addresses AS (
    SELECT *
    FROM {{ ref("dim_manual_labeled_addresses") }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY last_updated DESC) = 1
)

, all_addresses as (
SELECT 
    CASE
        WHEN substr(COALESCE(ua.address, TRIM(ra.address)), 1, 2) = '0x' THEN LOWER(COALESCE(ua.address, TRIM(ra.address)))
        ELSE COALESCE(ua.address, TRIM(ra.address))
    END AS address,
    ra.transaction_trace_type AS transaction_trace_type,
    ra.address_type AS address_type,
    COALESCE(OBJECT_CONSTRUCT('name', ua.name), nm.metadata, NULL) AS metadata,
    nm.namespace AS namespace,
    nm.raw_external_category,
    nm.raw_external_sub_category,
    COALESCE(ua.chain, ra.chain, NULL) AS chain,
    ac.total_gas AS total_gas,
    ac.total_gas_usd AS total_gas_usd,
    ac.total_transactions AS total_transactions,
    ac.average_dau AS average_dau,
    geo.country AS country,
    geo.region AS region,
    geo.subregion AS subregion,
    COALESCE(ua.last_updated, ra.last_updated) AS last_updated
FROM raw_addresses ra
LEFT JOIN distinct_all_chains ac
    ON LOWER(ra.address) = LOWER(ac.address) AND ra.chain = ac.chain
LEFT JOIN pc_dbt_db.prod.dim_geo_labels geo
    ON LOWER(ra.address) = LOWER(geo.address) AND ra.chain = geo.chain
LEFT JOIN deduped_labeled_name_metadata_with_types nm
    ON LOWER(ra.address) = LOWER(nm.address) AND ra.chain = nm.chain
FULL OUTER JOIN deduped_manual_labeled_addresses ua
    ON LOWER(ra.address) = LOWER(ua.address) AND ra.chain = ua.chain
)

select 
    address,
    transaction_trace_type,
    address_type,
    metadata,
    namespace,
    raw_external_category,
    raw_external_sub_category,
    chain,
    total_gas,
    total_gas_usd,
    total_transactions,
    average_dau,
    country,
    region,
    subregion,
    last_updated
from all_addresses
qualify row_number() over (partition by address, chain order by last_updated desc) = 1
