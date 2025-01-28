{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge",
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
        {"table": "dim_near_all_addresses", "chain": "near"},
        {"table": "dim_optimism_all_addresses", "chain": "optimism"},
        {"table": "dim_polygon_all_addresses", "chain": "polygon"},
        {"table": "dim_sei_all_addresses", "chain": "sei"},
        {"table": "dim_solana_all_addresses", "chain": "solana"},
        {"table": "dim_sui_all_addresses", "chain": "sui"},
        {"table": "dim_tron_all_addresses", "chain": "tron"}
    ] %}

    {% for sourc in sources %}
        {% if sourc.chain == 'injective' %}
            SELECT address, transaction_trace_type, address_type::STRING, chain, last_updated
            FROM  {{ source("PROD_LANDING", sourc.table) }}
        {% else %}
            SELECT * FROM {{ ref(sourc.table) }}
        {% endif %}

        {% if is_incremental() %}
            WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }} WHERE chain = '{{ sourc.chain }}')
        {% endif %}

        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
-- This contains name + icon metadata grabbed from labels
labeled_name_metadata AS (
    SELECT address, NULL AS namespace, NULL AS raw_external_category, NULL AS raw_external_sub_category, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated, 1 AS priority
    FROM {{ source("MANUAL_STATIC_TABLES", "dim_legacy_sigma_tagged_contracts") }}
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
        raw_external_category,
        raw_external_sub_category,
        chain,
        metadata,
        last_updated
    FROM labeled_name_metadata
    QUALIFY ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY priority ASC) = 1
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
    FROM {{ ref("all_chains_gas_dau_txns_by_contract") }}
    GROUP BY contract_address, chain
)

SELECT 
    COALESCE(ua.address, TRIM(ra.address)) AS address,
    ra.transaction_trace_type AS transaction_trace_type,
    ra.address_type AS address_type,
    COALESCE(OBJECT_CONSTRUCT('name', ua.name), nm.metadata, NULL) AS metadata,
    nm.namespace AS namespace,
    LOWER(REPLACE(nm.raw_external_category, ' ', '_')) AS raw_external_category,
    LOWER(REPLACE(nm.raw_external_sub_category, ' ', '_')) AS raw_external_sub_category,
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
    ON ra.address = ac.address AND ra.chain = ac.chain
LEFT JOIN pc_dbt_db.prod.dim_geo_labels geo
    ON ra.address = geo.address AND ra.chain = geo.chain
LEFT JOIN deduped_labeled_name_metadata nm
    ON ra.address = nm.address AND ra.chain = nm.chain
FULL OUTER JOIN {{ source("PYTHON_LOGIC", "dim_manual_labeled_addresses") }} ua
    ON ra.address = ua.address

