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
        {"table": "landing_database.prod_landing.dim_injective_all_addresses", "chain": "injective"},
        {"table": "dim_near_all_addresses", "chain": "near"},
        {"table": "dim_optimism_all_addresses", "chain": "optimism"},
        {"table": "dim_polygon_all_addresses", "chain": "polygon"},
        {"table": "dim_sei_all_addresses", "chain": "sei"},
        {"table": "dim_solana_all_addresses", "chain": "solana"},
        {"table": "dim_sui_all_addresses", "chain": "sui"},
        {"table": "dim_tron_all_addresses", "chain": "tron"}
    ] %}

    {% for source in sources %}
        SELECT * FROM 
        {% if source.chain == 'injective' %}
            {{ source.table }}
        {% else %}
            {{ ref(source.table) }}
        {% endif %}

        {% if is_incremental() %}
            WHERE last_updated > (SELECT MAX(last_updated) FROM {{ this }} WHERE chain = '{{ source.chain }}')
        {% endif %}

        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
-- This contains name + icon metadata grabbed from labels
labeled_name_metadata AS (
    SELECT address, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated
    FROM {{ source("MANUAL_STATIC_TABLES", "dim_legacy_sigma_tagged_contracts") }}
    UNION ALL
    SELECT address, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated
    FROM {{ ref("dim_dune_contracts") }}
    UNION ALL
    SELECT address, chain, OBJECT_CONSTRUCT('name', name, 'icon', icon) AS metadata, last_updated
    FROM {{ ref("dim_sui_contracts") }}
    UNION ALL
    SELECT address, chain, OBJECT_CONSTRUCT('name', name) AS metadata, last_updated
    FROM {{ ref("dim_flipside_contracts") }}
)

SELECT 
    TRIM(ra.address) AS address,
    ra.address_type AS address_type,
    nm.metadata AS metadata,
    ra.chain AS chain,
    ac.total_gas AS total_gas,
    ac.total_gas_usd AS total_gas_usd,
    ac.transactions AS transactions,
    ac.dau AS dau,
    ac.token_transfer_usd AS token_transfer_usd,
    ac.token_transfer AS token_transfer,
    ac.avg_token_price AS avg_token_price,
    geo.country AS country,
    geo.region AS region,
    geo.subregion AS subregion,
    ra.last_updated AS last_updated
FROM raw_addresses ra
LEFT JOIN {{ ref("all_chains_gas_dau_txns_by_contract") }} ac
    ON ra.address = ac.contract_address AND ra.chain = ac.chain
LEFT JOIN pc_dbt_db.prod.dim_geo_labels geo
    ON ra.address = geo.address AND ra.chain = geo.chain
LEFT JOIN labeled_name_metadata nm
    ON ra.address = nm.address AND ra.chain = nm.chain

