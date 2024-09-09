-- depends_on: {{ ref('fact_ethereum_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_ethereum_stablecoin_premint_addresses') }}
-- depends_on: {{ ref('fact_ethereum_stablecoin_bridge_addresses') }}

{{ config(
        materialized="incremental", 
        snowflake_warehouse="STABLECOIN_LG_2", 
        unique_key=["tx_hash", "index"],
    ) 
}}
{{ agg_chain_stablecoin_transfers("ethereum") }}
