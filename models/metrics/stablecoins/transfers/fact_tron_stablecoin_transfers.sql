-- depends_on: {{ ref('fact_tron_stablecoin_contracts') }}
-- depends_on: {{ ref('fact_tron_stablecoin_bridge_addresses') }}
{{ config(
    materialized="table", 
        snowflake_warehouse="STABLECOIN_LG_2", 
        unique_key=["tx_hash", "index"],
    ) 
}}
{{ agg_chain_stablecoin_transfers("tron") }}
