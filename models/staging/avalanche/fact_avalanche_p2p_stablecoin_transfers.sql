--depends_on: {{ ref("fact_avalanche_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ p2p_stablecoin_transfers("avalanche") }}