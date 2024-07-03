--depends_on: {{ ref("fact_arbitrum_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ p2p_stablecoin_transfers("arbitrum") }}