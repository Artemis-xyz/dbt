--depends_on: {{ ref("fact_near_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="NEAR",
    )
}}

{{ p2p_stablecoin_transfers("near") }}