--depends_on: {{ ref("fact_ethereum_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ETHEREUM_XS",
    )
}}

{{ p2p_stablecoin_transfers("ethereum") }}