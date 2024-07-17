--depends_on: {{ ref("fact_tron_stablecoin_transfers") }}
{{
    config(
        materialized="table",
        unique_key=["tx_hash", "index"],
    )
}}

{{ p2p_stablecoin_transfers("tron") }}