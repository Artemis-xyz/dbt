-- depends_on {{ ref("fact_ethereum_p2p_native_transfers") }}
-- depends_on {{ ref("fact_ethereum_p2p_token_transfers") }}
-- depends_on {{ ref("fact_ethereum_p2p_stablecoin_transfers") }}


{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_XS",
    )
}}

{{ p2p_transfer_volume("ethereum") }}