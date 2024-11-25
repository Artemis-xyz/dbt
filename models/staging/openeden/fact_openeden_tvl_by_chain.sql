{{
    config(
        materialized="table",
        snowflake_warehouse="OPENEDEN",
    )
}}

{{ rwa_data_by_chain_for_issuer("openeden") }}
