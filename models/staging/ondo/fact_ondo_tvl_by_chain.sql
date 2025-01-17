{{
    config(
        materialized="table",
        snowflake_warehouse="ONDO",
    )
}}

{{ rwa_data_by_chain_for_issuer("ondo") }}
