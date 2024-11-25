{{
    config(
        materialized="table",
        snowflake_warehouse="TETHER",
    )
}}

{{ rwa_data_by_chain_for_issuer("tether") }}