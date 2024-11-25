{{
    config(
        materialized="table",
        snowflake_warehouse="HASHNOTE",
    )
}}

{{ rwa_data_by_chain_for_issuer("Hashnote") }}