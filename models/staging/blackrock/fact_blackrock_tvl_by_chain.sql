{{
    config(
        materialized="table",
        snowflake_warehouse="BLACKROCK",
    )
}}

{{ rwa_data_by_chain_for_issuer("BlackRock") }}