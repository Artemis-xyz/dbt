{{
    config(
        materialized="table",
        snowflake_warehouse="PAXOS",
    )
}}

{{ rwa_data_by_product_for_issuer("paxos") }}
