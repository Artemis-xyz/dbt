{{
    config(
        materialized="table",
        snowflake_warehouse="PAXOS",
    )
}}

{{ rwa_data_by_chain_for_issuer("paxos") }}