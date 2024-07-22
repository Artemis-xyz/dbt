{{ config(materialized="table") }}
{{
    fact_aave_fork_lending(
        "raw_seamless_protocol_base_borrows_deposits", "base", "seamlessprotocol"
    )
}}
