{{ config(materialized="table", snowflake_warehouse="SEAMLESSPROTOCOL") }}
{{
    fact_aave_fork_lending(
        "raw_seamless_protocol_base_borrows_deposits", "base", "seamlessprotocol"
    )
}}
